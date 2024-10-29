# Portability & Client Plan (Android + Desktop UI)

Goal: turn tagnet into a usable product on **Android and Linux desktop** with a
**single shared UI codebase**, while keeping all sync/DB/business logic in the
existing Rust core.

Two process topologies must be supported:

- **Android:** one process. UI + sync engine + DB all in-process; the UI calls
  the Rust core directly via FFI.
- **Linux:** two processes. A systemd daemon owns the sync engine + DB; a
  separate UI application attaches to it over local IPC.

This document is a proposal to refine before implementation. Nothing here is set
in stone; items are grouped so they can be reordered or dropped.

---

## Decisions locked in

- **UI framework:** **Flutter + `flutter_rust_bridge`.**
  - Reason: mobile background/OS integration (foreground service, permissions,
    lifecycle, file pickers) is the hardest part and Flutter is the only
    candidate with a mature ecosystem for it; the UI is rich (tags, upload,
    search) which favors Flutter's mature widget set; single-language (Rust UI)
    was explicitly dropped as a requirement, removing the main reason to prefer
    Dioxus/Slint.
- **All business logic stays in Rust** (behind the FFI/IPC boundary). The Dart
  UI is a thin presentation layer. Tag rules, search semantics, subtag
  relationships (`SubtagRule`), etc. are never reimplemented in Dart.
- **One UI-facing API, two transports.** The UI always talks to the same logical
  API; only the transport underneath differs (in-process on Android, IPC to the
  daemon on Linux). See section 6.
- **Local control on Linux = Unix domain socket** in `$XDG_RUNTIME_DIR`, secured
  by filesystem permissions, reusing the existing WebSocket/`Frame` framing. See
  section 7.
- **The `0.0.0.0:{listen_port}` listener is unchanged** — it is the remote
  **peer-sync** port (`main.rs:210-227`) and is unrelated to local UI control.

---

## 0. Constraints discovered during analysis

- The workspace pins **nightly** (`rust-toolchain.toml`, `nightly-2025-12-01`)
  and uses `let`-chains, so the Android target's `std` must come from that same
  nightly channel.
- The code is fully cross-platform at the source level: no `#[cfg(target_os)]`,
  no `libc`, no `std::os`, no `unsafe`. OS specifics live inside crates
  (`notify`, `tokio`, `rusqlite` bundled, `rand`/`getrandom`).
- `notify` -> inotify works on Android for **app-private storage** only.
  Shared media (Pictures/DCIM/Screenshots) needs a Java/Flutter-side
  `MediaStore` observer -> Rust rescan (unreliable via inotify over the FUSE
  `/sdcard` mount). Other apps' private dirs (Signal, WhatsApp) are impossible
  without root — **out of scope**.
- `FileDatabase` is deliberately **single-owner**: it wraps a rusqlite
  `Connection` (`Send`, not `Sync`); all mutation funnels through the single
  `handle_changes` task (see comments at `main.rs:490-494`). Therefore two
  processes cannot both open the DB — on Linux the UI must *ask the daemon*, not
  open the DB itself. This is the root cause of the two-transport design.

---

## 1. Refactor `tagnet` from a pure binary into lib + thin binary

**Why:** Android loads a native library and calls in; there is no `main()`. The
`run` logic (`main.rs:151-233`) must be callable as a library function, and the
UI/IPC layers must be able to link the core.

**Changes:**

- Add `tagnet/src/lib.rs`; move module declarations (`configuration`,
  `database`, `directory_manager`, `identity`, `paths`, `watcher`) and the
  peer/session/change-handling functions into the lib.
- Add to `tagnet/Cargo.toml`:
  ```toml
  [lib]
  crate-type = ["rlib", "cdylib"]
  ```
- Keep `tagnet/src/main.rs` as a thin binary: clap CLI (`keygen`/`reset`/
  `generate`/`run`) that calls into the lib.
- Extract `Commands::Run`'s body into a public async function:
  ```rust
  pub struct RunPaths { pub data_dir: PathBuf, pub identity_file: PathBuf }
  pub async fn run(
      configuration: Configuration,
      paths: RunPaths,
      shutdown: ShutdownSignal, // section 4
  ) -> Result<(), RunError>;
  ```

**Open question:** JNI/bridge entry points live in a separate `tagnet-jni`/
bridge crate (preferred, keeps desktop binary clean) vs. a feature flag in
`tagnet`. Leaning separate crate.

---

## 2. Parameterize paths instead of reading env vars (`tagnet/src/paths.rs`)

**Why:** `paths.rs:8-20` reads `TAGNET_DATA_DIR` / `TAGNET_PRIVATE_KEY_FILE` and
`expect()`s them. Android has no shell env (app passes `filesDir`), and panics
would crash the app.

**Changes:**

- **Preferred:** replace the free functions with a `Paths` struct carrying
  `data_dir`, exposing `main_db_path()` / `sync_directory_db_path()` /
  `identity_path()` as methods; thread `&Paths` through call sites
  (`main.rs:39/157/173`, `directory_manager.rs`, `database.rs`).
- Convert `expect()` panics into returned errors.
- Each frontend supplies the data dir uniformly: desktop from env/XDG, Android
  from `getFilesDir()` via the bridge.

---

## 3. Add clean shutdown to `run`

**Why:** `run` currently blocks forever (`std::future::pending()`, `main.rs:231`)
or loops on `listener.accept()` (`main.rs:217`) with no clean stop. Needed for
the Android service lifecycle and for stopping the local control endpoint.

**Changes:**

- Introduce a shutdown primitive (`tokio_util::sync::CancellationToken` or
  `watch`/`oneshot`); pass into `run`.
- Wrap the accept loop, the `pending()` branch, and (later) the control endpoint
  in `tokio::select!` that also awaits shutdown, then returns cleanly.
- Cancel/drain spawned tasks (`handle_sync_directories`, `handle_changes`,
  `connect_to_peer`, `handle_connection`). Wire the watcher's existing
  `AtomicBool` stop flag (`watcher.rs`) to the same signal.
- Also a desktop improvement (Ctrl-C / systemd stop).

---

## 4. Build & toolchain plumbing (Android)

**Why:** cross-compile the nightly workspace for Android ABIs and link with NDK.

**Changes:**

- Add Android targets for the pinned nightly:
  ```
  rustup target add aarch64-linux-android    # primary
  rustup target add armv7-linux-androideabi   # optional 32-bit
  rustup target add x86_64-linux-android      # emulator
  ```
- Use **`cargo-ndk`** (or a `.cargo/config.toml` with per-target NDK clang
  linker/`CC`/`AR`). `cargo-ndk` copies `.so`s into `jniLibs/<abi>/`.
- `rusqlite` bundled compiles SQLite via the NDK C toolchain — confirm sysroot/
  `CC`/`AR` are picked up.
- Minimum API level: recommend 26+ (foreground service, getrandom). Set
  `ANDROID_NDK_HOME`.
- Optional: add `cargo-ndk` + NDK to the `flake.nix` dev shell for reproducible
  builds.

---

## 5. UI-facing API contract (transport-agnostic)

**Why:** the UI is rich — tag browsing, file upload + tagging, file/tag browsing.
This is a real query/command API, not just start/stop. It must be defined
**once** so the same Dart UI works over both transports.

This section is now **decided** (previously "to refine"). Scope is deliberately a
**v1**: it maps 1:1 onto capabilities that already exist in `FileDatabase` and
the change pipeline. Explicit non-goals for v1 are listed at the end.

### 5.1 Where it lives & the two-half split

- The API is one Rust surface defined in a shared module/crate, reusing
  `tagnet-core` types (`FileId`, `TagId`, `state::Change`, `SubtagRule`).
- It has a **read half** and a **write half**, because the core enforces a
  single-writer model:
  - **Reads** open their **own read-only** `Connection` from `main_db_path`,
    exactly as peer sessions do today (`lib.rs:574`). `FileDatabase` is
    `Send + !Sync`; a `&FileDatabase` is never held across `.await`.
  - **Writes** are expressed as `state::Change` values and pushed onto the
    existing ingest bus `change_sender`
    (`UnboundedSender<(Change, ChangeOrigin)>`) with
    `ChangeOrigin::Local { directory_path }`. The single `handle_changes` task
    (`lib.rs:1089`) remains the **only** DB writer and already does idempotent
    persistence + peer forwarding (`forward_to_peers`). **The API adds no new
    business logic and never writes the DB directly.**

### 5.2 Data types (serde-serializable)

JSON on the wire for IPC; the same structs are used directly in-process.

```rust
pub struct FileInfo {
    pub file_id: FileId,
    pub path: String,          // relative managed path
    pub content_hash: String,  // latest version hash (blake3 hex)
    pub version_number: i64,   // latest version
}

// Tag is reused from database.rs (id, name, color, metadata).
// metadata is always None in v1 (the tag::MetadataFormat API is todo!()).
```

### 5.3 Read API (own read-only connection)

- Tags: `list_tags() -> Result<Vec<Tag>, ApiError>` (backed by `get_all_tags`).
- Files: `list_files() -> Result<Vec<FileInfo>, ApiError>`.
- File tags: `tags_for_file(FileId, SubtagRule) -> Result<Vec<TagId>, ApiError>`
  (backed by `tag_ids_for_file`).
- Files for a tag:
  `files_for_tag(TagId, SubtagRule) -> Result<Vec<FileId>, ApiError>`
  (backed by `file_ids_for_tag`).

**Search in v1 is single-tag only** (`files_for_tag` above). Multi-tag
intersection/union and text search are **out of scope for v1** (see 5.6); they
require new query machinery that does not exist today.

**Requires two new `FileDatabase` read methods** (Section 5 is the reason to add
them):
- `get_all_files() -> Result<Vec<FileInfo>, DatabaseError>` — no file-listing
  method exists today.
- `path_for_file_id(FileId) -> Result<String, DatabaseError>` — only the reverse
  (`file_id_from_path`) exists today.

### 5.4 Write API (constructs `Change`, pushes onto `change_sender`)

Each call builds the matching `state::Change` and enqueues it; the response is
returned once accepted onto the bus (fire-and-forward, consistent with the
existing pipeline). Newly created ids are minted API-side (`FileId::new()` /
`TagId::new()`) and returned.

- `create_tag(name, color) -> Result<TagId, ApiError>` → `Change::TagAdded`.
- `delete_tag(TagId) -> Result<(), ApiError>` → `Change::TagRemoved`.
- `upload_file(path_name: String, content: Vec<u8>, tags: Vec<TagId>)
  -> Result<FileId, ApiError>` → `Change::FileAdded { file_id, path, content,
  content_hash, tags }`, where `content_hash = blake3(content)` (hex), matching
  `directory_manager.rs`. **Content is passed in memory as `Vec<u8>`** (same as
  `Change::FileAdded` today); streaming large media is deferred.
- `delete_file(FileId) -> Result<(), ApiError>` → `Change::FileDeleted`.
- `tag_file(TagId, FileId) -> Result<(), ApiError>` → `Change::FileTagged`.
- `untag_file(TagId, FileId) -> Result<(), ApiError>` → `Change::FileUntagged`.

**Not in v1:** tag rename/recolor and subtag management (`tag_tag`/`untag_tag`).
The DB supports them, but they're deferred to keep v1 focused (see 5.6).

### 5.5 Error model & event stream

- **`ApiError`** — one new enum, `#[derive(Debug, Serialize, Deserialize)]` +
  `Display` + `std::error::Error`. It wraps the existing hand-rolled errors and
  adds UI-facing variants, so the transport can serialize a single error type:
  ```rust
  pub enum ApiError {
      NotFound,            // unknown FileId/TagId
      InvalidArgument(String),
      Database(DatabaseError),
      Transport(String),   // IPC-only: socket/protocol failure
      Internal(String),    // e.g. a would-be panic path in handle_changes
  }
  ```
  Rationale: `DatabaseError` has no `Display` and flattens most SQL failures to
  `FailedToExecuteCommand`; leaking it raw to the UI is wrong, so it is wrapped
  rather than reused directly.

- **Event stream** — the UI subscribes to a stream of `Change` values. Delivery
  is **best-effort**, mirroring the in-process ingest bus (and, for IPC, the
  per-peer `outbound` fan-out used by `forward_to_peers`). There is **no
  per-event replay/buffering** in v1. On (re)connection over IPC the stream
  first emits a **`Resynced` marker**; the UI responds by re-fetching current
  state via the read API. This tolerates socket drops without a persistence/
  cursor mechanism.
  ```rust
  pub enum ApiEvent { Resynced, Changed(Change) }
  ```

### 5.6 Explicit v1 non-goals

- Multi-tag intersection/union queries and text/substring search (only
  single-tag lookup exists; AND-filtering is a manual free function today,
  `contains_all_tags`, `lib.rs`).
- Tag rename/recolor and subtag (`tag_tag`/`untag_tag`) management.
- Tag/file **metadata** — the entire `tag::MetadataFormat` API is `todo!()`.
- Event replay / guaranteed delivery / ordering cursors.
- Streaming upload/download of large files (v1 is in-memory `Vec<u8>`).

---

## 6. Transport abstraction (the linchpin for one UI codebase)

**Why:** Android is in-process; Linux is IPC-to-daemon. Keep the UI identical by
varying only the transport under the section-5 API.

**Design:**

- Define the section-5 API as a Rust trait/interface with **two backends**:
  1. **In-process backend (Android, and optional single-process desktop):**
     calls straight into `FileDatabase` / the change pipeline.
  2. **IPC-client backend (Linux daemon mode):** a thin embedded Rust client
     that connects to the daemon's control socket (section 7), serializes API
     calls, and returns results/events.
- **`flutter_rust_bridge` always targets this Rust API**, on both platforms. On
  Linux it wraps the IPC-client backend; on Android it wraps the in-process
  backend. **The Dart UI never knows which** — single UI codebase preserved.
- On desktop this also means **no JNI** and the Dart side never opens the DB or
  a socket directly; the embedded Rust client owns the connection.

---

## 7. Daemon local control endpoint (Linux)

**Why:** the separate desktop UI must reach the systemd daemon that owns the DB.

**Design:**

- Add a **Unix-domain-socket** control listener (e.g.
  `$XDG_RUNTIME_DIR/tagnet.sock`), **not** a TCP port.
  - Security via filesystem permissions: `$XDG_RUNTIME_DIR` is `0700`,
    user-owned -> only the owning user can connect; nothing is exposed on any
    network interface; **no separate auth handshake needed** for local control.
  - Reuse the existing WebSocket/`Frame` framing over `UnixListener` (tokio +
    tokio-tungstenite), so networking code stays unified.
- Add a **control/query message category** alongside the peer `Change`/`Sync`
  protocol (the peer `Frame` protocol is about *sync*; control is a distinct
  message set carrying the section-5 API requests/responses/events). Keep it
  logically separate from peer sync.
- A local UI client is conceptually "another kind of client" of the daemon —
  issuing queries/commands and subscribing to the change stream — reusing the
  broadcast plumbing (`forward_to_peers`, per-client outbound `UnboundedSender`).
- Wire the listener into the section-3 shutdown path.
- systemd: optionally socket-activate.

**`tagnet-cli` is the first consumer of the IPC-client backend.** Today the CLI
opens a WebSocket to the **peer-sync** port and hand-builds a `Change::FileAdded`
(`tagnet-cli/src/main.rs:40-75`) — duplicating exactly what `api::Api::upload_file`
now does, and currently **broken end-to-end** (it never performs the peer
handshake the daemon requires; see the comment at `tagnet-cli/src/main.rs:61-68`).
When this section lands, port the CLI to talk to the control socket via the
section-6 IPC-client backend and call the section-5 API (`upload_file`, and later
`list_tags`/`list_files`/etc.) instead of framing `Change`s itself. This both
fixes the CLI and validates the IPC-client backend with a minimal, UI-free
consumer. Do **not** wire the CLI onto the API over the peer-sync port — that
contradicts the rule below and would be thrown away.

**Unchanged:** the `0.0.0.0:{listen_port}` listener remains the remote
**peer-sync** port (`main.rs:210-227`). Do **not** route UI control through it.

**Remote control (rare, optional/future):** if the desktop UI ever controls a
*remote* daemon, that is a peer-like trust relationship — reuse the ed25519
handshake (`identity.rs`) + TLS over TCP. Not needed for the normal
UI-on-same-box case.

---

## 8. Android app scaffolding (Flutter)

- **Foreground service** hosting the native runtime with an ongoing
  notification, so the OS doesn't freeze the tokio runtime / inotify under Doze.
- **Bridge init:** on start, build the `tokio` runtime manually on a dedicated
  thread (not `#[tokio::main]`); call `tagnet::run(...)` with the in-process
  backend. On stop, trigger the section-3 shutdown and join. Route logs to
  logcat (`android_logger`) instead of `env_logger` (`main.rs:67`).
- **Permissions:** `INTERNET`, `FOREGROUND_SERVICE` (+
  `FOREGROUND_SERVICE_DATA_SYNC` on API 34+); media read perms only if section 9.
- **Storage:** synced dir + DBs under app-private storage (`getFilesDir()`) so
  inotify works with no storage permission.
- **Config + identity:** app generates the ed25519 identity on first launch
  (`identity.rs`, `OsRng` works via getrandom) and writes the `Configuration`
  JSON (peers, `listen_port`, `sync_directories`); reuse existing serde types.
  Note: add `Configuration::from_str` returning `Result` (current
  `Configuration::new` reads a file and unwraps, `configuration.rs:54`).
- **Lifecycle:** stop on service destroy; re-scan on resume — the existing
  initial-scan + `latest_content_hashes` diff (`main.rs:175`) catches changes
  missed while frozen.
- **Networking reality:** inbound `listen_port` is often unreachable behind
  carrier NAT; the outbound `connect_to_peer` path (`main.rs:198-208`) is more
  realistic on mobile; `listen_port = None` already supports outbound-only
  (`main.rs:228`).

---

## 9. (Optional) Shared-media syncing via MediaStore (Android)

**Why:** inotify can't reliably watch `/sdcard`. Only way to sync
Pictures/Screenshots/DCIM. Skip unless wanted.

**Changes:**

- Flutter/Android side: request `READ_MEDIA_IMAGES` / `READ_MEDIA_VIDEO`
  (API 33+) or `READ_EXTERNAL_STORAGE` (<=32); register a MediaStore
  `ContentObserver`.
- On callback -> bridge `rescan(path)` -> Rust rescans that subtree using the
  **existing** scan + `latest_content_hashes` diff (`main.rs:175`,
  `directory_manager.rs`). Needs an "externally injected rescan" entry point that
  pushes a `SyncDirectoryCommand` into `command_sender` (`main.rs:180`).
- SAF / `content://` trees are unsupported for watching; inotify-watched dirs
  must be real app-private paths.

---

## Suggested implementation order

1. **Sections 1, 2, 3** (lib/bin split, paths, shutdown) — pure refactors,
   verifiable on desktop with existing behavior intact.
2. **Section 5** (define the UI-facing API) + **Section 6** (transport
   abstraction) with the **in-process backend first**, exercised by the desktop
   binary or a tiny harness.
3. **Section 7** (daemon Unix-socket control endpoint) + the IPC-client backend
   -> desktop UI can attach to the systemd daemon.
4. **Section 4** (Android build plumbing) then **Section 8** (Flutter app +
   foreground service) -> first "runs on device" milestone with an app-private
   synced dir, reusing the same section-5 API over the in-process backend.
5. **Section 9** (MediaStore) — later, only if shared-media sync is wanted.

## Explicitly out of scope

- Syncing other apps' private directories (Signal, WhatsApp internal) — requires
  root.
- Play Store distribution / `MANAGE_EXTERNAL_STORAGE` policy — sideloaded
  personal app only.
