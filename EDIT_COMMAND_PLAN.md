# `tagnet-cli edit <uuid>` — Implementation Plan

## Goal

Add `tagnet-cli edit <uuid>` that opens a file in `$EDITOR` and, if the
content changed, persists the edit.

Two cases:

1. **File is already on the device** (present in a watched sync directory):
   open the real file in place. The existing filesystem watcher picks up the
   save and emits `Change::FileChanged` automatically. No temp file, no
   explicit write-back.
2. **File is not on the device**: fetch its bytes from a peer via a recursive
   flood, write them to a **temp file**, open `$EDITOR`, and on exit — only if
   the content hash changed — explicitly enqueue `Change::FileChanged`.

## Key facts established during exploration

- **Tech stack:** Rust (edition 2024), Cargo workspace, `clap` CLI, `tokio`
  async, SQLite (`rusqlite`), `blake3` hashes, WebSocket peer sync, Unix-socket
  control protocol between CLI and daemon.
- **Layers (mirrored 1:1):** CLI → `TransportBackend` trait → `InProcessBackend`
  / `IpcClientBackend` → `Backend` enum → control wire protocol
  (`ControlRequest`/`ControlResponse`) → core `Api`.
- **`Api` is `!Sync`, single-writer.** All DB writes happen in the
  `handle_changes` task. `Api` writes are fire-and-forget: they enqueue a
  `Change` on `change_sender` and return immediately.
- **`ReadFile` is internal.** `SyncDirectoryCommand::ReadFile`
  (`directory_manager.rs:76`, handled at `:832`) reads **local disk only** to
  answer an incoming peer `Sync::Request`. It is *not* a fetch-from-peer API and
  is not exposed to CLI/API.
- **Peer fetching today is automatic and connection-time only.** On connect,
  peers exchange `Sync::Manifest`; each side sends `Sync::Request` for files it
  is missing/behind on (`reconcile_peer_manifest`, `lib.rs:935`); the peer
  answers with `Change::FileAdded` carrying bytes. There is **no on-demand
  single-file fetch** and **no way to route a response back to a waiting
  caller**.
- **`forward_to_peers`** (`lib.rs:1180`) already broadcasts a `Frame` to all
  connected peers, skipping the origin. Reusable for the flood.
- **`FileInfo` already carries `content_hash`** (`tagnet-core/src/lib.rs:233`,
  via `list_files` / `api.rs:169`). The CLI can obtain the expected hash for a
  fetch from existing metadata — no new call needed for that.
- **`ControlFrame` already correlates request/response via `id`**
  (`control.rs:147`).
- **`FileChanged` daemon handling already exists** (`lib.rs:1506-1575`): records
  a version, dispatches bytes to matching sync directories, forwards to peers.
- **`dispatch` is synchronous and holds only `&Api`** (`control.rs:325`). It has
  no access to `runtime_configuration`, `command_sender`, or peer sessions.

## Architecture decisions (agreed)

1. **Local case opens the real file directly** — no temp file, no write-back;
   the watcher handles propagation.
2. **Write-back only if the content hash changed** (blake3 of original vs edited).
3. **Remote fetch model = recursive request over an acyclic peer tree** ("call
   stack across machines"), *not* a broadcast-with-dedup. Each node forwards the
   request to all neighbours **except** the one it came from; there is no
   seen-set because the graph is assumed acyclic (live-connection tree).
4. **First `Found` wins — return immediately.** The `children_outstanding` set
   exists only to decide when to report the **negative** answer (`Missing`):
   report `Missing` upward only when *all* children have returned `Missing` or
   timed out.
5. **Per-hop timeouts.** Every relaying node has its own timeout on outstanding
   children so one dead branch cannot stall the chain; the CLI also has an
   overall timeout.
6. **Fetched-for-edit bytes go to a temp file** (not persisted into a sync
   directory). This decouples fetch from sync-directory topology. Therefore the
   `edit_file` API (explicit `FileChanged`) is required.
7. **Do not break the control/peer separation.** The control layer continues to
   touch the daemon **only through `Api`**. The fetch is enqueued as a
   request-reply message carrying a `oneshot::Sender` (same idiom as
   `SyncDirectoryCommand::ReadFile { respond_to }`). The recursive engine and
   pending table live entirely in the peer world (`handle_changes` / peer
   sessions).
8. **One channel + enum (not two channels).** The existing `change_sender` bus
   becomes an enum carrying both the current fire-and-forget change and the new
   fetch request. Rationale:
   - **Ordering:** a single FIFO preserves the order of a client's messages
     (e.g. an edit followed by a fetch of the same file). Two channels +
     `select!` can reorder them.
   - **Matches existing idiom:** `Frame`, `SyncMessage`, `ControlRequest` are
     already single enums over heterogeneous messages on one path.
   - **Less plumbing / simpler shutdown:** `change_sender` is cloned through
     ~10 sites and drained by a single `drop()` (`lib.rs:324`); reuse it rather
     than duplicating the wiring and teardown.
   - Cost is a one-time mechanical refactor (wrap enqueues in a variant, add an
     outer `match` in `handle_changes`).

## Wire protocol changes (`tagnet-core/src/lib.rs`, `Sync` enum ~line 205)

Extend the `Sync` message enum:

```rust
pub enum Sync {
    Manifest { entries: Vec<ManifestEntry> },

    // Existing reconciliation request/notfound (keep as-is, file-keyed):
    Request { file_id: FileId },        // NOTE: see below
    NotFound { file_id: FileId },

    // New on-demand fetch messages (request_id-keyed):
    FetchRequest { request_id: RequestId, file_id: FileId, expected_hash: String },
    FetchFound   { request_id: RequestId, file_id: FileId, content: Vec<u8>, content_hash: String },
    FetchMissing { request_id: RequestId },
}
```

- Add a `RequestId` newtype (uuid) via the existing `make_id_type!` macro
  (`tagnet-core/src/lib.rs:237`).
- The new fetch messages are kept **separate** from the existing
  `Request`/`NotFound` used by manifest reconciliation, to avoid disturbing that
  flow. (Alternatively, add `expected_hash`/`request_id` to `Request` and reuse
  it — decide at implementation time; separate variants are the lower-risk
  default.)

## Daemon: recursive fetch engine + shared pending table

Lives in the peer world (`tagnet/src/lib.rs`), NOT in the control layer.

### Shared pending-request table

A structure shared across all peer sessions and `handle_changes` (candidate
home: alongside `RuntimeConfiguration` in `Arc<RwLock<...>>`, or a dedicated
`Arc<Mutex<HashMap<RequestId, PendingFetch>>>`):

```rust
struct PendingFetch {
    file_id: FileId,
    expected_hash: String,
    // Where to send the answer:
    //   - Peer(public_key): relay FetchFound/FetchMissing back to this parent.
    //   - Local(oneshot::Sender<Result<Vec<u8>, FetchError>>): answer the
    //     waiting control/CLI caller.
    reply_to: FetchReplyTarget,
    children_outstanding: HashSet<PeerPublicKey>,
    deadline: Instant,
}
```

### Behaviour

- **On local fetch request** (from the bus enum, see below): create a
  `RequestId`, insert a `PendingFetch` with `reply_to = Local(oneshot)`, check
  locally first (hash-gated `ReadFile`); if found, answer immediately. Otherwise
  broadcast `Sync::FetchRequest` to all connected peers (reuse the
  `forward_to_peers` pattern), record them as `children_outstanding`, arm the
  per-hop timeout.
- **On inbound `Sync::FetchRequest` (from peer P):**
  - Hash-gated local check (extend the `ReadFile` path / `build_request_response`
    at `lib.rs:1074` to compare `expected_hash`). If we have matching bytes →
    send `Sync::FetchFound` back to P.
  - Else insert `PendingFetch { reply_to = Peer(P), ... }`, forward
    `Sync::FetchRequest` to all connected peers **except P**, arm timeout.
- **On inbound `Sync::FetchFound`:** look up `request_id`.
  - Resolve immediately (first-wins). Deliver via `reply_to`:
    `Peer(parent)` → send `Sync::FetchFound` to parent; `Local(oneshot)` →
    `oneshot.send(Ok(bytes))`.
  - Remove the pending entry. Any later `FetchFound`/`FetchMissing` for the same
    `request_id` is dropped (no entry).
- **On inbound `Sync::FetchMissing` / child timeout:** remove that child from
  `children_outstanding`. If empty (all children exhausted) → deliver the
  negative answer via `reply_to` (`Sync::FetchMissing` to parent, or
  `oneshot.send(Err(NotFound))`), remove the entry.
- **Overall per-hop timeout:** on `deadline`, treat all still-outstanding
  children as `Missing`, deliver the negative answer, remove the entry.

### Cross-session routing note

Peer sessions are spawned per connection (`run_peer_session`, `lib.rs:602`),
each with its own inbound loop. A `FetchFound` arriving on peer B's session must
reach a pending entry created on peer A's session — hence the **shared** pending
table. Relayed outbound frames must be sent via the registered `outbound` sender
in `RuntimeConfiguration` (so that even "inbound-only" sibling sessions,
`lib.rs:662`, route correctly), not via a session-local sink.

## Bus enum (one channel + enum)

Change the `change_sender` / `change_receiver` payload from
`(Change, ChangeOrigin)` to an enum, e.g.:

```rust
enum DaemonMessage {
    Change(Change, ChangeOrigin),
    Fetch {
        file_id: FileId,
        expected_hash: String,
        respond_to: oneshot::Sender<Result<Vec<u8>, FetchError>>,
    },
}
```

- Update all `change_sender.send(...)` sites to wrap in
  `DaemonMessage::Change(...)` (`api.rs`, peer sessions at `lib.rs:745`, sync
  directories).
- `handle_changes` gains an outer `match`:
  - `DaemonMessage::Change(...)` → existing logic unchanged.
  - `DaemonMessage::Fetch { .. }` → seed the pending table with a local-origin
    entry and kick off the recursive engine (has `runtime_configuration` +
    peer `outbound` access already).
- Shutdown teardown (`lib.rs:324`) unchanged: still a single `drop`.

## Core `Api` additions (`tagnet/src/api.rs`)

1. `fetch_file(&self, file_id, expected_hash) -> Result<Vec<u8>, ApiError>`
   (async): create a `oneshot`, send `DaemonMessage::Fetch { .., respond_to }`,
   await the oneshot with an overall timeout, map to `ApiError`.
2. `edit_file(&self, file_id, content) -> Result<(), ApiError>`: compute blake3
   hash and enqueue `Change::FileChanged { file_id, content, content_hash }`
   (mirrors `upload_file` at `api.rs:230`; daemon handling already exists at
   `lib.rs:1506`).
3. `local_path_for_file(&self, file_id) -> Result<Option<PhysicalPath>, ApiError>`
   (read-only): resolve whether the file is present in a local sync directory
   and where, so the CLI can decide local-vs-remote and open the real file.
   Backed by the existing `ReadFile`-style lookup (path only).

## Control protocol (`tagnet/src/control.rs`)

Add to `ControlRequest` (~line 100):

- `FetchFile { file_id, expected_hash }` → `api.fetch_file(...).await`. On
  success the bytes are returned to the CLI (see response below). This makes the
  `FetchFile` dispatch arm **async**; `dispatch` becomes async (or that arm
  awaits). It still touches **only `api`** — no `runtime_configuration`.
- `LocalPathForFile { file_id }` → `api.local_path_for_file(...)`.
- `EditFile { file_id, content }` → `api.edit_file(...)`.

Add to `ControlResponse` (~line 128):

- `FileContent(Vec<u8>)` — bytes returned by `FetchFile`.
- `LocalPath(Option<PhysicalPath>)` — for `LocalPathForFile`.
- Reuse `Ok` for `EditFile`, `Error(ApiError)` for failures/timeouts.

Mirror each new request through:

- `TransportBackend` trait (`transport.rs` ~line 97).
- `InProcessBackend` impl (`transport.rs` ~line 240).
- `Backend` enum dispatch (`transport.rs` ~line 350).
- `IpcClientBackend` impl (`control.rs` ~line 637).

## CLI (`tagnet-cli/src/main.rs`)

1. Import `FileId` (currently only `TagId` is imported, `main.rs:20`).
2. Add `tempfile` dependency to `tagnet-cli/Cargo.toml`.
3. Add `Commands::Edit { uuid: String }` (`main.rs:34`) and a `run()` arm
   (`main.rs:85`):

```
parse FileId::from_string(uuid)  -> error on invalid
ask daemon: LocalPathForFile(file_id)
if Some(path):
    spawn $EDITOR (fallback vi) on the REAL path, blocking; done.
    (watcher emits FileChanged on save)
else:
    look up content_hash from list_files (expected hash)
    bytes = FetchFile { file_id, expected_hash }   // may time out -> clear error
    write bytes to a NamedTempFile
    original_hash = blake3(bytes)
    spawn $EDITOR on the temp file, blocking
    new_bytes = read temp file
    if blake3(new_bytes) != original_hash:
        EditFile { file_id, content: new_bytes }
        print "Edited <uuid>"
    else:
        print "No changes"
```

- Editor spawn: `std::process::Command::new(editor).arg(path).status()`,
  `editor = std::env::var("EDITOR").unwrap_or("vi")`.

## Error handling / edge cases

- **No peer has the file / all offline:** `FetchFile` times out → CLI prints a
  clear error ("file not available from any peer") and exits non-zero.
- **Invalid UUID:** CLI error before any daemon call.
- **`$EDITOR` unset:** fall back to `vi`.
- **Editor exits non-zero:** treat as abort — do not write back.
- **Local file lookup finds a DB row but missing bytes on disk:** treated as
  not-local (fall through to remote fetch) or surfaced as an error (decide at
  implementation).
- **Concurrency between fetch and write-back:** out of scope for v1; writes are
  fire-and-forget (enqueue only). Document as a known limitation.

## Implementation phases

1. **Wire + daemon engine (first, for review):**
   - `RequestId` newtype + new `Sync` fetch variants (`tagnet-core`).
   - `DaemonMessage` bus enum + update all send sites + `handle_changes` outer
     `match`.
   - Shared pending table + recursive engine + per-hop timeouts + hash-gated
     local check in the peer world (`tagnet/src/lib.rs`).
2. **Core `Api`:** `fetch_file`, `edit_file`, `local_path_for_file`.
3. **Control protocol:** `FetchFile`, `LocalPathForFile`, `EditFile` +
   responses, mirrored through transport/backend/IPC layers.
4. **CLI:** `Edit` command, tempfile + `$EDITOR`, local-vs-remote flow.

## Verification

- `cargo build` / `cargo clippy` clean across the workspace.
- Manual: two connected daemons, edit a locally-present file (watcher path);
  edit a file present only on the peer (fetch → temp → edit → `FileChanged`
  propagates back).
- Timeout path: edit a UUID no connected peer holds → clear error, non-zero exit.
