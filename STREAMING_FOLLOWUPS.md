## 1. Correctness gaps (should fix before relying on this in anger)

### 1.1 `tagnet u` blocks forever with no connected storing peer — HIGH
`tagnet u <file>` now blocks until the daemon reports the content was handed off
to a peer (`ApiEvent::ProviderReleased`). If **no** storing peer is ever
connected, it blocks indefinitely.

- Ctrl-C is clean: on disconnect the daemon unregisters the provider
  (`handle_control_connection` teardown in `control.rs`).
- Missing: a `--detach` flag and/or a timeout so the CLI can give up (or
  background itself) instead of hanging.
- Decision needed: what does "upload succeeded" mean with zero peers? Options
  discussed: block (current), spool to a daemon-owned store, or define done as
  "daemon accepted".

### 1.2 Provider releases after the FIRST completed transfer only — HIGH
The temporary provider is released (and the CLI unblocked) as soon as **one**
peer completes pulling the file (`ProviderSource` fires `on_complete` on the
last chunk). Correct for a strict hub-and-spoke topology (PC → central), but if
a device is directly connected to **multiple** storing peers, the second peer
can be stranded.

- Proper fix: track the outstanding set of directly-connected storing peers and
  release only when it drains. Needs per-transfer completion reporting from each
  peer session back to the provider registry.

### 1.5 On-demand fetch has no hop-level timeout once `FetchFound` arrives — MEDIUM
`FetchFound` is now content-less: it removes the pending-fetch entry
(`fetch.rs`, first-wins) and triggers a pull transfer. `arm_hop_timeout`
(`HOP_TIMEOUT = 8s`) only covers the pre-`FetchFound` flooding phase; once the
entry is removed, a stalled *transfer* has no per-hop timeout and relies solely
on the CLI's overall `FETCH_TIMEOUT` and transfer channel-close.

- Fix: give the fetch transfer its own timeout, or re-arm a deadline covering
  the transfer phase.

---

## 2. Robustness / shutdown-safety

### 2.1 `handle_command` / `handle_event` now `.await` — violates the sync invariant — MEDIUM
`directory_manager.rs` has a `DANGER` note (around line 1200): the sync-directory
`run` loop is cancelled abruptly on shutdown, and `handle_command` /
`handle_event` must stay fully synchronous so a shutdown can only land *between*
whole events, never mid-handler.

The streaming work introduced `.await`s inside these handlers (streaming
`materialize_to`, `get_file_content`). A shutdown can now interrupt a handler
mid-way (e.g. between materialize and the DB `add_file`), leaving on-disk state
inconsistent.

- Fix: make shutdown cooperative in that loop (observe cancellation at the top
  of the loop and return normally) instead of relying on abrupt drop, then the
  `.await`s are safe. Update the `DANGER` note accordingly.

### 2.2 Provider chunk protocol assumes one in-flight upload per control connection
`ControlFrame::ProviderChunkRequest` carries no `file_id`; the CLI serves from a
single `provider_path`. A connection uploading/editing more than one file
concurrently would misroute chunks. The current CLI does one at a time and
blocks, so this holds — but it is an unenforced invariant. Either enforce it or
key provider chunks by `file_id`.


### 4.5 `skip_queue` replaced by path-keyed self-write suppression — DONE
The directory manager's `skip_queue` predicted an exact `DebouncedEventKind` and
matched it verbatim, which was brittle against the debouncer (a materialize is a
rename the watcher reports as `Move`, not the predicted `Create`; create+modify
and other pairs coalesce). It was also only *partly* redundant with the
"already-tracked ⇒ ignore" guard: that DB check disambiguated the *ingest* path
(Create / move-in) but could not tell a self-caused `Modify`/`Remove` from a
real user edit/delete on a tracked path.

Both mechanisms are now unified into a single `self_writes` map
(`directory_manager.rs`), keyed by absolute on-disk path and consumed on the
first matching watcher event:

- Keyed by *path*, so it is invariant to how the OS/debouncer labeled the event
  (the brittleness that motivated the original "exit instead" fix).
- Records the written content's BLAKE3 hash, so a self-caused `Modify` is
  suppressed only when the on-disk bytes match what the daemon wrote; a genuine
  user edit hashes differently and is propagated. This is the case the DB-only
  check could not handle, so `skip_queue` could finally be removed entirely
  rather than kept for `Modify`/`Remove`.

Note: this is *not* DB-based (the earlier suggestion): the per-sync-directory DB
stores only `id`/`physical_path`, no content hash, and self-write intent is
transient rather than durable file state, so it lives in the manager.

### 4.6 Control socket path is a hard constant
`CONTROL_SOCKET_PATH` is fixed by deliberate design (see its doc), but it blocks
running multiple daemons on one host, which the tests in §3 need. If a test-only
override is added, keep it clearly test-scoped so the production invariant
holds.

---

## 5. Deferred by design (not bugs)

- **Peer transfer multiplexing / throughput**: the pull protocol does one
  round-trip per chunk with a small window (`WINDOW`, `CHUNK_SIZE` in
  `transfer.rs`). Fine for correctness; a larger window or a dedicated data
  channel is a throughput optimization for later.
- **Live-push pull is via reconciliation + the per-peer command channel**; there
  is no eager byte push on a content change (announce-then-pull, Option A). This
  is intended.
- **No daemon-owned content store**: uploads are served from the CLI provider;
  received files live in sync directories or transfer temp files. Intentional.
