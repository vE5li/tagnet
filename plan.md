# Plan: File Size Metadata + Deletion Sync

Two changes, both touching (one required, one shares) the database. Since only
the author's own devices use tagnet, migrations are **hardcoded and temporary**:
they run idempotently on startup and get deleted once every device is updated.

## Guiding decisions

- **Size type:** `i64` in SQLite (its native integer), `u64` on the wire and in
  `FileInfo`. Cast at the DB boundary. `FileBytes::byte_len()` already returns
  `u64` (`file_bytes.rs:130`).
- **Size is per-version:** every `file_versions` row carries its own `size`.
  Manifest history tuples expand to include it.
- **Deletions use tombstones:** mirroring the existing soft-delete pattern on
  `entries` (`database.rs:308-326`), which already works correctly.
  - **Files** need `deleted` + `deleted_at`: files have no single `modified_at`
    (their LWW timestamp is `latest_observed_at`, derived from `file_versions`),
    and a delete creates no version row, so the delete time needs its own column.
  - **Tags** need only `deleted`: they already carry `modified_at` for LWW, so a
    delete just sets `deleted=1, modified_at=now` — exactly like the relationship
    tombstones in `untag_entry` (`database.rs:1137`). No `deleted_at`.
- **Column constraints:** in `CREATE TABLE`, all new columns are `NOT NULL` with
  **no `DEFAULT`** — omitting a value is an error (a 0-byte file stores
  `size = 0`, which is a real value and passes `NOT NULL`). `DEFAULT` is used
  **only** in the temporary `ALTER TABLE` migration to backfill existing rows.
- **`record_version` gains a required `size` param** so omitting size is a Rust
  compile error, stronger than a runtime SQL error.

---

## Migration mechanism (hardcoded, temporary)

There is currently **no** migration mechanism (no `PRAGMA user_version`, no
`ALTER TABLE`; schema is `CREATE TABLE IF NOT EXISTS` + `DEFAULT`ed columns).

Add a minimal idempotent block in `FileDatabase::initialize`
(`database.rs:261`), after the `CREATE TABLE IF NOT EXISTS` calls:

- `PRAGMA table_info(file_versions)` — if `size` is absent:
  `ALTER TABLE file_versions ADD COLUMN size INTEGER NOT NULL DEFAULT 1`.
  (Existing rows backfill to `1`; files get correct sizes as they are modified.)
- `PRAGMA table_info(files)` — if absent:
  `ALTER TABLE files ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE files ADD COLUMN deleted_at INTEGER NOT NULL DEFAULT 0`.
- `PRAGMA table_info(tags)` — if absent:
  `ALTER TABLE tags ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0`.
  (Tags reuse the existing `modified_at` for LWW; no `deleted_at`.)

Mark the whole block `// TODO: DELETE after all devices migrated`. Idempotent
and safe to run every startup. After deletion, a freshly created DB uses the
no-default `CREATE TABLE` definitions; already-migrated DBs keep their columns.

---

## Feature 1: File size in metadata

Size rides the same rails as `content_hash`:
`ContentChange` (bus) -> `Change::FileMetadataAdded/Changed` (wire) ->
`record_version` (DB) -> `ManifestEntry` history (wire) -> `FileInfo`.

### Schema
- `file_versions` (`database.rs:353-365`): add `size INTEGER NOT NULL`
  (no default in `CREATE TABLE`).

### Database layer (`database.rs`)
- `FileVersion` struct (`:35`): add `pub size: i64`.
- `record_version` (`:387`): add `size: i64` param; include in INSERT.
- `latest_version` (`:482`): select `size`.
- `version_history` (`:518`): return `(version_number, content_hash, size)`.
- `manifest_entries` (`:551`): carry per-version size in history.
- `file_info_from_id` (`:1778`) and `get_all_files` (`:1824`): select `size`.

### Capture size at hash chokepoints (via `FileBytes::byte_len()`)
- `get_file_content` (`directory_manager.rs:491`): return
  `(FileBytes, String, u64)`; update ~10 call sites in that file.
- `transport.rs:392,404` (API upload/edit): call `source.byte_len()`.
- `control.rs:hash_file` (`:932`): also return size; plumb through the upload/
  edit control frames (`control.rs:144,152,577,598`).

### Bus (`bus.rs`)
- `ContentChange::FileAdded`/`FileChanged` (`:64`): add `size: u64`.
- `AnnounceProvided` (`:158`): add `size: u64`.
- `CatalogFile` (`:144`): carry size where a version is recorded.

### Wire (`tagnet-core/src/lib.rs`)
- `Change::FileMetadataAdded` (`:103`) and `FileMetadataChanged` (`:124`):
  add `size: u64`.
- `ManifestEntry.history` (`:234`): `Vec<(i64, String, i64)>`
  = `(version_number, content_hash, size)`.
- `FileInfo` (`:424`): add `pub size: u64`.

### Daemon threading (`lib.rs`)
- `handle_changes` `record_version` call sites (`:2622, 2751, 2811, 3028, 3176,
  3276, 3350`): pass size.
- Reconcile receive path (`CatalogFile`, `reconcile_peer_manifest`): record the
  per-version size from history.

### Downstream consumers
- `tagnet-bridge/src/api.rs:41` (Flutter FFI `FileInfo` mirror): add `size`.
- `tagnet/src/main.rs:44` (CLI file struct): add `size`, optionally display it.

---

## Feature 2: Sync deletions for files and tags

Currently hard deletes with no tombstones -> deletions resurrect after an
offline peer reconnects. Relationship edges (`entries.deleted`) already do this
correctly; copy that pattern.

### Files
- Schema `files` (`database.rs:273`): add `deleted INTEGER NOT NULL`,
  `deleted_at INTEGER NOT NULL` (no default in `CREATE TABLE`).
- `remove_file` (`:925`): soft delete -> `UPDATE files SET deleted=1,
  deleted_at=?`.
- All live reads filter `deleted=0`: `get_all_files`, `file_info_from_id`,
  `file_exists`, etc.
- `manifest_entries` (`:551`): **include** deleted files; carry `deleted` +
  `deleted_at`. Add these fields to `ManifestEntry` (`tagnet-core:234`).
- `Change::FileDeleted` (`tagnet-core:133`): add `deleted_at: i64`.
- `handle_changes` `FileDeleted` arm (`lib.rs:3456`): soft-delete + forward.
- Reconcile (`reconcile_peer_manifest` `:1704`, `decide_request` `:1810`):
  LWW between peer `deleted_at` and local latest version `observed_at`.
  - Delete newer than local latest edit -> apply delete.
  - Local edit (`observed_at`) newer than peer `deleted_at` -> file restored.
  - A stale peer re-announcing a regular version loses to our newer tombstone.

### Tags
Tags already carry `modified_at` for LWW, so a delete just bumps it — no
`deleted_at` column (unlike files).
- Schema `tags` (`database.rs:292`): add `deleted INTEGER NOT NULL` (no default
  in `CREATE TABLE`).
- `remove_tag` (`:1035`): soft delete -> `UPDATE tags SET deleted=1,
  modified_at=?` (LWW-gated, mirroring `untag_entry` at `:1137`).
- Live reads filter `deleted=0`.
- `tag_manifest_entries` (`:594`): include deleted tags; carry `deleted` in
  `TagManifestEntry` (`tagnet-core:258`). `modified_at` is already present.
- `Change::TagRemoved` (`tagnet-core:166`): add `modified_at: i64` (the delete's
  timestamp; no separate `deleted_at`).
- `handle_changes` `TagRemoved` arm (`lib.rs:3607`): soft-delete + forward.
- `reconcile_peer_tag_manifest` (`:2038`): the existing `modified_at` comparison
  already drives this; when the peer's entry is newer and `deleted=1`, apply the
  delete. When our live edit is newer, the tag stays (resurrected).

### Note
No tombstone GC exists (`database.rs:1134`). Tombstones accumulate; acceptable
short-term. GC can be added later.

---

## Work order

1. Migration scaffolding + schema columns (shared by both features).
2. Feature 1 (size) end-to-end — mechanical threading, no protocol semantics.
3. Feature 2 files — soft delete + reconcile LWW.
4. Feature 2 tags — copy the file pattern.
5. FFI (`tagnet-bridge`) + CLI (`tagnet`) `FileInfo.size` mirror + display.
6. Tests:
   - size round-trips through `record_version` / manifest history / `FileInfo`;
   - delete-vs-edit LWW both directions, for files and tags.

---

## Files touched (summary)

- `tagnetd/src/database.rs` — schema, migration, `record_version`,
  `FileVersion`, `latest_version`, `version_history`, `manifest_entries`,
  `tag_manifest_entries`, `remove_file`, `remove_tag`, read filters,
  `file_info_from_id`, `get_all_files`.
- `tagnet-core/src/lib.rs` — `Change` variants (`size`, `deleted_at`),
  `ManifestEntry` (history tuple + `deleted`/`deleted_at`), `TagManifestEntry`
  (`deleted`/`deleted_at`), `FileInfo` (`size`).
- `tagnetd/src/bus.rs` — `ContentChange` (`size`), `AnnounceProvided`,
  `CatalogFile`.
- `tagnetd/src/lib.rs` — `handle_changes` arms (size threading, delete LWW),
  `reconcile_peer_manifest` / `decide_request`, `reconcile_peer_tag_manifest`.
- `tagnetd/src/directory_manager.rs` — `get_file_content` returns size; ~10
  call sites.
- `tagnetd/src/transport.rs`, `tagnetd/src/control.rs`, `tagnetd/src/api.rs` —
  size capture + plumbing.
- `tagnet-bridge/src/api.rs`, `tagnet/src/main.rs` — `FileInfo.size` mirror +
  display.
