# SHARED_CODE.md — the copy ledger

Code in this repo that was **copied from `file-monitor`** rather than written
here. When `file-monitor` fixes a bug in one of these, it has to be carried
across by hand — there is no shared package. Keep this table honest.

Pin: `libs/nexus-proto` is a submodule pinned to **`cef65a6`**, the same
commit `file-monitor` uses. A mismatched pin means a mismatched `proto_hash`
and nothing connects. Check with `git submodule status`.

## Verbatim (only `file_monitor` → `session_manager` in imports)

| File | Source | Notes |
|---|---|---|
| `src/session_manager/supervision/supervisor.py` | `file-monitor` | `ProcessSupervisor` — opaque `ChildSpec`s, per-child backoff, sliding-window crash-loop → `DEGRADED`, `SIGTERM`-then-`SIGKILL` shutdown. Knows nothing about senders/receivers. |
| `src/session_manager/supervision/constants.py` | `file-monitor` | backoff schedule, crash-loop window, shutdown timeout. |
| `src/session_manager/adapters/system_clock.py` | `file-monitor` | `SystemClock` — `time.monotonic()` + `asyncio.sleep`. |
| `src/session_manager/adapters/blake3_hasher.py` | `file-monitor` | `Blake3Hasher` — chunked file hash on `asyncio.to_thread`. Same BLAKE3 as `handshake.py`; never introduce a second hash algorithm for the same kind of check. |
| `src/session_manager/adapters/asyncio_process_spawner.py` | `file-monitor` | `AsyncioProcessSpawner` — `ProcessSpawner` over `asyncio.subprocess`. |
| `src/session_manager/adapters/constants.py` | `file-monitor` | `CHUNK_SIZE`. |
| `tests/conftest.py` | `file-monitor` | Puts `libs/nexus-proto/generated/python` on `sys.path` (flat proto modules). |
| `tests/fakes/fake_clock.py` | `file-monitor` | `FakeClock` — manually advanced; `sleep` advances time. |
| `tests/fakes/fake_spawner.py` | `file-monitor` | `FakeSpawner` / `FakeProcess` — spawn-failure injection, kill/terminate, `reaped` → `ProcessLookupError`. |
| `tests/fakes/fake_hasher.py` | `file-monitor` | `FakeHasher` — fixed digest. |
| `tests/unit/test_supervisor.py` | `file-monitor` | Supervisor + `FakeSpawner`/`FakeClock`: backoff schedule, spawn-failure retry, degraded-and-stop, shutdown leaves nothing alive, **backoff interrupted by the stop event**. |

## Copied and adapted

`file-monitor` is the TX side (senders send `SenderHello`, keyed by
`SenderId`); this is the RX side (receivers send `ReceiverHello`, keyed by
`ReceiverId`). The IPC layer was renamed `Sender`→`Receiver` /
`sender_id`→`receiver_id` / `sender_hello`→`receiver_hello` throughout, and
the codec was repointed from `ipc_pb2.Envelope` to `rx_pb2.RxEnvelope`.

| File | Source | What changed |
|---|---|---|
| `src/session_manager/ipc/constants.py` | `ipc/constants.py` | Buffer/queue sizes and `ENVELOPE_ONEOF_GROUP_NAME` kept; the `SENDER_*` oneof field-name constants replaced with the `RxEnvelope` ones (`RECEIVER_HELLO_FIELD_NAME`, `BLOCK_DECODED_FIELD_NAME`, …). |
| `src/session_manager/ipc/codec.py` | `ipc/codec.py` | Wraps `rx_pb2.RxEnvelope`. `WhichOneof` group is still `"msg"`. `ValueError` text is "RxEnvelope has no message set". |
| `src/session_manager/ipc/message_types.py` | `ipc/message_types.py` | `FIELD_NAME_BY_MESSAGE_TYPE` maps the `RxEnvelope` payload types (`ReceiverHello`, `ManifestSeen`, `BlockDecoded`, `ReceiverStats`, `Config`, `SessionOpen`, `PurgeSession`) plus `ipc_pb2.Heartbeat`, which `RxEnvelope` reuses from the `ipc` package. |
| `src/session_manager/ipc/uds.py` | `ipc/uds.py` | `SenderId`→`ReceiverId`; handshake accepts `receiver_hello` and reads `hello.receiver_id`; log keys renamed. **The three hard-won behaviours below are unchanged in shape** — do not "simplify" them. |
| `src/session_manager/ipc/errors.py` | `ipc/errors.py` | `UnknownSenderError`→`UnknownReceiverError`; `sender_id`→`receiver_id` in every message and attribute; `HandshakeError` says "expected receiver_hello". |
| `src/session_manager/ipc/handshake.py` | `ipc/handshake.py` | `verify_proto_hash` parameter `sender_id`→`receiver_id`; docstring says "the Python services (file-monitor, session-manager)". The **algorithm is byte-identical** — the C++ side implements from that prose. |
| `src/session_manager/ports/protocols.py` | `ports/protocols.py` | Seeded from the copy; `IpcServer` speaks `ReceiverId`. Step 2 added invariant docstrings to every port and the RX-only ports `ShmReader` / `ShmWriter` (split so a reader can never hold a writable view into receiver memory), `FileStore`, `Journal`. `FileEvents` (and `tests/fakes/fake_file_events.py`) were pruned in Step 3 — the RX side has no directory to watch. |
| `src/session_manager/domain/ids.py` | `domain/ids.py` | `SenderId`→`ReceiverId`. `SessionId`/`BlockId`/`SymbolId` unchanged. |
| `tests/fakes/fake_ipc_server.py` | `tests/fakes/fake_ipc_server.py` | `SenderId`→`ReceiverId`; keeps per-peer send-failure injection. |
| `tests/unit/test_handshake.py` | `tests/unit/test_handshake.py` | `SenderId(1)`→`ReceiverId(1)` in the two `verify_proto_hash` cases. Real-contract digest test unchanged. |
| `tests/unit/test_codec.py` | `tests/unit/test_codec.py` | Round-trips `ReceiverHello`, `BlockDecoded`, and the cross-package `ipc_pb2.Heartbeat`; rejects an empty `RxEnvelope`. |
| `tests/integration/test_uds.py` | `tests/integration/test_uds.py` | Real `AF_UNIX`/`SOCK_SEQPACKET` server, `ReceiverHello` handshake, RX message types (`SessionOpen`/`PurgeSession` server→receiver, `BlockDecoded`/`Heartbeat` receiver→server), mismatched `proto_hash` refused, full send queue, write-failure teardown, **and a peer-identity-on-reconnect test**. Skipped (not faked) on a host without `AF_UNIX` — e.g. Windows dev boxes; it runs on the Linux grading machines. |

## Written fresh (patterned on file-monitor, not copied)

`src/session_manager/config.py` and `src/session_manager/constants.py` — the
RX schema is entirely different from file-monitor's TX one, but they follow
its shape: one `constants.py` holding every section/key/env-var/default, an
`ENV_OVERRIDES` data table (env var → section → key → caster) instead of one
branch per field, per-consumer frozen dataclasses (`PathsConfig`, `ShmConfig`,
…) rather than a god object, and `validate_config` raising `ValueError` that
names the offending key. `k`/`n`/`symbol_bytes` are deliberately not config —
they arrive per-session in the Manifest.

`tests/fakes/fake_shm.py` — new, no file-monitor equivalent (the TX side has
no shared memory). `FakeShm` satisfies both `ShmReader` and `ShmWriter` over a
`bytearray`; `bitmap_for` returns `memoryview(...).toreadonly()`. Test-driven
via `set_existing_segment` (absent / live / stale / incompatible),
`set_receiver_alive`, `corrupt_header`, `seed_payload`; records
`last_decision` (`CREATED` / `ADOPTED` / `REINITIALISED`) and exposes
`payload_is_zeroed()` so a test asserts the *decision* and that adopting a
live segment left a receiver's bytes intact. `fail_next_create_or_adopt` /
`fail_next_init_session` for failure injection. Built before the real POSIX
adapter on purpose — the adopt-vs-create cases can't be conjured against a
real `/dev/shm` segment on demand.

`src/session_manager/adapters/shm_layout.py` — the completion-segment header
as a cross-language contract (like `ipc/handshake.py`): `struct` format string
(explicit little-endian, no padding), `build_header` / `parse_header` /
`header_is_valid`, and the `AdoptDecision` enum both shm impls report. The
literal format/magic/version/offset live in `adapters/constants.py`.

`src/session_manager/adapters/posix_shm.py` — `PosixShm`, the real
implementation of both shm ports over `multiprocessing.shared_memory`.
`probe_receiver_alive` is a constructor callable (the composition root wires
it to the UDS server) so the adapter never imports a socket. `close()`
releases every `memoryview` handed out by `bitmap_for` first — you cannot
`SharedMemory.close()` while an exported view is alive. `detach_resource_
tracker()` unregisters each segment from `multiprocessing.resource_tracker`,
which would otherwise unlink the segment on a clean process exit — the exact
failure this service exists to prevent; POSIX-only, guarded, uses `_name`
because typeshed doesn't expose it.

`tests/integration/test_shm_contract.py` — the shared contract suite: five
adopt-vs-create cases parametrised over `FakeShm` and `PosixShm` via a
`Harness` protocol. This is what keeps the fake honest. Runs on Windows too
(SharedMemory works there; the harness keeps a handle open because Windows
frees an unreferenced segment immediately).

`src/session_manager/adapters/flock_file_lock.py` + `adapters/errors.py`
(`LockHeldError`) + `tests/fakes/fake_file_lock.py` — the `FileLock` port.
`FlockFileLock` is `fcntl.flock` (POSIX-only; its integration test is
`importorskip`-guarded, `mypy` sees it because `platform = "linux"`), writing
its pid into the file so a refused acquire can name the holder.

`src/session_manager/services/authority.py` + `services/errors.py`
(`ManifestRejected`) + fakes `fake_journal.py`, `fake_file_store.py` —
`SessionAuthority`, the sole writer of session state. `start()` takes the
`flock` before touching shm, then `create_or_adopt`; on adopt it replays the
journal per `ShmWriter.open_sessions()` to rebuild each decoded set.
`handle_manifest_seen` dedupes (first of three identical `ManifestSeen` wins,
no inter-receiver lock — that is the point), translates the `Manifest`
(wire `block_bytes` = symbol size), validates (`k >= n`, `total_blocks`
vs `ceil(file_size / k·symbol_bytes)`, `..` / absolute / backslash relpath),
`fallocate`s, `init_session`s, then broadcasts one `SessionOpen`.
`ShmWriter.open_sessions()` was added to the port for the adopt-recovery
path; `PosixShm` backs it with an on-segment session table in `shm_layout.py`.

`src/session_manager/services/aggregator.py` — `ProgressAggregator`. Folds
`BlockDecoded` into a per-session decoded set (idempotent; a block from two
receivers is one block); refreshes the stall timer only on real growth, so
duplicate spam can't keep a dead session looking alive. `poll()` builds a
`SessionSnapshot`, hands `COMPLETE` ones to the verifier callback exactly
once, logs `INCOMPLETE` with the missing-block preview. The shm cross-check
(popcount vs the UDS count, `log.warning` on divergence naming both) is gated
on `aggregation.shm_crosscheck` — that gate is what makes the UDS record
genuinely authoritative. Holds `ShmReader` only, never `ShmWriter`.

`src/session_manager/domain/paths.py` — `is_unsafe_relpath`, pulled out of
`authority.py` (no I/O, so it belongs in domain) so `publisher.py` can reuse
the exact same check rather than a second copy that could drift.

`src/session_manager/services/verifier.py` + `publisher.py` —
`IntegrityVerifier.verify(spec) -> bool` hashes the staged file
(`FileStore.staged_path`) via the `Hasher` port and compares hex digests,
`log.error("hash_mismatch", ...)` with both on a miss; it never touches the
filesystem itself. **Note:** `Hasher.compute_hash` is already `async` and
already thread-offloaded (`adapters/blake3_hasher.py`, from Step 0) — the
guide's "call it through asyncio.to_thread" is already satisfied one layer
down, so verifier.py just `await`s it rather than double-wrapping.
`Publisher.publish` / `.quarantine` re-check `is_unsafe_relpath` (defence in
depth — this is the point a corrupting-link value becomes a filesystem
write) then delegate to `FileStore`; two classes because verification is
CPU-only and publication is a filesystem mutation, and they fail and test
differently. `services/errors.py` gained `PublishRejected`. **No real
`FileStore` adapter exists yet** — these are tested against
`FakeFileStore`/`FakeHasher`, so "atomic rename" and "output directory empty
on mismatch" are properties the future adapter must uphold; today's tests
verify the orchestration (right method, right relpath, failure propagates
without touching `staged`). `fake_file_store.py` gained `staged` tracking +
`fail_next_publish`/`fail_next_quarantine`; `fake_hasher.py` gained
`fail_next_compute_hash` (it had no failure injection at all before this).

`src/session_manager/adapters/local_file_store.py` — `LocalFileStore`, the
real `FileStore`. `publish` uses `os.replace`, never `shutil.move` (`move`
falls back to non-atomic copy-then-delete across filesystems — exactly what
the config-time `st_dev` check on staging/output exists to rule out), and
refuses to overwrite an existing output file rather than silently clobbering
a verified file with an unverified one. `allocate` reserves the full size
with `os.posix_fallocate` where available, falling back to `truncate` on
Windows (dev platform) or when the filesystem rejects fallocate — a sparse
extent isn't the same guarantee, noted in the docstring. `quarantine` moves
(never deletes) into a `quarantine/` subdirectory of staging.
`tests/integration/test_file_store_contract.py` runs the same
staged/published/quarantined state-machine assertions against `FakeFileStore`
and `LocalFileStore` via a `Harness`, same pattern as the shm contract test;
`tests/unit/test_local_file_store.py` covers what only the real adapter can
prove — content survives the rename, the fallocate/truncate fallback, the
overwrite refusal.

`src/session_manager/adapters/append_journal.py` — `AppendJournal`, the real
`Journal`. Fixed-width binary records (`session_id` 16 bytes padded,
`block_id` u32, `offset` u64, `length` u32, then a CRC32 over those fields —
formats in `adapters/constants.py`), one file per session under
`journal_dir`. Relies on destination writes being idempotent (fixed offset,
fixed content) so a replayed duplicate is harmless — that property is why
this is ~100 lines instead of a write-ahead protocol; said explicitly in the
class docstring. A short read or a failed CRC stops replay **without
raising** — a torn tail is the expected shape of a crash, not an error.
Batches `fdatasync` (Linux) / `fsync` (fallback, e.g. Windows) behind an
explicit `sync()`, auto-triggered every `JOURNAL_SYNC_BATCH_SIZE` appends as
a backstop if a caller never calls it; `replay` also flushes (not syncs) this
instance's own open handle first, since visibility and durability are
different guarantees and only the latter needs the barrier. `session_id`
becomes a filename here, so it gets the same path-traversal check as
`relpath` (`domain/paths.is_unsafe_relpath`), independently, on its own field.
`tests/integration/test_journal_contract.py` runs the shared append/replay
properties against `FakeJournal` and `AppendJournal` — the pattern's third
outing. `tests/unit/test_append_journal.py` covers what only the real
adapter can prove: a truncated tail replays every complete record and stops
silently, a corrupted record stops replay there and logs, and `sync()`
survives a fresh instance reopening the file.

**Not yet wired:** nothing calls `Journal.append` when a block decodes —
`ProgressAggregator.handle_block_decoded` doesn't hold a `Journal` reference.
`SessionAuthority.start()` already calls `replay` on adopt, so the read side
works; the write side needs wiring in Step 12 (composition root), most
naturally as an extra call alongside `aggregator.handle_block_decoded` in
whatever dispatches `BlockDecoded`. **Ordering matters when it's wired:
`journal.append` first, then fold into the in-memory decoded set.** Folding
first and crashing before the append loses the block from the journal while
it was already reported complete — on restart the manager thinks it's
missing blocks the receivers actually finished writing, and stalls a session
that was fine.

`src/session_manager/services/status_display.py` — the graded status
display. Split in two per the brief: `render(snapshots) -> RenderableType`
is pure (no clock, no state, no I/O — this is why `SessionSnapshot` exists),
`StatusDisplay` is the thin `rich` `Live` driver that calls it every
`refresh_interval_s`. `crc_fail`/`kernel_drops`/`arena_exhausted` render
bold-red when non-zero — three unrelated failure modes (corrupting router,
slow host, decode falling behind) whose fixes have nothing in common, so the
display is what tells you which one you have. Session loss % is styled
green/yellow/red against `services/constants.py` thresholds so it reads at a
glance. **Receiver table headers are abbreviated** (`aexh`, `hwm%`, `kdrops`)
— the full field names overflowed and Rich truncated them to unreadable
fragments at an 80-column width, which is exactly the width `docker compose
logs` assumes when it can't detect a real terminal size; verified by
re-rendering at 72/80/100/120 columns, not just eyeballed at one width.
`pyproject.toml` gained `rich` and dropped `inotify-simple` (dead since
Step 3 pruned `FileEvents` — nothing ever imported it).

`src/session_manager/main.py` — the composition root. The only file that
imports both adapters and services. Startup order: proto-hash check →
construct adapters → `authority.start()` (flock, then `create_or_adopt`,
before anything else touches shm) → five tasks under one `TaskGroup`
(`ipc.serve`, `supervisor.run`, the dispatch loop, `aggregator.run`,
`status_display.run`) → log listening. Shutdown via an `asyncio.Event` set
from a signal handler plus a small watcher task that cancels the five
workers — never self-cancellation, copied from `file-monitor`'s `main.py`
(which really did have this bug once). `run()` takes an optional
`shutdown_event` param so a test can trigger shutdown directly instead of
sending a real OS signal.

Small additions made while wiring, not deferred to a later step:
- `services/receiver_registry.py` (`ReceiverRegistry`) — new; mirrors
  file-monitor's `SenderRegistry` exactly (three missed heartbeats, not
  one). `active_receivers` doubles as `ProgressAggregator`'s
  `live_receivers` callable; `any_alive` as `PosixShm`'s
  `probe_receiver_alive` — "does a receiver answer on the socket" is
  literally what this registry tracks.
- `domain/blocks.py` (`block_byte_range`) — `BlockDecoded` carries only a
  `block_id`, never a byte range, so the journal needs this pure helper to
  recompute `(offset, length)` from the spec before it can append.
- `ProgressAggregator.spec_for()` and `.snapshots()` — the dispatch loop
  needs the former to compute journal offsets for `block_decoded`; the
  status display needs the latter to enumerate all sessions. Both trivial,
  additive, `snapshots()` returns a tuple over the internal dict's values.
- `ProgressAggregator.handle_receiver_stats` now takes `receiver_id`
  explicitly instead of reading `stats.receiver_id` — a real inconsistency
  in the Step 7 code, caught while wiring: `handle_block_decoded` already
  trusted the connection's verified identity, not the payload; stats was
  the odd one out.

**The dispatch loop's `block_decoded` handler is where the Step 10 ordering
constraint actually lives**: for each in-range block id it calls
`journal.append` before `aggregator.handle_block_decoded`, and a failed
append raises before any fold happens (`test_a_journal_append_failure_stops_
that_block_reaching_the_aggregator` pins this).

**The adopted-sessions gap above is now fixed** (same day, before Step 13):
`src/session_manager/adapters/json_session_spec_store.py`
(`JsonSessionSpecStore`) persists the full `SessionSpec` as a JSON sidecar,
one file per session, next to its journal file (`journal_dir/<session_id>
.spec.json`) — a Python-only file, not a widened on-segment session table,
so B's receivers never need to parse it. New port
`ports/protocols.SessionSpecStore` (`save` / `load` / `delete`);
`SessionAuthority` gained a `spec_store` constructor param, calls
`spec_store.save(spec)` right after `init_session` succeeds in
`handle_manifest_seen` (durable before the `SessionOpen` broadcast), and
`_recover()` now calls `spec_store.load()` per adopted session, populating
`recovered_specs()` alongside the existing `recovered_blocks()`. `main.py`
wires `JsonSessionSpecStore(config.paths.journal_dir)` in and, after
`authority.start()`, registers every `authority.recovered_specs()` result
with the aggregator (`decoded=` from `recovered_blocks()`) — replacing the
log-only gap warning entirely. A session whose sidecar is itself lost
(corrupt JSON, missing field, deleted) logs
`session_spec_missing_on_recovery` / `session_spec_corrupt` loudly and stays
`_known` but unregistered, rather than either crashing or (worse)
re-`init_session`-ing and zeroing a bitmap a live receiver is still writing
into — decoded blocks and shm bytes are still safe, only verify/publish for
that one session is lost.
`domain/paths.py` gained `is_unsafe_filename_component`, factored out of
`AppendJournal._path_for` (which now uses it too) so the "session_id becomes
a filename" hazard has one check, not two independently-drifting copies.
Fourth contract-test outing:
`tests/integration/test_session_spec_store_contract.py` runs
save/load/delete against `FakeSessionSpecStore` and `JsonSessionSpecStore`.

**Windows dev-loop note:** `FlockFileLock` (`import fcntl`) is imported
lazily inside `run()`, not at module scope, so `import session_manager.main`
— and everything in it except an actual `run()` call — stays testable on a
non-POSIX box. `python -m session_manager.main` with no config present
confirms this: clean `invalid_config` log, exit 78, no traceback, on
Windows. `tests/integration/test_main_composition.py` is the real end-to-end
check (lock/segment/socket lifecycle) and is POSIX-only
(`pytest.importorskip("fcntl")`), same as the UDS and flock integration
tests.

## Config / build (renamed, structure kept)

`pyproject.toml`, `Dockerfile`, `.dockerignore`, `scripts/entrypoint.sh`,
`.gitignore`, `.env.example` — package renamed `file-monitor`/`file_monitor`
→ `session-manager`/`session_manager`; container paths renamed to the RX
schema (`NEXUS_WATCH_PATH` → `NEXUS_STAGING_DIR`, plus `NEXUS_OUTPUT_DIR` /
`NEXUS_JOURNAL_DIR` / `NEXUS_RUN_DIR` / `NEXUS_LOCK_PATH`); socket basename
changed. **The hatch `force-include` of
`libs/nexus-proto/generated/python` at the wheel root is kept** — the flat
proto modules (`rx_pb2.py` does `import common_pb2`) must land on `sys.path`,
never nested in a package:

```toml
[tool.hatch.build.targets.wheel.force-include]
"libs/nexus-proto/generated/python" = "."
```

Without that block the container build succeeds and the process dies on
`import rx_pb2` at runtime — and the `conftest.py` `sys.path` insert would
hide it from the test suite, so this is verified by eye, not by a test.

## The three that took several rounds each (in `uds.py` / `supervisor.py`)

1. **Peer identity on reconnect.** A peer that reconnects gets a *new*
   `send_queue`; `_peers[receiver_id]` is overwritten on the new handshake.
   Both the disconnect cleanup in `_handle_peer`'s `finally` and the
   write-failure cleanup in `_write_loop` guard with
   `if self._peers.get(receiver_id) is <this queue>` before popping — so a
   stale connection tearing down does **not** evict the live reconnected
   peer. Covered by
   `test_a_reconnecting_peer_keeps_its_live_queue_when_the_stale_one_tears_down`.

2. **Write-failure teardown via `SHUT_RD`.** When `sock_sendall` raises
   `OSError`, `_write_loop` removes the peer from `_peers` and calls
   `connection.shutdown(socket.SHUT_RD)` (tolerating a further `OSError`).
   Shutting the read half unblocks the `sock_recv` in `_handle_peer` with an
   empty read, so the reader task exits cleanly and the `finally` closes the
   socket — instead of the reader hanging on a half-dead connection. Covered
   by `test_a_write_failure_tears_down_the_whole_peer`.

3. **Backoff interrupted by the stop event** (`supervisor.py`).
   `_wait_for_backoff_or_stop` races `clock.sleep(delay)` against
   `stop_event.wait()` with `FIRST_COMPLETED` and cancels the loser. Without
   this, `shutdown()` during a 30-second backoff would block for the full
   30 seconds. Covered by
   `test_shutdown_during_backoff_window_exits_without_waiting_out_the_delay`.

## Done when

- `python -c "import rx_pb2, common_pb2"` succeeds
  (`PYTHONPATH=libs/nexus-proto/generated/python`).
- `pytest` runs green (`test_uds.py` skips off-POSIX).
