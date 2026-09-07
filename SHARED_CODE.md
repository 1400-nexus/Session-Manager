# SHARED_CODE.md — the copy ledger

Code in this repo that was **copied from `file-monitor`** rather than written
here. When `file-monitor` fixes a bug in one of these, it has to be carried
across by hand — there is no shared package. Keep this table honest.

## Cross-repo state, as of `nexus-proto@7f406db`

| repo | `nexus-proto` pin | state |
|---|---|---|
| `session-manager` | `7f406db` | this repo. |
| `file-monitor` | `7f406db` | realigned. |
| `sender` (Person A) | `60bd06e` | **five commits behind.** Its `SenderHello.proto_hash` will not match `file-monitor`'s — **the connection is refused**, not subtly wrong. Also missing `AssignSession.source_path` and the `Manifest.sender_id` shard-residue comment. Must bump before any integration. |
| `receiver` (Person B) | — | one commit, a README. No code yet. |

`proto_hash` is BLAKE3 over the raw bytes of every `*.proto` file, so it moves
with any edit — a comment included — and it has moved several times. The
digest at `7f406db` is
`38cac339d495241ae757fbeec84a6ecdc5377f838798ff9df1e19650bcff20df`, recorded
in `docs/INTEGRATION.md` step 1, `docs/RECEIVER_CONTRACT.md` §2, and
`file-monitor/docs/SENDER_CONTRACT.md` §2. Check the pin with
`git submodule status`.

The RX contract has not changed structurally since `cef65a6` (the `rx.proto`
edits since — `dest_path` absolute, `SessionOpen` idempotent, `BlockDecoded`
durable-before-report — are all comments). `ipc.proto` gained
`AssignSession.source_path` at `7f406db` for the sender; nothing in it changed
for the RX side.

**Line-ending trap:** `proto_hash` is over raw bytes, so a CRLF checkout of
the `.proto` files hashes differently at the same pin — `git submodule status`
shows the right commit and the handshake refuses anyway.
`nexus-proto/.gitattributes` pins `*.proto` to `eol=lf`, so a **fresh** clone
is fine, but an *existing* checkout from before that attribute was added keeps
its CRLF (git does not renormalise on `git checkout <sha>` when the blob is
unchanged). Fix a stale one with `git -C libs/nexus-proto checkout --force
HEAD` or re-clone. `*.proto text eol=lf` is also repeated in this repo's
`.gitattributes` as documentation (a superproject's attributes do not govern
a submodule).

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
| `tests/integration/stub_receiver.py` | `tests/integration/stub_sender.py` | Same structure (argparse, `connect`/heartbeat/receive loops, independent re-derivation of wire arithmetic, `main()`), but the RX side has more to prove: it writes real bytes into the staged file at independently-recomputed offsets (`_block_byte_range`, not imported from `domain.blocks`) and derives a whole `Manifest` — including `file_hash` — from `(--session-id, --blocks)` alone, so three stub processes agree on one file with no coordination. `--exit-after-assignments`/`--expect-refused` (file-monitor's one-shot sender) become an infinite reconnect loop (a receiver has no natural "done"): `_Progress.dest_path` (the absolute path from `SessionOpen`) is cached across reconnects so a resend after a crash doesn't wait forever for a `SessionOpen` the recovered session will never re-broadcast. `--withhold`/`--corrupt`, per-block-paced reporting (`BLOCK_REPORT_PACING_SECONDS`), and `flush + fdatasync before every BlockDecoded` (`_make_durable` — the milestone-4 durability contract) are all new, for milestones 2/3/4. Must not import `session_manager.domain`/`services` — only `ipc.codec`/`ipc.handshake`/`ipc.constants` — same rule as the file it's copied from. |
| `scripts/run_milestones.sh` | `scripts/run_milestones.sh` | Same skeleton (temp `WORK_DIR`, `trap cleanup EXIT`, `start_*`/`stop_*`, `LAST_STUB_PID` global, `interruptible_sleep`, `record`, a RESULTS table). Five milestones instead of three, driven by `session_manager.main` instead of `file_monitor.main`. Milestone 3 (this repo's numbering) is new: `verify_output_hash` imports `stub_receiver._file_hash` to independently check the published file's actual bytes, not just a log line. Milestone 4 (`kill -9` the manager, restart, assert `adopted=True` + `session_recovered` + a correct republished file) has no file-monitor equivalent — file-monitor has no persistent-adopt-vs-create path to exercise. |

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
because typeshed doesn't expose it. `close(unlink=True)` calls
`reattach_resource_tracker()` first: `SharedMemory.unlink()` unconditionally
notifies the tracker, and without a matching re-register that is a message
for a name detach already removed — the tracker child printed a `KeyError`
traceback on **every clean shutdown** (in every manager log) until this.

`tests/integration/test_shm_contract.py` — the shared contract suite: six
adopt-vs-create / lifecycle cases parametrised over `FakeShm` and `PosixShm`
via a `Harness` protocol (the sixth: create → `close(unlink=True)` → the name
is free and re-creatable, the traceback regression). Runs on Windows too
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

**Wired in Step 12:** `main.py`'s `block_decoded` handler calls
`journal.append` for each in-range block id **before**
`aggregator.handle_block_decoded` — a failed append raises before any fold
(`test_a_journal_append_failure_stops_that_block_reaching_the_aggregator`
pins this). Folding first and crashing before the append would lose the
block from the journal while it was reported complete; on restart the
manager would think it's missing blocks the receivers finished writing and
stall a session that was fine.

`src/session_manager/services/status_display.py` — the graded status
display. `render(snapshots) -> RenderableType` is pure (no clock, no state,
no I/O — this is why `SessionSnapshot` exists). `StatusDisplay.run()`
branches on `Console.is_terminal`: with a TTY it drives `rich.Live` at
`refresh_interval_s`; **with no TTY** it emits one structured `status` log
event every `STATUS_LOG_INTERVAL_SECONDS` (5 s) carrying the same fields
(`log_status`), because `Live` on a pipe repaints the whole tables into
`docker compose logs` on every refresh and buries every other line. `main.py`
passes `Console(force_terminal=True if config.status.force_terminal else
None)` — `None` is auto-detect; the config flag is force-*on* only, since
`force_terminal=False` in rich means force-*off* even on a real PTY (that
wiring bug produced zero status output until it was caught by looking at the
container). `crc_fail`/`kernel_drops`/`arena_exhausted` render bold-red when
non-zero — three unrelated failure modes. **Receiver headers are abbreviated**
(`aexh`, `hwm%`, `kdrops`): the full names overflowed and Rich truncated them
to unreadable fragments at 80 columns, verified re-rendering at 72/80/100/120.
`pyproject.toml` gained `rich` and dropped `inotify-simple`.

`src/session_manager/main.py` — the composition root, the only file importing
both adapters and services. Everything is constructed first (adapters,
`aggregator`, then `authority` with `on_session_opened=aggregator.register_
session`), then inside one `TaskGroup`:

1. `ipc.serve()` and the dispatch loop start **immediately** — a receiver
   reconnecting after a crash needs somewhere to say `ReceiverHello` before
   the adopt decision is made. `receiver_hello` / `heartbeat` are handled
   ungated; `manifest_seen` / `block_decoded` `await` a `ready` event.
2. wait for the socket file, then up to `ADOPT_GRACE_PERIOD_SECONDS` (1.5 s)
   for `registry.any_alive()`.
3. `authority.start()` — flock, `create_or_adopt`, `_recover()` on adopt.
   `LockHeldError` routes to the common cleanup tail, not an early return.
4. set `ready`; create the other three workers (`supervisor.run`,
   `aggregator.run`, `status_display.run`) and the shutdown watcher; log
   `session_manager_listening adopted=<bool>`.

Without step 1–2, `probe_receiver_alive()` is checked before the socket is
even bound, so a real crash+restart always reinitialises instead of adopting.
Shutdown via an `asyncio.Event` + a watcher task that cancels the workers —
never self-cancellation (copied from file-monitor, which had that bug).
`run(config, shutdown_event=None)` for tests.

Wiring-time additions:
- `services/receiver_registry.py` (`ReceiverRegistry`) — mirrors
  file-monitor's `SenderRegistry` (three missed heartbeats:
  `HEARTBEAT_INTERVAL_SECONDS` 5.0 × `MISSED_HEARTBEAT_LIMIT` 3 = 15 s).
  `active_receivers` is the aggregator's `live_receivers`; `any_alive` is
  `PosixShm`'s `probe_receiver_alive`.
- `domain/blocks.py` (`block_byte_range`) — recomputes `(offset, length)`
  from the spec since `BlockDecoded` carries only a `block_id`.
- `ProgressAggregator.spec_for()` / `.snapshots()`; `handle_receiver_stats`
  takes `receiver_id` explicitly (Step 7 inconsistency — `handle_block_
  decoded` already trusted the connection, stats was the odd one out).
- the `block_decoded` handler calls `journal.append` per in-range id
  **before** `aggregator.handle_block_decoded` (see the journal section).

`src/session_manager/adapters/json_session_spec_store.py`
(`JsonSessionSpecStore`, port `SessionSpecStore` — `save`/`load`/`delete`)
persists the full `SessionSpec` as a JSON sidecar, one per session next to
its journal file (`journal_dir/<session_id>.spec.json`) — Python-only, not a
widened on-segment table, so B's receivers never parse it. `SessionAuthority`
`save`s it right after `init_session`, durable before the `SessionOpen`
broadcast. On adopt, `_recover()` `load`s it per open session and calls
`on_session_opened(spec, decoded_from_journal)` — the **same callback** a
fresh `handle_manifest_seen` calls, so a recovered session is registered
with the aggregator on exactly the path a new one is (there is no separate
seeding loop in `main.py` any more, and no `recovered_specs()` /
`recovered_blocks()` accessors — `authority.was_recovered(session_id)` is all
that's left). A session whose sidecar is lost logs
`session_spec_missing_on_recovery` / `session_spec_corrupt` and stays
`_known` but unregistered — decoded blocks and shm bytes are still safe,
only verify/publish for that one session is lost.
`domain/paths.py` gained `is_unsafe_filename_component` (shared by
`AppendJournal._path_for` and the spec store). Fourth contract-test outing:
`tests/integration/test_session_spec_store_contract.py`.

`SessionAuthority` also sends **`Config`** (`shm_name` / `staging_dir` /
`journal_dir`) to each receiver right after its `ReceiverHello`
(`send_config_to`), replays a `SessionOpen` for every open session on that
hello once `ready` is set (`send_open_sessions_to`), and answers a
**duplicate** `ManifestSeen` with a targeted `SessionOpen` rather than a
re-broadcast. `main.py` broadcasts **`PurgeSession`** when a session goes
terminal — `verified` / `hash_mismatch` from the `on_complete` closure,
`incomplete` from a new `ProgressAggregator` `on_stalled` callback (fires
once). Both messages existed in `rx.proto` unsent until this.

**Milestone-4 durability:** a block reported before its bytes are durable is
a hole in the recovered file (the manager journals the report and, on adopt,
never re-asks). `rx.proto`'s `BlockDecoded` and `RECEIVER_CONTRACT.md` §5
make write+fsync-before-report a hard receiver requirement;
`tests/integration/stub_receiver.py` `fdatasync`s each block. `main.py` logs
`recovered_session_failed_verification` when a hash mismatch follows an
adopt, to separate a recovery hole from FEC corruption
(`SessionAuthority.was_recovered`).

`ProgressAggregator` — `mark_verified` / `mark_hash_mismatch` move a session
into the `VERIFIED` / `HASH_MISMATCH` terminal states (defined since Step 1,
never set until Step 13); `poll()` skips terminal sessions so the outcome
isn't recomputed back to `COMPLETE`. The shm cross-check warns **once per
session** (`_crosscheck_warned`), not every poll — a permanent divergence
was 300 identical lines otherwise; `aggregation.shm_crosscheck` defaults to
`false` (config.toml) until a receiver actually writes the bitmap.

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

`pyproject.toml`, `Dockerfile`, `.dockerignore`, `compose.yml`,
`scripts/entrypoint.sh`, `.gitignore`, `.gitattributes`, `.env.example` —
package renamed `file-monitor`/`file_monitor` → `session-manager`/
`session_manager`; container paths renamed to the RX schema
(`NEXUS_WATCH_PATH` → `NEXUS_STAGING_DIR`, plus `NEXUS_OUTPUT_DIR` /
`NEXUS_JOURNAL_DIR` / `NEXUS_RUN_DIR` / `NEXUS_LOCK_PATH`); socket basename
changed. `compose.yml` has three RX-specific settings — `shm_size: 512m`
(the arena is 256 MB, the container default `/dev/shm` is 64 MB), one named
volume for all of `/var/nexus` (staging and output must share a filesystem —
`os.replace`), `NEXUS_RECEIVERS_COUNT=0` — and deliberately **no** `tty: true`
(a PTY makes `rich.Live` spam the logs; the no-TTY `status` line is the
container path). `.gitattributes` pins `*.sh`/`*.py`/`*.toml`/`*.proto` to
`eol=lf` — a Windows clone (`core.autocrlf=true`) otherwise checks out
`run_milestones.sh` with CRLF and it fails under Linux bash. **The hatch
`force-include` of
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

## The multi-round ones (in `uds.py` / `supervisor.py` / `posix_shm.py` / `main.py`)

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

4. **Bind before adopt** (`main.py`). `create_or_adopt`'s adopt path checks
   `probe_receiver_alive()`, which reads the registry, which is populated by
   handshakes, which need a bound socket. The original order ran
   `authority.start()` before `ipc.serve()`, so a manager restart always saw
   an empty registry and reinitialised the segment a live receiver was
   still using. Fixed by starting the socket + dispatch loop first, gating
   only `manifest_seen`/`block_decoded` on a `ready` event, and waiting out
   `ADOPT_GRACE_PERIOD_SECONDS` for a reconnect. Milestone 4 exercises it;
   `test_main_dispatch.py` covers the gating.

5. **resource_tracker double-unregister** (`posix_shm.py`) — see the
   `reattach_resource_tracker` note above.

## Done when

- `python -c "import rx_pb2, common_pb2"` succeeds
  (`PYTHONPATH=libs/nexus-proto/generated/python`).
- `ruff check`, `ruff format --check`, `mypy --strict` clean; `pytest` green
  (`fcntl`/`AF_UNIX` tests skip off-POSIX); `scripts/run_milestones.sh` 5/5
  on Linux.
