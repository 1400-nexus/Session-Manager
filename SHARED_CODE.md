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
