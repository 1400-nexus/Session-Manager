import asyncio
import dataclasses
import signal
from typing import Any

import common_pb2
import ipc_pb2
import pytest
import rx_pb2
from structlog.testing import capture_logs

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import SessionSnapshot, SessionSpec, SessionState
from session_manager.ipc import codec
from session_manager.main import (
    DispatchContext,
    _build_receiver_specs,
    _cancel_workers_on_shutdown,
    _handle_block_decoded,
    _handle_heartbeat,
    _handle_manifest_seen,
    _handle_receiver_hello,
    _handle_receiver_stats,
    _install_shutdown_signal_handlers,
    _make_on_complete,
    _run_incoming_dispatch_loop,
)
from session_manager.services.aggregator import ProgressAggregator
from session_manager.services.authority import SessionAuthority
from session_manager.services.publisher import Publisher
from session_manager.services.receiver_registry import ReceiverRegistry
from session_manager.services.verifier import IntegrityVerifier
from tests.fakes.fake_clock import FakeClock
from tests.fakes.fake_file_lock import FakeFileLock
from tests.fakes.fake_file_store import FakeFileStore
from tests.fakes.fake_hasher import FakeHasher
from tests.fakes.fake_ipc_server import FakeIpcServer
from tests.fakes.fake_journal import FakeJournal
from tests.fakes.fake_shm import FakeShm

ARENA_BYTES = 1 << 20
REGION_BASE = 8192
SESSION = SessionId("s-1")
MATCHING_DIGEST_HEX = "ab" * 32


def _spec(*, total_blocks: int = 4) -> SessionSpec:
    return SessionSpec(
        session_id=SESSION,
        relpath="sub/file.bin",
        file_size=total_blocks * 280_000,
        file_hash=b"\x00" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=total_blocks,
    )


def _context(
    *, registry: ReceiverRegistry | None = None, journal: FakeJournal | None = None
) -> tuple[DispatchContext, FakeClock, FakeShm, ProgressAggregator, FakeJournal]:
    clock = FakeClock()
    registry = registry or ReceiverRegistry(clock, heartbeat_interval_s=5.0)
    shm = FakeShm()
    journal = journal or FakeJournal()
    completed: list[Any] = []

    async def on_complete(snapshot: Any) -> None:
        completed.append(snapshot)

    aggregator = ProgressAggregator(
        clock=clock,
        shm_reader=shm,
        on_complete=on_complete,
        live_receivers=registry.active_receivers,
        poll_interval_s=1.0,
        stall_timeout_s=8.0,
        shm_crosscheck=False,
    )
    authority = SessionAuthority(
        file_lock=FakeFileLock(),
        shm=shm,
        file_store=FakeFileStore(),
        journal=journal,
        broadcast=lambda payload: asyncio.sleep(0),
        shm_name="seg",
        arena_bytes=ARENA_BYTES,
        session_region_base=REGION_BASE,
    )
    context = DispatchContext(
        registry=registry, authority=authority, aggregator=aggregator, journal=journal
    )
    return context, clock, shm, aggregator, journal


async def test_handle_receiver_hello_registers_in_the_registry() -> None:
    context, _clock, _shm, _aggregator, _journal = _context()

    await _handle_receiver_hello(context, ReceiverId(1), rx_pb2.ReceiverHello(receiver_id=1))

    assert context.registry.active_receivers() == frozenset({ReceiverId(1)})


async def test_handle_heartbeat_refreshes_the_registry() -> None:
    context, clock, _shm, _aggregator, _journal = _context()
    context.registry.register(ReceiverId(1))

    clock.advance(14.999)  # just under the 3 x 5.0s timeout
    await _handle_heartbeat(context, ReceiverId(1), ipc_pb2.Heartbeat())

    assert context.registry.active_receivers() == frozenset({ReceiverId(1)})


async def test_handle_manifest_seen_delegates_to_the_authority() -> None:
    context, _clock, shm, _aggregator, _journal = _context()
    await context.authority.start()
    manifest_seen = rx_pb2.ManifestSeen(
        receiver_id=1,
        manifest=common_pb2.Manifest(
            session_id=str(SESSION),
            filepath="sub/file.bin",
            file_size=1_120_000,
            file_hash=b"\x00" * 32,
            k=200,
            n=255,
            block_bytes=1400,
            total_blocks=4,
        ),
    )

    await _handle_manifest_seen(context, ReceiverId(1), manifest_seen)

    assert [session.session_id for session in shm.open_sessions()] == [SESSION]


async def test_handle_receiver_stats_uses_the_connection_receiver_id_not_the_payload() -> None:
    context, _clock, _shm, aggregator, _journal = _context()
    aggregator.register_session(_spec())
    stats = rx_pb2.ReceiverStats(receiver_id=99, pkts_ok=42)  # payload claims receiver 99

    await _handle_receiver_stats(context, ReceiverId(7), stats)
    await aggregator.poll()

    snapshot = aggregator.snapshot_for(SESSION)
    assert snapshot is not None
    assert snapshot.counters_for(ReceiverId(7)) is not None
    assert snapshot.counters_for(ReceiverId(7)).pkts_ok == 42  # type: ignore[union-attr]
    assert snapshot.counters_for(ReceiverId(99)) is None


async def test_block_decoded_journals_before_folding_into_the_aggregator() -> None:
    context, _clock, _shm, aggregator, journal = _context()
    aggregator.register_session(_spec(total_blocks=4))
    message = rx_pb2.BlockDecoded(session_id=str(SESSION), receiver_id=1, block_ids=[0, 2])

    await _handle_block_decoded(context, ReceiverId(1), message)

    assert [block_id async for block_id in journal.replay(SESSION)] == [BlockId(0), BlockId(2)]
    await aggregator.poll()
    snapshot = aggregator.snapshot_for(SESSION)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 2


async def test_a_journal_append_failure_stops_that_block_reaching_the_aggregator() -> None:
    journal = FakeJournal()
    context, _clock, _shm, aggregator, journal = _context(journal=journal)
    aggregator.register_session(_spec(total_blocks=4))
    journal.fail_next_append(OSError("disk full"))
    message = rx_pb2.BlockDecoded(session_id=str(SESSION), receiver_id=1, block_ids=[0])

    with pytest.raises(OSError, match="disk full"):
        await _handle_block_decoded(context, ReceiverId(1), message)

    await aggregator.poll()
    snapshot = aggregator.snapshot_for(SESSION)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 0


async def test_block_decoded_never_journals_out_of_range_block_ids() -> None:
    context, _clock, _shm, aggregator, journal = _context()
    aggregator.register_session(_spec(total_blocks=4))
    message = rx_pb2.BlockDecoded(session_id=str(SESSION), receiver_id=1, block_ids=[5, 99])

    await _handle_block_decoded(context, ReceiverId(1), message)

    assert [block_id async for block_id in journal.replay(SESSION)] == []


async def test_block_decoded_for_an_unknown_session_does_not_touch_the_journal() -> None:
    context, _clock, _shm, _aggregator, journal = _context()
    message = rx_pb2.BlockDecoded(session_id="ghost", receiver_id=1, block_ids=[0])

    await _handle_block_decoded(context, ReceiverId(1), message)

    assert [block_id async for block_id in journal.replay(SessionId("ghost"))] == []


async def test_dispatch_loop_routes_receiver_hello_to_the_registry() -> None:
    context, _clock, _shm, _aggregator, _journal = _context()
    ipc = FakeIpcServer()
    ipc._incoming.put_nowait((ReceiverId(3), codec.encode(rx_pb2.ReceiverHello(receiver_id=3))))

    loop_task = asyncio.create_task(_run_incoming_dispatch_loop(ipc, context))
    try:
        await _wait_until(lambda: context.registry.active_receivers() == frozenset({ReceiverId(3)}))
    finally:
        loop_task.cancel()
        await asyncio.gather(loop_task, return_exceptions=True)


async def test_dispatch_loop_warns_and_continues_on_an_unhandled_message_type() -> None:
    context, _clock, _shm, _aggregator, _journal = _context()
    ipc = FakeIpcServer()
    ipc._incoming.put_nowait(
        (ReceiverId(1), codec.encode(rx_pb2.PurgeSession(session_id="s-1", reason="done")))
    )
    ipc._incoming.put_nowait((ReceiverId(1), codec.encode(rx_pb2.ReceiverHello(receiver_id=1))))

    loop_task = asyncio.create_task(_run_incoming_dispatch_loop(ipc, context))
    try:
        with capture_logs() as logs:
            await _wait_until(
                lambda: context.registry.active_receivers() == frozenset({ReceiverId(1)})
            )
        unknown = [entry for entry in logs if entry["event"] == "unknown_incoming_message_type"]
        assert len(unknown) == 1
        assert unknown[0]["field_name"] == "purge_session"
    finally:
        loop_task.cancel()
        await asyncio.gather(loop_task, return_exceptions=True)


async def test_on_complete_publishes_a_matching_session() -> None:
    hasher = FakeHasher(digest=MATCHING_DIGEST_HEX)
    store = FakeFileStore()
    store.staged.add("sub/file.bin")
    on_complete = _make_on_complete(IntegrityVerifier(hasher, store), Publisher(store))
    spec = _spec_with_hash(bytes.fromhex(MATCHING_DIGEST_HEX))

    await on_complete(_snapshot_for(spec))

    assert store.published == ["sub/file.bin"]
    assert store.quarantined == []


async def test_on_complete_quarantines_a_mismatch_and_never_raises() -> None:
    hasher = FakeHasher(digest=MATCHING_DIGEST_HEX)
    store = FakeFileStore()
    store.staged.add("sub/file.bin")
    on_complete = _make_on_complete(IntegrityVerifier(hasher, store), Publisher(store))
    spec = _spec_with_hash(bytes.fromhex("ff" * 32))  # does not match hasher's digest

    await on_complete(_snapshot_for(spec))

    assert store.quarantined == ["sub/file.bin"]
    assert store.published == []


async def test_on_complete_guards_against_a_hashing_failure() -> None:
    hasher = FakeHasher()
    hasher.fail_next_compute_hash(OSError("read error"))
    store = FakeFileStore()
    on_complete = _make_on_complete(IntegrityVerifier(hasher, store), Publisher(store))

    with capture_logs() as logs:
        await on_complete(_snapshot_for(_spec()))  # must not raise

    assert any(entry["event"] == "session_completion_failed" for entry in logs)


def _spec_with_hash(file_hash: bytes) -> SessionSpec:
    return dataclasses.replace(_spec(), file_hash=file_hash)


def _snapshot_for(spec: SessionSpec) -> SessionSnapshot:
    return SessionSnapshot(
        spec=spec,
        state=SessionState.COMPLETE,
        blocks_decoded=spec.total_blocks,
        total_blocks=spec.total_blocks,
        observed_loss_pct=0.0,
        per_receiver=(),
        live_receivers=frozenset(),
        missing_blocks=(),
        missing_block_count=0,
    )


async def test_cancel_workers_on_shutdown_cancels_every_task() -> None:
    shutdown_event = asyncio.Event()

    async def _forever() -> None:
        await asyncio.get_running_loop().create_future()

    workers = [asyncio.create_task(_forever()), asyncio.create_task(_forever())]
    watcher = asyncio.create_task(_cancel_workers_on_shutdown(shutdown_event, workers))

    shutdown_event.set()
    await _wait_until(lambda: all(task.cancelled() for task in workers))
    await asyncio.gather(watcher, return_exceptions=True)


def test_install_shutdown_signal_handlers_falls_back_when_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnsupportedLoop:
        def add_signal_handler(self, sig: int, callback: Any) -> None:
            raise NotImplementedError

    recorded: list[int] = []
    monkeypatch.setattr(signal, "signal", lambda sig, handler: recorded.append(sig))

    _install_shutdown_signal_handlers(_UnsupportedLoop(), lambda: None)  # type: ignore[arg-type]

    assert recorded == [signal.SIGTERM, signal.SIGINT]


def test_build_receiver_specs_one_per_configured_receiver() -> None:
    from pathlib import Path

    from session_manager.config import (
        AggregationConfig,
        AppConfig,
        PathsConfig,
        ReceiversConfig,
        ShmConfig,
        StatusConfig,
        SupervisionConfig,
    )

    config = AppConfig(
        paths=PathsConfig(
            staging_dir=Path("staging"),
            output_dir=Path("output"),
            journal_dir=Path("journal"),
            run_dir=Path("run"),
            socket_path=Path("run/x.sock"),
            lock_path=Path("run/x.lock"),
        ),
        shm=ShmConfig(name="n", arena_bytes=4096, slot_bytes=1024),
        aggregation=AggregationConfig(
            poll_interval_s=1.0, stall_timeout_s=8.0, shm_crosscheck=True
        ),
        receivers=ReceiversConfig(count=2, ports=(9100, 9101, 9102), binary_path="./bin/rx"),
        supervision=SupervisionConfig(),
        status=StatusConfig(refresh_interval_s=0.5, force_terminal=False),
    )

    specs = _build_receiver_specs(config)

    assert [spec.name for spec in specs] == ["receiver-0", "receiver-1"]
    assert specs[0].argv == ["./bin/rx", "--receiver-id", "0", "--listen-port", "9100"]
    assert specs[1].argv == ["./bin/rx", "--receiver-id", "1", "--listen-port", "9101"]


async def _wait_until(predicate: Any, timeout: float = 1.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout=timeout)
