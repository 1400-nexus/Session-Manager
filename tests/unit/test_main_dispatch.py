import asyncio
import signal
from pathlib import Path
from typing import Any

import common_pb2
import ipc_pb2
import pytest
import rx_pb2
from structlog.testing import capture_logs

from session_manager.adapters.system_clock import SystemClock
from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import SessionSpec
from session_manager.domain.purge_policy import PurgeReason
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
    _run_incoming_dispatch_loop,
    _wait_for_a_receiver_or_timeout,
    _wait_for_path,
)
from session_manager.services.aggregator import ProgressAggregator
from session_manager.services.authority import SessionAuthority
from session_manager.services.receiver_registry import ReceiverRegistry
from tests.fakes.fake_clock import FakeClock
from tests.fakes.fake_file_lock import FakeFileLock
from tests.fakes.fake_file_store import FakeFileStore
from tests.fakes.fake_ipc_server import FakeIpcServer
from tests.fakes.fake_journal import FakeJournal
from tests.fakes.fake_session_spec_store import FakeSessionSpecStore
from tests.fakes.fake_shm import FakeShm

ARENA_BYTES = 1 << 20
REGION_BASE = 8192
SESSION = SessionId("s-1")


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
    *,
    registry: ReceiverRegistry | None = None,
    journal: FakeJournal | None = None,
    ready: asyncio.Event | None = None,
    sends: list[tuple[ReceiverId, bytes]] | None = None,
) -> tuple[DispatchContext, FakeClock, FakeShm, ProgressAggregator, FakeJournal]:
    clock = FakeClock()
    registry = registry or ReceiverRegistry(clock, heartbeat_interval_s=5.0)
    shm = FakeShm()
    journal = journal or FakeJournal()
    completed: list[Any] = []
    sends = sends if sends is not None else []

    async def on_complete(snapshot: Any) -> None:
        completed.append(snapshot)

    aggregator = ProgressAggregator(
        clock=clock,
        shm_reader=shm,
        on_complete=on_complete,
        live_receivers=registry.active_receivers,
        poll_interval_s=1.0,
        shm_crosscheck=False,
    )

    async def send_to_receiver(receiver_id: ReceiverId, payload: bytes) -> None:
        sends.append((receiver_id, payload))

    authority = SessionAuthority(
        file_lock=FakeFileLock(),
        shm=shm,
        file_store=FakeFileStore(),
        journal=journal,
        spec_store=FakeSessionSpecStore(),
        broadcast=lambda payload: asyncio.sleep(0),
        send_to_receiver=send_to_receiver,
        on_session_opened=aggregator.register_session,
        clock=clock,
        progress_of=aggregator.progress_of,
        on_incomplete=aggregator.mark_incomplete,
        shm_name="seg",
        staging_dir="/staging",
        journal_dir="/journal",
        arena_bytes=ARENA_BYTES,
        session_region_base=REGION_BASE,
        sweep_interval_s=0.0,  # tests here don't exercise the sweep
        stall_timeout_s=60.0,
    )
    if ready is None:
        # Pre-set by default: most of these tests call handlers directly and
        # don't care about run()'s startup gate. Tests of the gate itself
        # pass an unset Event explicitly.
        ready = asyncio.Event()
        ready.set()
    context = DispatchContext(
        registry=registry, authority=authority, aggregator=aggregator, journal=journal, ready=ready
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


def _manifest_seen(*, receiver_id: int = 1, total_blocks: int = 4) -> Any:
    return rx_pb2.ManifestSeen(
        receiver_id=receiver_id,
        manifest=common_pb2.Manifest(
            session_id=str(SESSION),
            filepath="sub/file.bin",
            file_size=total_blocks * 280_000,
            file_hash=b"\x00" * 32,
            k=200,
            n=255,
            block_bytes=1400,
            total_blocks=total_blocks,
        ),
    )


async def test_handle_manifest_seen_delegates_to_the_authority() -> None:
    context, _clock, shm, _aggregator, _journal = _context()
    await context.authority.start()

    await _handle_manifest_seen(context, ReceiverId(1), _manifest_seen())

    assert [session.session_id for session in shm.open_sessions()] == [SESSION]


async def test_a_fresh_session_is_registered_so_its_blocks_are_not_unknown() -> None:
    context, _clock, _shm, aggregator, _journal = _context()
    await context.authority.start()

    await _handle_manifest_seen(context, ReceiverId(1), _manifest_seen(total_blocks=4))
    decoded = rx_pb2.BlockDecoded(session_id=str(SESSION), receiver_id=1, block_ids=[0, 1])
    with capture_logs() as logs:
        await _handle_block_decoded(context, ReceiverId(1), decoded)

    assert not any(entry["event"] == "block_decoded_for_unknown_session" for entry in logs)
    await aggregator.poll()
    snapshot = aggregator.snapshot_for(SESSION)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 2


async def test_a_duplicate_manifest_seen_is_answered_with_session_open_to_that_receiver() -> None:
    sends: list[tuple[ReceiverId, bytes]] = []
    context, _clock, _shm, _aggregator, _journal = _context(sends=sends)
    await context.authority.start()

    await _handle_manifest_seen(context, ReceiverId(1), _manifest_seen())  # creates
    await _handle_manifest_seen(context, ReceiverId(2), _manifest_seen())  # duplicate

    assert len(sends) == 1
    receiver_id, payload = sends[0]
    assert receiver_id == ReceiverId(2)
    field_name, message = codec.decode(payload)
    assert field_name == "session_open"
    assert message.session_id == str(SESSION)  # type: ignore[attr-defined]


def _decoded_sends(sends: list[tuple[ReceiverId, bytes]]) -> list[tuple[ReceiverId, str, Any]]:
    return [(receiver_id, *codec.decode(payload)) for receiver_id, payload in sends]


async def test_receiver_hello_sends_config_then_replays_open_sessions() -> None:
    sends: list[tuple[ReceiverId, bytes]] = []
    context, _clock, _shm, _aggregator, _journal = _context(sends=sends)
    await context.authority.start()

    await _handle_manifest_seen(context, ReceiverId(1), _manifest_seen())
    sends.clear()

    await _handle_receiver_hello(context, ReceiverId(5), rx_pb2.ReceiverHello(receiver_id=5))

    decoded = _decoded_sends(sends)
    assert all(receiver_id == ReceiverId(5) for receiver_id, _, _ in decoded)
    assert [field_name for _, field_name, _ in decoded] == ["config", "session_open"]
    config = decoded[0][2]
    assert config.shm_name == "seg"
    assert config.staging_dir == "/staging"
    assert config.journal_dir == "/journal"
    assert decoded[1][2].session_id == str(SESSION)


async def test_receiver_hello_before_authority_start_sends_config_but_no_session_open() -> None:
    sends: list[tuple[ReceiverId, bytes]] = []
    unset_ready = asyncio.Event()  # authority.start() has not run
    context, _clock, _shm, _aggregator, _journal = _context(sends=sends, ready=unset_ready)

    await _handle_receiver_hello(context, ReceiverId(5), rx_pb2.ReceiverHello(receiver_id=5))

    assert context.registry.active_receivers() == frozenset({ReceiverId(5)})
    assert [field_name for _, field_name, _ in _decoded_sends(sends)] == ["config"]


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


async def test_block_decoded_after_purge_is_dropped_at_debug_without_journaling() -> None:
    context, _clock, _shm, _aggregator, journal = _context()
    await context.authority.start()
    await _handle_manifest_seen(context, ReceiverId(1), _manifest_seen())
    await context.authority.purge(SESSION, PurgeReason.INCOMPLETE)

    message = rx_pb2.BlockDecoded(session_id=str(SESSION), receiver_id=1, block_ids=[0, 1])
    with capture_logs() as logs:
        await _handle_block_decoded(context, ReceiverId(1), message)

    assert [block_id async for block_id in journal.replay(SESSION)] == []  # not journaled
    dropped = [entry for entry in logs if entry["event"] == "block_decoded_after_purge"]
    assert dropped and dropped[0]["log_level"] == "debug"
    assert not any(entry["event"] == "block_decoded_for_unknown_session" for entry in logs)


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
    from session_manager.config import (
        AggregationConfig,
        AppConfig,
        PathsConfig,
        PurgeConfig,
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
        purge=PurgeConfig(),
    )

    specs = _build_receiver_specs(config)

    assert [spec.name for spec in specs] == ["receiver-0", "receiver-1"]
    assert specs[0].argv == ["./bin/rx", "--receiver-id", "0", "--listen-port", "9100"]
    assert specs[1].argv == ["./bin/rx", "--receiver-id", "1", "--listen-port", "9101"]


async def test_handle_manifest_seen_waits_for_ready_before_touching_the_authority() -> None:
    ready = asyncio.Event()
    context, _clock, shm, _aggregator, _journal = _context(ready=ready)
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

    handler_task = asyncio.create_task(_handle_manifest_seen(context, ReceiverId(1), manifest_seen))
    await asyncio.sleep(0)
    assert shm.open_sessions() == ()  # still waiting, ready not set yet

    ready.set()
    await asyncio.wait_for(handler_task, timeout=1.0)

    assert [session.session_id for session in shm.open_sessions()] == [SESSION]


async def test_block_decoded_waits_for_ready_before_journaling() -> None:
    ready = asyncio.Event()
    context, _clock, _shm, aggregator, journal = _context(ready=ready)
    aggregator.register_session(_spec(total_blocks=4))
    message = rx_pb2.BlockDecoded(session_id=str(SESSION), receiver_id=1, block_ids=[0, 2])

    handler_task = asyncio.create_task(_handle_block_decoded(context, ReceiverId(1), message))
    await asyncio.sleep(0)
    assert [block_id async for block_id in journal.replay(SESSION)] == []  # still waiting

    ready.set()
    await asyncio.wait_for(handler_task, timeout=1.0)

    assert [block_id async for block_id in journal.replay(SESSION)] == [BlockId(0), BlockId(2)]


async def test_receiver_hello_and_heartbeat_are_never_gated_by_ready() -> None:
    ready = asyncio.Event()  # deliberately never set
    context, _clock, _shm, _aggregator, _journal = _context(ready=ready)

    await asyncio.wait_for(
        _handle_receiver_hello(context, ReceiverId(1), rx_pb2.ReceiverHello(receiver_id=1)),
        timeout=1.0,
    )
    await asyncio.wait_for(
        _handle_heartbeat(context, ReceiverId(1), ipc_pb2.Heartbeat()), timeout=1.0
    )

    assert context.registry.active_receivers() == frozenset({ReceiverId(1)})


async def test_wait_for_path_returns_once_the_path_exists(tmp_path: Path) -> None:
    # A real clock, not FakeClock: FakeClock.sleep() advances time and returns
    # without ever suspending, so a loop built on it never yields back to this
    # test to call target.touch() -- only a clock backed by real asyncio.sleep
    # lets the two coroutines interleave.
    clock = SystemClock()
    target = tmp_path / "socket"

    wait_task = asyncio.create_task(_wait_for_path(target, clock, poll_interval_s=0.01))
    await asyncio.sleep(0.03)
    assert not wait_task.done()

    target.touch()
    await asyncio.wait_for(wait_task, timeout=1.0)


async def test_wait_for_a_receiver_returns_immediately_once_alive() -> None:
    clock = FakeClock()
    registry = ReceiverRegistry(clock, heartbeat_interval_s=5.0)
    registry.register(ReceiverId(1))

    await asyncio.wait_for(
        _wait_for_a_receiver_or_timeout(registry, clock, timeout_s=10.0, poll_interval_s=0.01),
        timeout=1.0,
    )


async def test_wait_for_a_receiver_gives_up_when_nobody_reconnects() -> None:
    # FakeClock.sleep() advances time synchronously, so this loop's own polling
    # drives the clock past the deadline and returns without any external
    # advance() call or real elapsed time.
    clock = FakeClock()
    registry = ReceiverRegistry(clock, heartbeat_interval_s=5.0)

    await asyncio.wait_for(
        _wait_for_a_receiver_or_timeout(registry, clock, timeout_s=1.0, poll_interval_s=0.1),
        timeout=1.0,
    )

    assert not registry.any_alive()


async def _wait_until(predicate: Any, timeout: float = 1.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout=timeout)
