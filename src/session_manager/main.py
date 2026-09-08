import asyncio
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog
from rich.console import Console

from session_manager.adapters.append_journal import AppendJournal
from session_manager.adapters.asyncio_process_spawner import AsyncioProcessSpawner
from session_manager.adapters.blake3_hasher import Blake3Hasher
from session_manager.adapters.constants import SHM_SESSION_TABLE_BYTES, SHM_SESSION_TABLE_OFFSET
from session_manager.adapters.errors import LockHeldError
from session_manager.adapters.json_session_spec_store import JsonSessionSpecStore
from session_manager.adapters.local_file_store import LocalFileStore
from session_manager.adapters.posix_shm import PosixShm
from session_manager.adapters.system_clock import SystemClock
from session_manager.config import AppConfig, load_config
from session_manager.constants import (
    ADOPT_GRACE_PERIOD_SECONDS,
    CONFIG_ERROR_EXIT_CODE,
    DEFAULT_CONFIG_PATH,
    DEFAULT_PROTO_CONTRACT_DIR,
    NEXUS_CONFIG_ENV_VAR,
    PROTO_CONTRACT_DIR_ENV_VAR,
    STARTUP_POLL_INTERVAL_SECONDS,
)
from session_manager.domain.blocks import block_byte_range
from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.purge_policy import PurgeReason
from session_manager.ipc import codec, handshake
from session_manager.ipc.constants import (
    BLOCK_DECODED_FIELD_NAME,
    HEARTBEAT_FIELD_NAME,
    MANIFEST_SEEN_FIELD_NAME,
    RECEIVER_HELLO_FIELD_NAME,
    RECEIVER_STATS_FIELD_NAME,
)
from session_manager.ipc.uds import UdsIpcServer
from session_manager.ports.protocols import Clock, IpcServer, Journal
from session_manager.services.aggregator import ProgressAggregator
from session_manager.services.authority import SessionAuthority
from session_manager.services.constants import HEARTBEAT_INTERVAL_SECONDS
from session_manager.services.publisher import Publisher
from session_manager.services.receiver_registry import ReceiverRegistry
from session_manager.services.status_display import StatusDisplay
from session_manager.services.verifier import IntegrityVerifier
from session_manager.supervision.supervisor import ChildSpec, ProcessSupervisor

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DispatchContext:
    registry: ReceiverRegistry
    authority: SessionAuthority
    aggregator: ProgressAggregator
    journal: Journal
    # Set once authority.start() has run. manifest_seen/block_decoded touch
    # shm (via the authority/journal) and must wait for it; receiver_hello/
    # heartbeat never touch shm and run immediately -- that asymmetry is
    # what lets a receiver reconnect and register during the adopt grace
    # window below, before create_or_adopt has even been called.
    ready: asyncio.Event


IncomingMessageHandler = Callable[[DispatchContext, ReceiverId, Any], Awaitable[None]]


async def _handle_receiver_hello(
    context: DispatchContext, receiver_id: ReceiverId, message: Any
) -> None:
    context.registry.register(receiver_id)
    logger.info("receiver_connected", receiver_id=receiver_id)
    # Config carries the shm name / staging dir / journal dir so a receiver
    # never learns them by out-of-band convention. Static, so it goes out
    # immediately -- no need to wait on authority.start().
    await context.authority.send_config_to(receiver_id)
    # Replaying open sessions, on the other hand, has to wait: before
    # authority.start() there is nothing to replay and _recover() may still
    # be mutating session state. Registry stays unconditional above -- that
    # is what lets a reconnecting receiver be seen alive during the grace
    # window.
    if context.ready.is_set():
        await context.authority.send_open_sessions_to(receiver_id)


async def _handle_heartbeat(
    context: DispatchContext, receiver_id: ReceiverId, message: Any
) -> None:
    context.registry.refresh(receiver_id)


async def _handle_manifest_seen(
    context: DispatchContext, receiver_id: ReceiverId, message: Any
) -> None:
    await context.ready.wait()
    await context.authority.handle_manifest_seen(receiver_id, message)


async def _handle_receiver_stats(
    context: DispatchContext, receiver_id: ReceiverId, message: Any
) -> None:
    context.aggregator.handle_receiver_stats(receiver_id, message)


async def _handle_block_decoded(
    context: DispatchContext, receiver_id: ReceiverId, message: Any
) -> None:
    await context.ready.wait()
    session_id = SessionId(message.session_id)
    if context.authority.is_purged(session_id):
        # An expected in-flight race, not an error: the sweep or the
        # completion path purged this session and a receiver's last few
        # BlockDecoded are still on the wire. Drop them without journaling.
        logger.debug("block_decoded_after_purge", session_id=session_id, receiver_id=receiver_id)
        return
    spec = context.aggregator.spec_for(session_id)
    if spec is not None:
        for raw_block_id in message.block_ids:
            if not 0 <= raw_block_id < spec.total_blocks:
                continue
            block_id = BlockId(raw_block_id)
            offset, length = block_byte_range(spec, block_id)
            # journal.append BEFORE folding into the aggregator's in-memory
            # set: if that order were reversed and the process crashed
            # between the two, the block would be reported complete but
            # absent from the journal, and a restart would think it's
            # missing bytes the receivers already finished writing.
            context.journal.append(session_id, block_id, offset, length)
    context.aggregator.handle_block_decoded(receiver_id, session_id, message.block_ids)


INCOMING_MESSAGE_HANDLERS: dict[str, IncomingMessageHandler] = {
    RECEIVER_HELLO_FIELD_NAME: _handle_receiver_hello,
    HEARTBEAT_FIELD_NAME: _handle_heartbeat,
    MANIFEST_SEEN_FIELD_NAME: _handle_manifest_seen,
    RECEIVER_STATS_FIELD_NAME: _handle_receiver_stats,
    BLOCK_DECODED_FIELD_NAME: _handle_block_decoded,
}


async def _run_incoming_dispatch_loop(ipc: IpcServer, context: DispatchContext) -> None:
    async for receiver_id, raw in ipc.incoming():
        field_name, message = codec.decode(raw)
        handler = INCOMING_MESSAGE_HANDLERS.get(field_name)
        if handler is None:
            logger.warning(
                "unknown_incoming_message_type", field_name=field_name, receiver_id=receiver_id
            )
            continue
        await handler(context, receiver_id, message)


async def _wait_for_path(path: Path, clock: Clock, poll_interval_s: float) -> None:
    while not path.exists():
        await clock.sleep(poll_interval_s)


async def _wait_for_a_receiver_or_timeout(
    registry: ReceiverRegistry, clock: Clock, timeout_s: float, poll_interval_s: float
) -> None:
    # Only matters on the adopt path (a segment already exists from a
    # previous run): a receiver whose connection just died in a crash needs
    # a moment to notice and reconnect before create_or_adopt asks whether
    # anyone is alive. Bounded, so a fresh start with no receivers waiting
    # to reconnect doesn't hang -- it just spends this long doing nothing,
    # once, at startup.
    deadline = clock.now() + timeout_s
    while not registry.any_alive() and clock.now() < deadline:
        await clock.sleep(poll_interval_s)


async def _cancel_workers_on_shutdown(
    shutdown_event: asyncio.Event, worker_tasks: list[asyncio.Task[None]]
) -> None:
    await shutdown_event.wait()
    logger.info("shutdown_in_progress")
    for task in worker_tasks:
        task.cancel()


def _build_receiver_specs(config: AppConfig) -> list[ChildSpec]:
    return [
        ChildSpec(
            name=f"receiver-{index}",
            argv=[
                config.receivers.binary_path,
                "--receiver-id",
                str(index),
                "--listen-port",
                str(config.receivers.ports[index]),
            ],
        )
        for index in range(config.receivers.count)
    ]


def _install_shutdown_signal_handlers(
    loop: asyncio.AbstractEventLoop, trigger_shutdown: Callable[[], None]
) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, trigger_shutdown)
        except NotImplementedError:
            # Windows: asyncio event loops don't implement
            # add_signal_handler at all. signal.signal() covers SIGINT
            # there (Ctrl+C); its handler runs outside the loop, so hand
            # off via call_soon_threadsafe rather than touching asyncio
            # state directly from it.
            signal.signal(sig, lambda *_args: loop.call_soon_threadsafe(trigger_shutdown))


async def run(config: AppConfig, shutdown_event: asyncio.Event | None = None) -> int:
    proto_dir = Path(os.environ.get(PROTO_CONTRACT_DIR_ENV_VAR, DEFAULT_PROTO_CONTRACT_DIR))
    if not proto_dir.is_dir():
        logger.error(
            "proto_contract_missing",
            proto_dir=str(proto_dir),
            hint="the nexus-proto submodule may not be initialised — "
            "run: git submodule update --init",
        )
        return 1
    expected_proto_hash = handshake.compute_proto_hash(proto_dir)

    # Imported here, not at module scope: fcntl (and so this whole adapter
    # module) does not exist on Windows, and importing session_manager.main
    # is otherwise platform-independent -- useful for testing everything in
    # this file except an actual run() on a non-POSIX dev box.
    from session_manager.adapters.flock_file_lock import FlockFileLock

    clock = SystemClock()
    hasher = Blake3Hasher()
    spawner = AsyncioProcessSpawner()
    file_store = LocalFileStore(config.paths.staging_dir, config.paths.output_dir)
    journal = AppendJournal(config.paths.journal_dir)
    # Sidecar next to the journal, not a wider shm session table: this file
    # is Python-only, so it can carry the full SessionSpec without adding
    # anything B's C++ receivers need to parse.
    spec_store = JsonSessionSpecStore(config.paths.journal_dir)
    ipc = UdsIpcServer(config.paths.socket_path, expected_proto_hash=expected_proto_hash)
    file_lock = FlockFileLock(config.paths.lock_path)
    registry = ReceiverRegistry(clock, heartbeat_interval_s=HEARTBEAT_INTERVAL_SECONDS)
    shm = PosixShm(slot_bytes=config.shm.slot_bytes, probe_receiver_alive=registry.any_alive)

    async def broadcast(payload: bytes) -> None:
        for receiver_id in registry.active_receivers():
            try:
                await ipc.send(receiver_id, payload)
            except Exception as error:
                logger.error(
                    "session_open_broadcast_failed", receiver_id=receiver_id, error=str(error)
                )

    async def send_to_receiver(receiver_id: ReceiverId, payload: bytes) -> None:
        try:
            await ipc.send(receiver_id, payload)
        except Exception as error:
            logger.error("session_open_send_failed", receiver_id=receiver_id, error=str(error))

    verifier = IntegrityVerifier(hasher, file_store)
    publisher = Publisher(file_store)

    async def on_complete(snapshot: Any) -> None:
        session_id = snapshot.spec.session_id
        try:
            hash_ok = await verifier.verify(snapshot.spec)
            reason = authority.classify_completion(session_id, hash_ok=hash_ok)
            if reason is PurgeReason.PUBLISHED:
                publisher.publish(snapshot.spec)
                aggregator.mark_verified(session_id)
            else:
                if authority.was_recovered(session_id):
                    # A mismatch right after an adopt is a recovery hole -- a
                    # block journaled before its bytes were durable (see
                    # rx.proto BlockDecoded) -- not FEC corruption. Log it
                    # apart so it is not chased as the latter.
                    logger.error("recovered_session_failed_verification", session_id=session_id)
                publisher.quarantine(snapshot.spec)
                aggregator.mark_hash_mismatch(session_id)
            await authority.purge(session_id, reason)
        except Exception as error:
            # on_complete runs inside ProgressAggregator.poll(), inside the
            # TaskGroup -- an unhandled exception here would propagate out
            # of poll(), fail that task, and cancel every sibling task. One
            # bad session must not take down the process.
            logger.error("session_completion_failed", session_id=session_id, error=str(error))

    # `aggregator` does not exist yet when `on_complete` is defined above --
    # that's fine, `on_complete`'s body isn't executed until a session
    # actually completes, by which point the assignment below has run.
    # Built before the authority because the authority's on_session_opened
    # callback is aggregator.register_session -- the one wiring that makes a
    # fresh session and its progress record impossible to create separately.
    aggregator = ProgressAggregator(
        clock=clock,
        shm_reader=shm,
        on_complete=on_complete,
        live_receivers=registry.active_receivers,
        poll_interval_s=config.aggregation.poll_interval_s,
        shm_crosscheck=config.aggregation.shm_crosscheck,
    )

    # Session regions are allocated after the session table, which starts
    # right after the header (see adapters/shm_layout.py) -- the authority
    # owns this allocation policy, ShmWriter.init_session just takes offsets.
    session_region_base = SHM_SESSION_TABLE_OFFSET + SHM_SESSION_TABLE_BYTES
    authority = SessionAuthority(
        file_lock=file_lock,
        shm=shm,
        file_store=file_store,
        journal=journal,
        spec_store=spec_store,
        broadcast=broadcast,
        send_to_receiver=send_to_receiver,
        on_session_opened=aggregator.register_session,
        clock=clock,
        progress_of=aggregator.progress_of,
        on_incomplete=aggregator.mark_incomplete,
        shm_name=config.shm.name,
        staging_dir=str(config.paths.staging_dir),
        journal_dir=str(config.paths.journal_dir),
        arena_bytes=config.shm.arena_bytes,
        session_region_base=session_region_base,
        sweep_interval_s=config.purge.sweep_interval_s,
        stall_timeout_s=config.purge.stall_timeout_s,
    )
    # force_terminal=False would pin is_terminal OFF even on a real PTY, so
    # `rich` never live-renders and StatusDisplay's `while True` prints
    # nothing. None means auto-detect (correct under compose `tty: true`);
    # the config flag only forces rendering ON, for a no-TTY container whose
    # tables must still reach `docker compose logs`.
    force_terminal = True if config.status.force_terminal else None
    status_display = StatusDisplay(
        Console(force_terminal=force_terminal),
        clock,
        config.status.refresh_interval_s,
        snapshots_provider=aggregator.snapshots,
        stall_timeout_s=config.purge.stall_timeout_s,
    )
    supervisor = ProcessSupervisor(_build_receiver_specs(config), spawner, clock)

    authority_ready = asyncio.Event()
    context = DispatchContext(
        registry=registry,
        authority=authority,
        aggregator=aggregator,
        journal=journal,
        ready=authority_ready,
    )

    loop = asyncio.get_running_loop()
    shutdown_event = shutdown_event if shutdown_event is not None else asyncio.Event()

    def trigger_shutdown() -> None:
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    _install_shutdown_signal_handlers(loop, trigger_shutdown)

    exit_code = 0
    clean_shutdown = True
    try:
        async with asyncio.TaskGroup() as task_group:
            # Bind the socket and start accepting connections immediately --
            # a receiver reconnecting after a crash needs somewhere to say
            # ReceiverHello before create_or_adopt (below) decides whether
            # anyone is alive. manifest_seen/block_decoded wait on `ready`,
            # so nothing actually touches shm before authority.start() runs.
            ipc_task = task_group.create_task(ipc.serve())
            dispatch_task = task_group.create_task(_run_incoming_dispatch_loop(ipc, context))

            await _wait_for_path(config.paths.socket_path, clock, STARTUP_POLL_INTERVAL_SECONDS)
            await _wait_for_a_receiver_or_timeout(
                registry, clock, ADOPT_GRACE_PERIOD_SECONDS, STARTUP_POLL_INTERVAL_SECONDS
            )

            lock_error: LockHeldError | None = None
            try:
                # flock (inside authority.start(), before create_or_adopt) is
                # acquired before anything else touches shm -- two managers
                # on one segment is the worst bug available here.
                await authority.start()
            except LockHeldError as error:
                lock_error = error

            if lock_error is not None:
                logger.error(
                    "lock_held_by_another_process",
                    path=str(lock_error.path),
                    holder_pid=lock_error.holder_pid,
                )
                exit_code = 1
                ipc_task.cancel()
                dispatch_task.cancel()
            else:
                authority_ready.set()

                # Recovered sessions were already handed to the aggregator by
                # authority.start() via the on_session_opened callback -- the
                # same path a fresh session takes -- so there is nothing to
                # seed here.

                worker_tasks = [
                    ipc_task,
                    task_group.create_task(supervisor.run()),
                    dispatch_task,
                    task_group.create_task(aggregator.run()),
                    task_group.create_task(status_display.run()),
                ]
                task_group.create_task(_cancel_workers_on_shutdown(shutdown_event, worker_tasks))
                logger.info(
                    "session_manager_listening",
                    socket_path=str(config.paths.socket_path),
                    shm_name=config.shm.name,
                    adopted=authority.adopted(),
                )
    except* asyncio.CancelledError:
        pass  # defensive: normal shutdown exits the block above without raising
    except* Exception as exception_group:
        for task_error in exception_group.exceptions:
            logger.error("task_failed", error=str(task_error))
        exit_code = 1
        clean_shutdown = False

    await supervisor.shutdown()
    await authority.stop()  # stop the sweep loop before tearing down further
    journal.sync()
    config.paths.socket_path.unlink(missing_ok=True)
    # authority.shutdown() closes shm (unlink only if this was a clean exit,
    # so a surviving receiver keeps its mapping on a crash path) and then
    # releases the lock, in that order -- it is the sole owner of both, so
    # main.py doesn't reach around it to interleave the socket unlink
    # between the two. Safe even if authority.start() never got past
    # acquiring the lock (or never ran at all): both close(unlink=...) and
    # release() are no-ops when nothing was actually acquired.
    authority.shutdown(clean=clean_shutdown)

    return exit_code


def main() -> int:
    config_path = Path(os.environ.get(NEXUS_CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH))
    try:
        config = load_config(config_path)
    except (ValueError, OSError) as error:
        logger.error("invalid_config", config_path=str(config_path), error=str(error))
        return CONFIG_ERROR_EXIT_CODE
    return asyncio.run(run(config))


if __name__ == "__main__":
    sys.exit(main())
