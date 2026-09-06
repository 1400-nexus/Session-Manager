import math
from collections.abc import Awaitable, Callable, Collection
from typing import Any

import rx_pb2
import structlog

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import OpenSession, SessionSpec
from session_manager.domain.paths import is_unsafe_relpath
from session_manager.ipc import codec
from session_manager.ports.protocols import (
    FileLock,
    FileStore,
    Journal,
    SessionSpecStore,
    ShmWriter,
)
from session_manager.services.errors import ManifestRejected

logger = structlog.get_logger(__name__)

Broadcast = Callable[[bytes], Awaitable[None]]
SendToReceiver = Callable[[ReceiverId, bytes], Awaitable[None]]
# Called on the one path that opens a session -- fresh create and adopt
# recovery both -- with the decoded blocks known so far (empty for a fresh
# session). Wiring registration here rather than at each call site is what
# stops a new creation path from silently skipping progress tracking.
OnSessionOpened = Callable[[SessionSpec, Collection[BlockId]], None]


def _bitmap_bytes(total_blocks: int) -> int:
    return (total_blocks + 7) // 8


def _manifest_to_spec(manifest: Any) -> SessionSpec:
    # The wire field Manifest.block_bytes carries the SYMBOL size, not the
    # block size (see domain.models.SessionSpec.block_bytes) -- this is the
    # boundary where that copy happens.
    return SessionSpec(
        session_id=SessionId(manifest.session_id),
        relpath=manifest.filepath,
        file_size=manifest.file_size,
        file_hash=manifest.file_hash,
        k=manifest.k,
        n=manifest.n,
        symbol_bytes=manifest.block_bytes,
        total_blocks=manifest.total_blocks,
    )


def _validate_spec(spec: SessionSpec) -> None:
    if spec.k <= 0 or spec.n <= 0 or spec.symbol_bytes <= 0:
        raise ManifestRejected(spec.session_id, "k, n and symbol_bytes must all be positive")
    if spec.k >= spec.n:
        raise ManifestRejected(spec.session_id, f"k ({spec.k}) must be < n ({spec.n})")
    if spec.file_size < 0:
        raise ManifestRejected(spec.session_id, f"file_size {spec.file_size} is negative")
    expected_blocks = math.ceil(spec.file_size / spec.block_bytes) if spec.file_size else 0
    if spec.total_blocks != expected_blocks:
        raise ManifestRejected(
            spec.session_id,
            f"total_blocks {spec.total_blocks} does not match "
            f"ceil(file_size / (k * symbol_bytes)) = {expected_blocks}",
        )
    if is_unsafe_relpath(spec.relpath):
        raise ManifestRejected(
            spec.session_id,
            f"relpath {spec.relpath!r} is absolute or escapes the staging directory",
        )


class SessionAuthority:
    """Sole writer of session state.

    Every session is created here, so the three receivers all reporting the
    same manifest is not a race: the first `ManifestSeen` creates the session
    and the rest are duplicates. There is deliberately no lock between
    receivers -- adding one would look correct and be pure overhead.
    """

    def __init__(
        self,
        file_lock: FileLock,
        shm: ShmWriter,
        file_store: FileStore,
        journal: Journal,
        spec_store: SessionSpecStore,
        broadcast: Broadcast,
        send_to_receiver: SendToReceiver,
        on_session_opened: OnSessionOpened,
        shm_name: str,
        arena_bytes: int,
        session_region_base: int,
    ) -> None:
        self._file_lock: FileLock = file_lock
        self._shm: ShmWriter = shm
        self._file_store: FileStore = file_store
        self._journal: Journal = journal
        self._spec_store: SessionSpecStore = spec_store
        self._broadcast: Broadcast = broadcast
        self._send_to_receiver: SendToReceiver = send_to_receiver
        self._on_session_opened: OnSessionOpened = on_session_opened
        self._shm_name: str = shm_name
        self._arena_bytes: int = arena_bytes
        self._next_offset: int = session_region_base
        self._known: dict[SessionId, OpenSession] = {}
        self._specs: dict[SessionId, SessionSpec] = {}
        self._adopted: bool = False

    async def start(self) -> None:
        # flock before ANYTHING touches shm: two managers on one segment is
        # the worst bug available here, so the second one must fail loudly.
        self._file_lock.acquire()
        self._adopted = self._shm.create_or_adopt(self._shm_name, self._arena_bytes)
        if self._adopted:
            await self._recover()

    async def handle_manifest_seen(self, receiver_id: ReceiverId, manifest_seen: Any) -> None:
        manifest = manifest_seen.manifest
        session_id = SessionId(manifest.session_id)
        if session_id in self._known:
            # A duplicate is still information this receiver needs: its
            # ManifestSeen may have arrived after another receiver's won the
            # create race, or it reconnected after the one broadcast already
            # went out. SessionOpen is idempotent (see rx.proto), so answer
            # this receiver directly rather than re-broadcasting.
            await self._send_session_open(receiver_id, session_id)
            return

        spec = _manifest_to_spec(manifest)
        _validate_spec(spec)

        block_table_offset = self._next_offset
        bitmap_offset = block_table_offset + _bitmap_bytes(spec.total_blocks)
        region_end = bitmap_offset + _bitmap_bytes(spec.total_blocks)
        if region_end > self._arena_bytes:
            raise ManifestRejected(
                spec.session_id, f"needs {region_end} bytes, arena is {self._arena_bytes}"
            )

        self._file_store.allocate(spec.relpath, spec.file_size)
        self._shm.init_session(spec, block_table_offset, bitmap_offset)
        # Durable before broadcasting: an adopting restart after this point
        # recovers the full spec, not just the decoded blocks.
        self._spec_store.save(spec)
        self._next_offset = region_end
        open_session = OpenSession(
            session_id=session_id,
            total_blocks=spec.total_blocks,
            block_table_offset=block_table_offset,
            bitmap_offset=bitmap_offset,
        )
        self._known[session_id] = open_session
        self._specs[session_id] = spec
        # Register progress tracking on the SAME path that creates the
        # session, so a future creation path physically cannot forget to.
        self._on_session_opened(spec, ())
        logger.info("session_opened", session_id=session_id, total_blocks=spec.total_blocks)
        await self._broadcast(self._session_open_message(spec, open_session))

    async def send_open_sessions_to(self, receiver_id: ReceiverId) -> None:
        # A receiver that connects (or reconnects) after a session opened
        # never saw its broadcast; this is how it learns the session exists
        # at all -- the path a receiver restart mid-transfer depends on.
        for session_id in list(self._specs):
            await self._send_session_open(receiver_id, session_id)

    def adopted(self) -> bool:
        return self._adopted

    def shutdown(self, clean: bool = True) -> None:
        self._shm.close(unlink=clean)
        self._file_lock.release()

    async def _recover(self) -> None:
        for open_session in self._shm.open_sessions():
            self._known[open_session.session_id] = open_session
            region_end = open_session.bitmap_offset + _bitmap_bytes(open_session.total_blocks)
            self._next_offset = max(self._next_offset, region_end)
            decoded: set[BlockId] = set()
            async for block_id in self._journal.replay(open_session.session_id):
                decoded.add(block_id)

            spec = self._spec_store.load(open_session.session_id)
            if spec is None:
                # The bytes are safe (decoded set above, bitmap in shm) but
                # without the spec this session can never be handed to the
                # aggregator, so it can never verify or publish. It stays in
                # _known so a resent ManifestSeen is (correctly) treated as
                # a duplicate rather than re-init_session-ing and zeroing a
                # bitmap receivers are still writing into.
                logger.error(
                    "session_spec_missing_on_recovery",
                    session_id=open_session.session_id,
                    decoded_blocks=len(decoded),
                )
                continue
            self._specs[open_session.session_id] = spec
            self._on_session_opened(spec, frozenset(decoded))

            logger.info(
                "session_recovered",
                session_id=open_session.session_id,
                decoded_blocks=len(decoded),
            )

    async def _send_session_open(self, receiver_id: ReceiverId, session_id: SessionId) -> None:
        spec = self._specs.get(session_id)
        open_session = self._known.get(session_id)
        if spec is None or open_session is None:
            return
        await self._send_to_receiver(receiver_id, self._session_open_message(spec, open_session))

    def _session_open_message(self, spec: SessionSpec, open_session: OpenSession) -> bytes:
        # Absolute, not spec.relpath: a receiver must write to exactly this
        # path, not reconstruct one against its own idea of a staging
        # directory -- see rx.proto's SessionOpen.dest_path.
        dest_path = str(self._file_store.staged_path(spec.relpath).resolve())
        session_open = rx_pb2.SessionOpen(
            session_id=spec.session_id,
            dest_path=dest_path,
            total_blocks=spec.total_blocks,
            k=spec.k,
            n=spec.n,
            block_bytes=spec.symbol_bytes,
            block_table_offset=open_session.block_table_offset,
            bitmap_offset=open_session.bitmap_offset,
        )
        return codec.encode(session_open)
