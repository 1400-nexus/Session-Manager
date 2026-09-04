import math
from collections.abc import Awaitable, Callable
from typing import Any

import rx_pb2
import structlog

from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import OpenSession, SessionSpec
from session_manager.domain.paths import is_unsafe_relpath
from session_manager.ipc import codec
from session_manager.ports.protocols import FileLock, FileStore, Journal, ShmWriter
from session_manager.services.errors import ManifestRejected

logger = structlog.get_logger(__name__)

Broadcast = Callable[[bytes], Awaitable[None]]


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
        broadcast: Broadcast,
        shm_name: str,
        arena_bytes: int,
        session_region_base: int,
    ) -> None:
        self._file_lock: FileLock = file_lock
        self._shm: ShmWriter = shm
        self._file_store: FileStore = file_store
        self._journal: Journal = journal
        self._broadcast: Broadcast = broadcast
        self._shm_name: str = shm_name
        self._arena_bytes: int = arena_bytes
        self._next_offset: int = session_region_base
        self._known: dict[SessionId, OpenSession] = {}
        self._recovered: dict[SessionId, frozenset[BlockId]] = {}
        self._adopted: bool = False

    async def start(self) -> None:
        # flock before ANYTHING touches shm: two managers on one segment is
        # the worst bug available here, so the second one must fail loudly.
        self._file_lock.acquire()
        self._adopted = self._shm.create_or_adopt(self._shm_name, self._arena_bytes)
        if self._adopted:
            await self._recover()

    async def handle_manifest_seen(self, manifest_seen: Any) -> None:
        manifest = manifest_seen.manifest
        session_id = SessionId(manifest.session_id)
        if session_id in self._known:
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
        self._next_offset = region_end
        self._known[session_id] = OpenSession(
            session_id=session_id,
            total_blocks=spec.total_blocks,
            block_table_offset=block_table_offset,
            bitmap_offset=bitmap_offset,
        )
        logger.info("session_opened", session_id=session_id, total_blocks=spec.total_blocks)
        await self._broadcast_session_open(spec, block_table_offset, bitmap_offset)

    def recovered_blocks(self, session_id: SessionId) -> frozenset[BlockId]:
        return self._recovered.get(session_id, frozenset())

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
            self._recovered[open_session.session_id] = frozenset(decoded)
            logger.info(
                "session_recovered",
                session_id=open_session.session_id,
                decoded_blocks=len(decoded),
            )

    async def _broadcast_session_open(
        self, spec: SessionSpec, block_table_offset: int, bitmap_offset: int
    ) -> None:
        session_open = rx_pb2.SessionOpen(
            session_id=spec.session_id,
            dest_path=spec.relpath,
            total_blocks=spec.total_blocks,
            k=spec.k,
            n=spec.n,
            block_bytes=spec.symbol_bytes,
            block_table_offset=block_table_offset,
            bitmap_offset=bitmap_offset,
        )
        await self._broadcast(codec.encode(session_open))
