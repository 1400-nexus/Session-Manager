from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Protocol, TypeVar

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import OpenSession, SessionSpec

ProcessHandle = TypeVar("ProcessHandle")


class Clock(Protocol):
    """Monotonic time source.

    `now` never goes backwards between calls on one instance. `sleep`
    suspends the caller for at least `seconds` and is cancellable at the
    suspension point.
    """

    def now(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class Hasher(Protocol):
    """File content hash.

    `compute_hash` is pure over the file's bytes: identical content yields an
    identical digest regardless of path, host, or run. The digest algorithm
    must match the one the C++ side verifies against (BLAKE3).
    """

    async def compute_hash(self, path: Path) -> str: ...


class IpcServer(Protocol):
    """Message transport to the connected receiver processes.

    `serve` accepts connections until cancelled. `send` raises
    `UnknownReceiverError` if that receiver is not currently connected and
    `SendQueueFullError` if its send queue is saturated -- it never blocks the
    caller. `incoming` yields every framed message in arrival order; a slow
    consumer applies backpressure rather than dropping messages.
    """

    async def serve(self) -> None: ...

    async def send(self, receiver_id: ReceiverId, payload: bytes) -> None: ...

    def incoming(self) -> AsyncIterator[tuple[ReceiverId, bytes]]: ...


class ProcessSpawner(Protocol[ProcessHandle]):
    """Child-process lifecycle over an opaque handle.

    `wait` returns the child's exit status; both `wait` and the signalling
    methods raise `ProcessLookupError` if the child has already been reaped,
    so callers must tolerate that rather than assuming the handle is live.
    """

    async def spawn(
        self, argv: Sequence[str], env: dict[str, str] | None = None
    ) -> ProcessHandle: ...

    def terminate(self, process: ProcessHandle) -> None: ...

    def kill(self, process: ProcessHandle) -> None: ...

    async def wait(self, process: ProcessHandle) -> int: ...


class ShmReader(Protocol):
    """Read-only attachment to the completion shared-memory segment.

    The segment is written by the C++ receivers and by `ShmWriter`; this port
    never mutates it. `bitmap_for` returns a read-only `memoryview` -- a
    writable view into receiver memory turns decoded blocks into silent
    garbage with no error raised anywhere, so the view's `readonly` flag must
    be true. `block_table_seen` is the per-block cross-check the aggregator
    uses to locate a disagreement with its UDS-derived count.
    """

    def attach(self, name: str) -> None: ...

    def bitmap_for(self, session_id: SessionId) -> memoryview: ...

    def block_table_seen(self, session_id: SessionId, block_id: BlockId) -> bool: ...


class ShmWriter(Protocol):
    """Sole writer of session state in the shared-memory segment.

    Exactly one process holds this at a time (enforced by an flock taken
    before attach). `create_or_adopt` returns True when it adopted a segment a
    live receiver is already using and False when it created a fresh one --
    the caller must not choose between the two, because the decision depends
    on whether a receiver answers and that logic belongs in one place.
    `init_session` and `purge_session` are the only ways session state
    changes; `open_sessions` re-reads the on-segment session table so an
    authority that adopted a live segment learns which sessions are already
    running rather than re-creating them. `close(unlink=True)` removes the
    segment; pass False on an unclean shutdown so a surviving receiver keeps
    its mapping.
    """

    def create_or_adopt(self, name: str, arena_bytes: int) -> bool: ...

    def init_session(
        self, spec: SessionSpec, block_table_offset: int, bitmap_offset: int
    ) -> None: ...

    def open_sessions(self) -> tuple[OpenSession, ...]: ...

    def purge_session(self, session_id: SessionId) -> None: ...

    def close(self, unlink: bool) -> None: ...


class FileStore(Protocol):
    """Staging area plus atomic publication to the output directory.

    Staging and output must sit on one filesystem: `publish` is a rename, and
    across filesystems a rename degrades to a non-atomic copy that a reader
    can observe half-written. `allocate` reserves space up front (`fallocate`)
    so a transfer fails before it starts rather than midway. `quarantine`
    keeps the staged bytes on a hash mismatch -- they are the only evidence
    for diagnosing what corrupted them, so nothing deletes them. `staged_path`
    is a pure path computation and touches no filesystem.
    """

    def allocate(self, relpath: str, size: int) -> Path: ...

    def publish(self, relpath: str) -> Path: ...

    def quarantine(self, relpath: str) -> Path: ...

    def staged_path(self, relpath: str) -> Path: ...


class Journal(Protocol):
    """Append-only record of decoded blocks, for restart recovery.

    `append` need not be durable when it returns; `sync` is the durability
    barrier -- once it returns, a crash must not lose any record appended
    before it. `replay` streams the block ids recorded for a session in the
    order they were appended, so a restarting authority can rebuild its
    decoded set without re-reading the shared-memory bitmap.
    """

    def append(
        self, session_id: SessionId, block_id: BlockId, offset: int, length: int
    ) -> None: ...

    def replay(self, session_id: SessionId) -> AsyncIterator[BlockId]: ...

    def sync(self) -> None: ...


class SessionSpecStore(Protocol):
    """Durable `SessionSpec` storage, keyed by session id.

    `ShmWriter.open_sessions()` (the on-segment session table) carries only
    session_id/total_blocks/offsets -- not enough to rebuild a full
    `SessionSpec` after a restart. This is the missing piece: an authority
    that adopts a live segment loads the spec for each recovered session
    from here, rather than needing the sender to resend a `ManifestSeen`
    that would otherwise be treated as a duplicate and dropped. `save` must
    be durable before it returns -- it is called once, at session creation,
    not on a hot path. `load` returns `None` for a session that was never
    saved or whose record is unreadable; it does not raise, since a missing
    spec on recovery is a degraded-but-survivable state, not this port's
    failure to report.
    """

    def save(self, spec: SessionSpec) -> None: ...

    def load(self, session_id: SessionId) -> SessionSpec | None: ...

    def delete(self, session_id: SessionId) -> None: ...


class FileLock(Protocol):
    """Single-holder advisory lock on a path.

    `acquire` is non-blocking: it raises `LockHeldError` (carrying the holding
    pid, read from the lock file) rather than waiting, because two managers on
    one shared-memory segment is the worst bug available here and the second
    one must fail loudly, not queue. `release` is idempotent. The lock is held
    for the process lifetime, so there is no timeout.
    """

    def acquire(self) -> None: ...

    def release(self) -> None: ...

    def holder_pid(self) -> int | None: ...
