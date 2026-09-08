from collections.abc import AsyncIterator, Iterable

from session_manager.domain.ids import BlockId, SessionId


class FakeJournal:
    """In-memory Journal.

    `preload` seeds records as if a previous run had written them, so a test
    can prove restart recovery replays them. `fail_next_replay` /
    `fail_next_append` inject failure -- a journal that cannot fail would let
    a broken recovery path pass.
    """

    def __init__(self) -> None:
        self._records: dict[SessionId, list[BlockId]] = {}
        self.sync_count: int = 0
        self.purged: list[SessionId] = []
        self._pending_replay_error: Exception | None = None
        self._pending_append_error: Exception | None = None

    def preload(self, session_id: SessionId, block_ids: Iterable[BlockId]) -> None:
        self._records[session_id] = list(block_ids)

    def fail_next_replay(self, error: Exception) -> None:
        self._pending_replay_error = error

    def fail_next_append(self, error: Exception) -> None:
        self._pending_append_error = error

    def append(self, session_id: SessionId, block_id: BlockId, offset: int, length: int) -> None:
        if self._pending_append_error is not None:
            error = self._pending_append_error
            self._pending_append_error = None
            raise error
        self._records.setdefault(session_id, []).append(block_id)

    async def replay(self, session_id: SessionId) -> AsyncIterator[BlockId]:
        if self._pending_replay_error is not None:
            error = self._pending_replay_error
            self._pending_replay_error = None
            raise error
        for block_id in self._records.get(session_id, []):
            yield block_id

    def sync(self) -> None:
        self.sync_count += 1

    def purge(self, session_id: SessionId) -> None:
        self._records.pop(session_id, None)
        self.purged.append(session_id)
