import asyncio
from collections.abc import AsyncIterator

from session_manager.domain.ids import ReceiverId


class FakeIpcServer:
    def __init__(self) -> None:
        self.sent: list[tuple[ReceiverId, bytes]] = []
        self._pending_outcomes: dict[ReceiverId, list[Exception | None]] = {}
        self._incoming: asyncio.Queue[tuple[ReceiverId, bytes]] = asyncio.Queue()

    def fail_next_send(self, receiver_id: ReceiverId, error: Exception) -> None:
        self._pending_outcomes.setdefault(receiver_id, []).append(error)

    def succeed_next_send(self, receiver_id: ReceiverId) -> None:
        self._pending_outcomes.setdefault(receiver_id, []).append(None)

    async def serve(self) -> None:
        return None

    async def send(self, receiver_id: ReceiverId, payload: bytes) -> None:
        pending = self._pending_outcomes.get(receiver_id)
        if pending:
            outcome = pending.pop(0)
            if outcome is not None:
                raise outcome
        self.sent.append((receiver_id, payload))

    def incoming(self) -> AsyncIterator[tuple[ReceiverId, bytes]]:
        return self._iter_incoming()

    async def _iter_incoming(self) -> AsyncIterator[tuple[ReceiverId, bytes]]:
        while True:
            yield await self._incoming.get()
