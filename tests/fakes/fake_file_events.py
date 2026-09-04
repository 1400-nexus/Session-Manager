import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path


class FakeFileEvents:
    def __init__(self) -> None:
        self._events: asyncio.Queue[Path | Exception] = asyncio.Queue()
        self.closed: bool = False

    def emit(self, path: Path) -> None:
        self._events.put_nowait(path)

    def fail(self, error: Exception) -> None:
        self._events.put_nowait(error)

    def close(self) -> None:
        self.closed = True

    async def listen(self) -> AsyncGenerator[Path, None]:
        while True:
            item = await self._events.get()
            if isinstance(item, Exception):
                raise item
            yield item
