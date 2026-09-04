import asyncio
from collections.abc import Sequence


class AsyncioProcessSpawner:
    async def spawn(
        self, argv: Sequence[str], env: dict[str, str] | None = None
    ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(*argv, env=env)

    def terminate(self, process: asyncio.subprocess.Process) -> None:
        process.terminate()

    def kill(self, process: asyncio.subprocess.Process) -> None:
        process.kill()

    async def wait(self, process: asyncio.subprocess.Process) -> int:
        return await process.wait()
