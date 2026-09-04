import asyncio
from collections.abc import Sequence

KILL_EXIT_CODE = -9


class FakeProcess:
    def __init__(self, argv: Sequence[str], env: dict[str, str] | None) -> None:
        self.argv: Sequence[str] = argv
        self.env: dict[str, str] | None = env
        self.terminated: bool = False
        self.killed: bool = False
        self.exit_code: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        self.reaped: bool = False


class FakeSpawner:
    def __init__(self) -> None:
        self.spawned: list[FakeProcess] = []
        self._pending_spawn_errors: list[OSError] = []

    def fail_next_spawn(self, error: OSError) -> None:
        self._pending_spawn_errors.append(error)

    async def spawn(self, argv: Sequence[str], env: dict[str, str] | None = None) -> FakeProcess:
        if self._pending_spawn_errors:
            raise self._pending_spawn_errors.pop(0)
        process = FakeProcess(argv, env)
        self.spawned.append(process)
        return process

    def terminate(self, process: FakeProcess) -> None:
        if process.reaped:
            raise ProcessLookupError(process.argv)
        process.terminated = True

    def kill(self, process: FakeProcess) -> None:
        if process.reaped:
            raise ProcessLookupError(process.argv)
        process.killed = True
        if not process.exit_code.done():
            process.exit_code.set_result(KILL_EXIT_CODE)

    async def wait(self, process: FakeProcess) -> int:
        if process.reaped:
            raise ProcessLookupError(process.argv)
        return await process.exit_code

    def exit(self, process: FakeProcess, code: int) -> None:
        if not process.exit_code.done():
            process.exit_code.set_result(code)
