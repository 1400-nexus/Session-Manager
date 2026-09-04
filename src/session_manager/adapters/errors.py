from pathlib import Path


class LockHeldError(Exception):
    def __init__(self, path: Path, holder_pid: int | None) -> None:
        held_by = f"pid {holder_pid}" if holder_pid is not None else "another process"
        super().__init__(f"lock {path} is held by {held_by}")
        self.path: Path = path
        self.holder_pid: int | None = holder_pid
