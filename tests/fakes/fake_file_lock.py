from pathlib import Path

from session_manager.adapters.errors import LockHeldError


class FakeFileLock:
    """In-memory FileLock.

    `held_by` models a lock another process already owns: `acquire` then
    raises `LockHeldError` with that pid. `fail_next_acquire` injects an
    arbitrary failure. A fake that could not refuse would make "second manager
    refuses to start" pass without testing anything.
    """

    def __init__(self, path: Path = Path("fake.lock"), held_by: int | None = None) -> None:
        self._path: Path = path
        self._external_holder: int | None = held_by
        self._acquired: bool = False
        self._pending_error: Exception | None = None
        self.acquire_count: int = 0
        self.release_count: int = 0

    def fail_next_acquire(self, error: Exception) -> None:
        self._pending_error = error

    def acquire(self) -> None:
        self.acquire_count += 1
        if self._pending_error is not None:
            error = self._pending_error
            self._pending_error = None
            raise error
        if self._external_holder is not None:
            raise LockHeldError(self._path, self._external_holder)
        self._acquired = True

    def release(self) -> None:
        self.release_count += 1
        self._acquired = False

    def holder_pid(self) -> int | None:
        if self._acquired:
            return 0
        return self._external_holder

    @property
    def acquired(self) -> bool:
        return self._acquired
