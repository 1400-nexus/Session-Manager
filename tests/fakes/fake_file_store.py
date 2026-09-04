from pathlib import Path


class FakeFileStore:
    """In-memory FileStore: records calls, returns plausible paths.

    `fail_next_allocate` injects a `fallocate` failure (ENOSPC and friends);
    a store that always succeeds would hide the authority's ordering -- shm is
    only initialised after the staging file is reserved.
    """

    def __init__(self, root: Path = Path("/fake-store")) -> None:
        self._root: Path = root
        self.allocated: list[tuple[str, int]] = []
        self.published: list[str] = []
        self.quarantined: list[str] = []
        self._pending_allocate_error: Exception | None = None

    def fail_next_allocate(self, error: Exception) -> None:
        self._pending_allocate_error = error

    def allocate(self, relpath: str, size: int) -> Path:
        if self._pending_allocate_error is not None:
            error = self._pending_allocate_error
            self._pending_allocate_error = None
            raise error
        self.allocated.append((relpath, size))
        return self.staged_path(relpath)

    def publish(self, relpath: str) -> Path:
        self.published.append(relpath)
        return self._root / "output" / relpath

    def quarantine(self, relpath: str) -> Path:
        self.quarantined.append(relpath)
        return self._root / "quarantine" / relpath

    def staged_path(self, relpath: str) -> Path:
        return self._root / "staging" / relpath
