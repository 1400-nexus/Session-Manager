from pathlib import Path


class FakeFileStore:
    """In-memory FileStore: records calls, returns plausible paths.

    `staged` tracks which relpaths currently have reserved bytes, so a test
    can prove that a failed `publish`/`quarantine` leaves the staged file
    exactly where it was -- `fail_next_publish` raises before `staged` is
    touched, the way a failed atomic rename leaves the source untouched.
    `fail_next_allocate` injects a `fallocate` failure (ENOSPC and friends);
    a store that always succeeds would hide the authority's ordering -- shm is
    only initialised after the staging file is reserved.
    """

    def __init__(self, root: Path = Path("/fake-store")) -> None:
        self._root: Path = root
        self.allocated: list[tuple[str, int]] = []
        self.published: list[str] = []
        self.quarantined: list[str] = []
        self.staged: set[str] = set()
        self._pending_allocate_error: Exception | None = None
        self._pending_publish_error: Exception | None = None
        self._pending_quarantine_error: Exception | None = None

    def fail_next_allocate(self, error: Exception) -> None:
        self._pending_allocate_error = error

    def fail_next_publish(self, error: Exception) -> None:
        self._pending_publish_error = error

    def fail_next_quarantine(self, error: Exception) -> None:
        self._pending_quarantine_error = error

    def allocate(self, relpath: str, size: int) -> Path:
        if self._pending_allocate_error is not None:
            error = self._pending_allocate_error
            self._pending_allocate_error = None
            raise error
        self.allocated.append((relpath, size))
        self.staged.add(relpath)
        return self.staged_path(relpath)

    def publish(self, relpath: str) -> Path:
        if self._pending_publish_error is not None:
            error = self._pending_publish_error
            self._pending_publish_error = None
            raise error
        self.staged.discard(relpath)
        self.published.append(relpath)
        return self._root / "output" / relpath

    def quarantine(self, relpath: str) -> Path:
        if self._pending_quarantine_error is not None:
            error = self._pending_quarantine_error
            self._pending_quarantine_error = None
            raise error
        self.staged.discard(relpath)
        self.quarantined.append(relpath)
        return self._root / "quarantine" / relpath

    def staged_path(self, relpath: str) -> Path:
        return self._root / "staging" / relpath
