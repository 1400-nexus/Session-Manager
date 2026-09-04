from pathlib import Path


class FakeHasher:
    """Fixed or per-path digests, with failure injection.

    `set_digest` seeds a specific hex digest for a path (a real file's
    content never has to exist); `fail_next_compute_hash` injects a hashing
    failure (a truncated read, permission error) so callers that assume
    hashing never fails don't pass by accident.
    """

    def __init__(self, digest: str = "ab" * 32) -> None:
        self._default_digest: str = digest
        self._digest_by_path: dict[Path, str] = {}
        self._pending_errors: list[Exception] = []

    def set_digest(self, path: Path, digest: str) -> None:
        self._digest_by_path[path] = digest

    def fail_next_compute_hash(self, error: Exception) -> None:
        self._pending_errors.append(error)

    async def compute_hash(self, path: Path) -> str:
        if self._pending_errors:
            raise self._pending_errors.pop(0)
        return self._digest_by_path.get(path, self._default_digest)
