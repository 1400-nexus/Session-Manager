from pathlib import Path


class FakeHasher:
    def __init__(self, digest: str = "ab" * 32) -> None:
        self._digest: str = digest

    async def compute_hash(self, path: Path) -> str:
        return self._digest
