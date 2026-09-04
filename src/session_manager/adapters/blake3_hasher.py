import asyncio
from pathlib import Path

import blake3

from session_manager.adapters.constants import CHUNK_SIZE


class Blake3Hasher:
    @staticmethod
    def _hash_file_sync(path: Path) -> str:
        hasher = blake3.blake3()
        with open(path, "rb") as file_handle:
            while True:
                chunk = file_handle.read(CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    async def compute_hash(self, path: Path) -> str:
        return await asyncio.to_thread(self._hash_file_sync, path)
