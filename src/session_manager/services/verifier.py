import structlog

from session_manager.domain.models import SessionSpec
from session_manager.ports.protocols import FileStore, Hasher

logger = structlog.get_logger(__name__)


class IntegrityVerifier:
    """Hashes the staged file and compares it to the manifest's digest.

    Pure CPU work with no side effects on the filesystem: it never publishes,
    never quarantines, and never deletes anything. `Hasher.compute_hash`
    already runs the digest on a thread (see adapters/blake3_hasher.py), so
    this coroutine never blocks the event loop even for a large file.
    """

    def __init__(self, hasher: Hasher, file_store: FileStore) -> None:
        self._hasher: Hasher = hasher
        self._file_store: FileStore = file_store

    async def verify(self, spec: SessionSpec) -> bool:
        staged_path = self._file_store.staged_path(spec.relpath)
        computed_hex = await self._hasher.compute_hash(staged_path)
        expected_hex = spec.file_hash.hex()
        matches = computed_hex == expected_hex
        if not matches:
            logger.error(
                "hash_mismatch",
                session_id=spec.session_id,
                relpath=spec.relpath,
                expected=expected_hex,
                computed=computed_hex,
            )
        return matches
