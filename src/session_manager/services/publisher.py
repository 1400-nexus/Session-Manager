from pathlib import Path

import structlog

from session_manager.domain.models import IncompleteReport, SessionSpec
from session_manager.domain.paths import is_unsafe_relpath
from session_manager.ports.protocols import FileStore
from session_manager.services.errors import PublishRejected

logger = structlog.get_logger(__name__)


class Publisher:
    """Publishes a verified session, or quarantines a failed one.

    A filesystem mutation with no CPU work of its own -- the opposite of
    `IntegrityVerifier`, which is why they are two classes. The caller decides
    which method to call; this class never re-verifies. `relpath` is
    re-checked here even though `SessionAuthority` already rejected an unsafe
    one at manifest time: this is the point a value that arrived over a
    corrupting link becomes a filesystem write, so it gets its own defence
    rather than trusting an earlier one.
    """

    def __init__(self, file_store: FileStore) -> None:
        self._file_store: FileStore = file_store

    def publish(self, spec: SessionSpec) -> Path:
        self._reject_unsafe(spec.relpath)
        published_path = self._file_store.publish(spec.relpath)
        logger.info("session_published", session_id=spec.session_id, path=str(published_path))
        return published_path

    def quarantine(self, spec: SessionSpec) -> Path:
        self._reject_unsafe(spec.relpath)
        quarantined_path = self._file_store.quarantine(spec.relpath)
        logger.error("session_quarantined", session_id=spec.session_id, path=str(quarantined_path))
        return quarantined_path

    def quarantine_incomplete(self, spec: SessionSpec, report: IncompleteReport) -> Path:
        # The sweep's terminal path for a stalled session: same move as
        # `quarantine`, plus a record of what the partial holds. A distinct
        # log event from `session_quarantined` -- that one means a hash
        # mismatch, and downstream (the milestone harness included) reads it
        # that way.
        self._reject_unsafe(spec.relpath)
        quarantined_path = self._file_store.quarantine_incomplete(spec.relpath, report)
        logger.warning(
            "incomplete_partial_quarantined",
            session_id=spec.session_id,
            path=str(quarantined_path),
            decoded_blocks=report.decoded_blocks,
            missing_block_count=len(report.missing_block_ids),
        )
        return quarantined_path

    def _reject_unsafe(self, relpath: str) -> None:
        if is_unsafe_relpath(relpath):
            raise PublishRejected(relpath, "absolute or escapes the staging directory")
