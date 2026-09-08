from pathlib import Path

from session_manager.adapters.quarantine_paths import quarantine_name
from session_manager.domain.ids import SessionId
from session_manager.domain.models import IncompleteReport


class FakeFileStore:
    """In-memory FileStore: records calls, returns plausible paths.

    `staged` tracks which relpaths currently have reserved bytes, so a test
    can prove that a failed `publish`/`quarantine` leaves the staged file
    exactly where it was -- `fail_next_publish` raises before `staged` is
    touched, the way a failed atomic rename leaves the source untouched.
    `fail_next_allocate` injects a `fallocate` failure (ENOSPC and friends);
    a store that always succeeds would hide the authority's ordering -- shm is
    only initialised after the staging file is reserved.

    `quarantined` and `incomplete_reports` record what the store was *handed*
    -- the (relpath, session_id) of each move, and the report keyed by its
    session id -- not a name derived through `quarantine_name`. The returned
    `Path` is still `quarantine_name`-shaped so callers and log lines get a
    plausible one, but tests assert on the recorded inputs. `quarantine_name`
    itself is pinned only by `tests/unit/test_quarantine_paths.py`.
    """

    def __init__(self, root: Path = Path("/fake-store")) -> None:
        self._root: Path = root
        self.allocated: list[tuple[str, int]] = []
        self.published: list[str] = []
        self.quarantined: list[tuple[str, SessionId]] = []
        self.incomplete_reports: dict[SessionId, IncompleteReport] = {}
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

    def quarantine(self, relpath: str, session_id: SessionId) -> Path:
        if self._pending_quarantine_error is not None:
            error = self._pending_quarantine_error
            self._pending_quarantine_error = None
            raise error
        self.staged.discard(relpath)
        self.quarantined.append((relpath, session_id))
        return self._root / "quarantine" / quarantine_name(relpath, session_id)

    def quarantine_incomplete(self, relpath: str, report: IncompleteReport) -> Path:
        quarantined = self.quarantine(relpath, report.session_id)
        self.incomplete_reports[report.session_id] = report
        return quarantined

    def staged_file_exists(self, relpath: str) -> bool:
        return relpath in self.staged

    def staged_path(self, relpath: str) -> Path:
        return self._root / "staging" / relpath
