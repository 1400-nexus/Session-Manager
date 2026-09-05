from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec


class FakeSessionSpecStore:
    """In-memory SessionSpecStore.

    `drop` simulates the sidecar being lost or unreadable independently of
    `delete` (a real corruption, not a deliberate cleanup) -- the case
    `SessionAuthority._recover` must survive without crashing.
    """

    def __init__(self) -> None:
        self._specs: dict[SessionId, SessionSpec] = {}

    def save(self, spec: SessionSpec) -> None:
        self._specs[spec.session_id] = spec

    def load(self, session_id: SessionId) -> SessionSpec | None:
        return self._specs.get(session_id)

    def delete(self, session_id: SessionId) -> None:
        self._specs.pop(session_id, None)

    def drop(self, session_id: SessionId) -> None:
        self._specs.pop(session_id, None)
