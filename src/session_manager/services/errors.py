class ManifestRejected(ValueError):
    def __init__(self, session_id: str, reason: str) -> None:
        super().__init__(f"manifest for session {session_id!r} rejected: {reason}")
        self.session_id: str = session_id
        self.reason: str = reason
