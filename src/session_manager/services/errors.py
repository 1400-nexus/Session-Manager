class ManifestRejected(ValueError):
    def __init__(self, session_id: str, reason: str) -> None:
        super().__init__(f"manifest for session {session_id!r} rejected: {reason}")
        self.session_id: str = session_id
        self.reason: str = reason


class PublishRejected(ValueError):
    def __init__(self, relpath: str, reason: str) -> None:
        super().__init__(f"publish rejected for {relpath!r}: {reason}")
        self.relpath: str = relpath
        self.reason: str = reason
