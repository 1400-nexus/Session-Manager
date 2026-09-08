from pathlib import PurePosixPath

from session_manager.domain.ids import SessionId


def quarantine_name(relpath: str, session_id: SessionId) -> str:
    """The relative name a quarantined file takes: `<stem>.<session_id><ext>`.

    Two sessions can quarantine the same filename in one manager run -- a
    hash mismatch and an incomplete transfer of `report.bin`, or two of
    either. `os.replace` would silently clobber the first, destroying its
    evidence. The session id (``secrets.token_hex(8)``, unique per transfer)
    disambiguates them; it goes *before* the extension so the name still
    sorts beside its siblings and still opens as the right type:
    ``reports/q3.bin`` + ``a3f9c1d2e4b50678`` -> ``reports/q3.a3f9c1d2e4b50678.bin``.
    It can only collide if one session quarantines twice, which the purge
    dedup rules out.
    """
    name = PurePosixPath(relpath)
    return str(name.with_name(f"{name.stem}.{session_id}{name.suffix}"))
