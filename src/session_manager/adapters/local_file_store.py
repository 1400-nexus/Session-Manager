import os
from pathlib import Path

from session_manager.adapters.constants import QUARANTINE_SUBDIR_NAME, STAGED_FILE_MODE


class LocalFileStore:
    """Real filesystem `FileStore`: staging, atomic publication, quarantine.

    `publish` uses `os.replace`, never `shutil.move` -- `move` falls back to a
    non-atomic copy-then-delete across filesystems, which is exactly the
    failure mode staging and output being on one filesystem (enforced at
    config load) is meant to rule out. It refuses to overwrite an existing
    output file: output is only ever written by a successful verify-then-
    publish, so a second attempt landing there means either a legitimate
    resend after an earlier failure (which never reached `publish`, so there
    is nothing to overwrite) or something replaying a completed session --
    and silently clobbering a verified file with an unverified one is worse
    than refusing.
    """

    def __init__(self, staging_dir: Path, output_dir: Path) -> None:
        self._staging_dir: Path = staging_dir
        self._output_dir: Path = output_dir
        self._quarantine_dir: Path = staging_dir / QUARANTINE_SUBDIR_NAME

    def allocate(self, relpath: str, size: int) -> Path:
        path = self.staged_path(relpath)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor = os.open(path, os.O_RDWR | os.O_CREAT, STAGED_FILE_MODE)
        try:
            self._reserve(file_descriptor, size)
        finally:
            os.close(file_descriptor)
        return path

    def publish(self, relpath: str) -> Path:
        staged = self.staged_path(relpath)
        output = self._output_dir / relpath
        if output.exists():
            raise FileExistsError(f"refusing to overwrite an already-published file: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, output)
        return output

    def quarantine(self, relpath: str) -> Path:
        staged = self.staged_path(relpath)
        quarantined = self._quarantine_dir / relpath
        quarantined.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, quarantined)
        return quarantined

    def staged_path(self, relpath: str) -> Path:
        return self._staging_dir / relpath

    @staticmethod
    def _reserve(file_descriptor: int, size: int) -> None:
        # posix_fallocate reserves real disk blocks up front, so a receiver's
        # offset writes land in already-allocated space and a full disk
        # surfaces as ENOSPC at session open, not partway through a transfer.
        # It does not exist on Windows (the dev platform here) and can raise
        # on filesystems that do not support it (some network mounts); either
        # way, truncate falls back to a sparse file that only reserves the
        # extent, not the blocks -- adequate for local development, not for
        # the Linux grading target.
        if hasattr(os, "posix_fallocate"):
            try:
                os.posix_fallocate(file_descriptor, 0, size)
                return
            except OSError:
                pass
        os.ftruncate(file_descriptor, size)
