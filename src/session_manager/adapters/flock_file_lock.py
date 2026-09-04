import fcntl
import os
from pathlib import Path

from session_manager.adapters.constants import LOCK_PID_READ_BYTES
from session_manager.adapters.errors import LockHeldError


class FlockFileLock:
    """`fcntl.flock` advisory lock, with the holder's pid written into the file.

    flock alone does not tell you who holds the lock, so on a successful
    acquire this writes `os.getpid()` into the file; a failed acquire reads it
    back to name the offending process. The kernel drops the flock when the fd
    closes (or the process dies), so a crashed manager never wedges its
    successor.
    """

    def __init__(self, path: Path) -> None:
        self._path: Path = path
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            holder = self._read_holder_pid(fd)
            os.close(fd)
            raise LockHeldError(self._path, holder) from error
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        os.fsync(fd)
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None

    def holder_pid(self) -> int | None:
        if self._fd is not None:
            return os.getpid()
        try:
            fd = os.open(self._path, os.O_RDONLY)
        except FileNotFoundError:
            return None
        try:
            return self._read_holder_pid(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _read_holder_pid(fd: int) -> int | None:
        try:
            raw = os.pread(fd, LOCK_PID_READ_BYTES, 0).strip()
        except OSError:
            return None
        try:
            return int(raw)
        except ValueError:
            return None
