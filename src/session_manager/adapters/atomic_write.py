import json
import os
from pathlib import Path

from session_manager.adapters.constants import STAGED_FILE_MODE


def write_json_atomically(path: Path, data: dict[str, object]) -> None:
    """Write `data` as JSON to `path` so a crash never leaves it truncated.

    A temp file in the same directory (so `os.replace` is a same-filesystem
    rename, which is atomic), `fsync`'d, then renamed over the target: an
    interrupted write leaves either the previous file or the complete new
    one, never a half-written file the reader chokes on. Used for the two
    restart-critical sidecars -- the session spec and the incomplete-transfer
    report.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(data).encode()
    file_descriptor = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, STAGED_FILE_MODE)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except BaseException:
        # A failed write must not leave its temp file behind next to the real
        # one; the target itself is untouched until the os.replace above.
        temp_path.unlink(missing_ok=True)
        raise
