from pathlib import PurePosixPath


def is_unsafe_relpath(relpath: str) -> bool:
    if not relpath or relpath.startswith("/") or "\\" in relpath:
        return True
    return ".." in PurePosixPath(relpath).parts


def is_unsafe_filename_component(value: str) -> bool:
    # For a value (session_id, today) that becomes a whole filename rather
    # than a joinable relative path -- no leading-slash or ".." traversal
    # check needed, just "does this stay inside one path segment".
    return not value or "/" in value or "\\" in value or value in {".", ".."}
