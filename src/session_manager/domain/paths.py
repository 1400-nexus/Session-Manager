from pathlib import PurePosixPath


def is_unsafe_relpath(relpath: str) -> bool:
    if not relpath or relpath.startswith("/") or "\\" in relpath:
        return True
    return ".." in PurePosixPath(relpath).parts
