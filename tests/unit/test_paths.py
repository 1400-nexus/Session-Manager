import pytest

from session_manager.domain.paths import is_unsafe_filename_component, is_unsafe_relpath


@pytest.mark.parametrize(
    "relpath", ["/etc/passwd", "../../etc/passwd", "sub/../../escape", "sub\\..\\x", ""]
)
def test_is_unsafe_relpath_rejects(relpath: str) -> None:
    assert is_unsafe_relpath(relpath) is True


@pytest.mark.parametrize("relpath", ["sub/dir/file.bin", "file.bin", "a/b/c.mp4"])
def test_is_unsafe_relpath_accepts(relpath: str) -> None:
    assert is_unsafe_relpath(relpath) is False


@pytest.mark.parametrize("value", ["", "a/b", "a\\b", ".", ".."])
def test_is_unsafe_filename_component_rejects(value: str) -> None:
    assert is_unsafe_filename_component(value) is True


@pytest.mark.parametrize("value", ["s-1", "session_1234", "a.b.c"])
def test_is_unsafe_filename_component_accepts(value: str) -> None:
    assert is_unsafe_filename_component(value) is False
