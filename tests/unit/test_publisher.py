from pathlib import Path

import pytest

from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec
from session_manager.services.errors import PublishRejected
from session_manager.services.publisher import Publisher
from tests.fakes.fake_file_store import FakeFileStore


def _spec(*, relpath: str = "sub/file.bin") -> SessionSpec:
    return SessionSpec(
        session_id=SessionId("s-1"),
        relpath=relpath,
        file_size=1_000_000,
        file_hash=b"\x00" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=3,
    )


def test_publish_delegates_to_the_file_store_and_clears_staged() -> None:
    store = FakeFileStore(root=Path("/store"))
    store.staged.add("sub/file.bin")
    publisher = Publisher(store)

    published_path = publisher.publish(_spec())

    assert published_path == Path("/store/output/sub/file.bin")
    assert store.published == ["sub/file.bin"]
    assert "sub/file.bin" not in store.staged


def test_quarantine_delegates_and_never_touches_published() -> None:
    store = FakeFileStore(root=Path("/store"))
    store.staged.add("sub/file.bin")
    publisher = Publisher(store)

    quarantined_path = publisher.quarantine(_spec())

    assert quarantined_path == Path("/store/quarantine/sub/file.bin")
    assert store.quarantined == ["sub/file.bin"]
    assert store.published == []
    assert "sub/file.bin" not in store.staged


@pytest.mark.parametrize(
    "relpath",
    ["/etc/passwd", "../../etc/passwd", "sub/../../escape", "sub\\..\\x", ""],
)
def test_publish_rejects_an_unsafe_relpath_before_any_write(relpath: str) -> None:
    store = FakeFileStore()
    publisher = Publisher(store)

    with pytest.raises(PublishRejected):
        publisher.publish(_spec(relpath=relpath))

    assert store.published == []


def test_quarantine_also_rejects_an_unsafe_relpath() -> None:
    store = FakeFileStore()
    publisher = Publisher(store)

    with pytest.raises(PublishRejected):
        publisher.quarantine(_spec(relpath="../escape"))

    assert store.quarantined == []


def test_a_publish_failure_mid_rename_leaves_the_staged_file_intact() -> None:
    store = FakeFileStore()
    store.staged.add("sub/file.bin")
    store.fail_next_publish(OSError("EXDEV: cross-device link"))
    publisher = Publisher(store)

    with pytest.raises(OSError, match="EXDEV"):
        publisher.publish(_spec())

    assert "sub/file.bin" in store.staged
    assert store.published == []


def test_a_quarantine_failure_leaves_the_staged_file_intact() -> None:
    store = FakeFileStore()
    store.staged.add("sub/file.bin")
    store.fail_next_quarantine(OSError("EIO"))
    publisher = Publisher(store)

    with pytest.raises(OSError, match="EIO"):
        publisher.quarantine(_spec())

    assert "sub/file.bin" in store.staged
    assert store.quarantined == []
