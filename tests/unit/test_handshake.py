from pathlib import Path

import blake3
import pytest

from session_manager.domain.ids import ReceiverId
from session_manager.ipc.errors import ProtoHashMismatchError
from session_manager.ipc.handshake import compute_proto_hash, verify_proto_hash

REPO_PROTO_DIR = Path(__file__).resolve().parents[2] / "libs" / "nexus-proto" / "proto"


def test_compute_proto_hash_is_stable_across_calls(tmp_path: Path) -> None:
    (tmp_path / "foo.proto").write_bytes(b"message Foo { int32 bar = 1; }")
    assert compute_proto_hash(tmp_path) == compute_proto_hash(tmp_path)


def test_compute_proto_hash_changes_when_a_comment_changes(tmp_path: Path) -> None:
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / "foo.proto").write_bytes(b"message Foo { int32 bar = 1; }")

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / "foo.proto").write_bytes(b"// a comment\nmessage Foo { int32 bar = 1; }")

    assert compute_proto_hash(dir_a) != compute_proto_hash(dir_b)


def test_compute_proto_hash_respects_filename_ordering(tmp_path: Path) -> None:
    (tmp_path / "b.proto").write_bytes(b"second")
    (tmp_path / "a.proto").write_bytes(b"first")

    expected = blake3.blake3(b"firstsecond").digest()
    assert compute_proto_hash(tmp_path) == expected


def test_compute_proto_hash_is_raw_bytes_concatenated_with_no_separator(tmp_path: Path) -> None:
    (tmp_path / "a.proto").write_bytes(b"AAA")
    (tmp_path / "b.proto").write_bytes(b"BBB")

    assert compute_proto_hash(tmp_path) == blake3.blake3(b"AAABBB").digest()


def test_compute_proto_hash_is_a_blake3_digest(tmp_path: Path) -> None:
    (tmp_path / "foo.proto").write_bytes(b"message Foo {}")
    assert len(compute_proto_hash(tmp_path)) == blake3.blake3().digest_size


def test_compute_proto_hash_against_the_real_contract() -> None:
    digest = compute_proto_hash(REPO_PROTO_DIR)
    assert len(digest) == blake3.blake3().digest_size


def test_verify_proto_hash_accepts_a_match() -> None:
    verify_proto_hash(ReceiverId(1), b"abc", b"abc")


def test_verify_proto_hash_rejects_a_mismatch() -> None:
    with pytest.raises(ProtoHashMismatchError):
        verify_proto_hash(ReceiverId(1), b"abc", b"xyz")
