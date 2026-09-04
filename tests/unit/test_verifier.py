import pytest
from structlog.testing import capture_logs

from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec
from session_manager.services.verifier import IntegrityVerifier
from tests.fakes.fake_file_store import FakeFileStore
from tests.fakes.fake_hasher import FakeHasher

DEFAULT_DIGEST_HEX = "ab" * 32


def _spec(
    *, relpath: str = "sub/file.bin", file_hash: bytes = bytes.fromhex(DEFAULT_DIGEST_HEX)
) -> SessionSpec:
    return SessionSpec(
        session_id=SessionId("s-1"),
        relpath=relpath,
        file_size=1_000_000,
        file_hash=file_hash,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=3,
    )


async def test_a_matching_hash_verifies_true() -> None:
    verifier = IntegrityVerifier(FakeHasher(), FakeFileStore())

    assert await verifier.verify(_spec()) is True


async def test_a_mismatched_hash_verifies_false_and_logs_both_digests() -> None:
    verifier = IntegrityVerifier(FakeHasher(), FakeFileStore())
    spec = _spec(file_hash=bytes.fromhex("cd" * 32))

    with capture_logs() as logs:
        result = await verifier.verify(spec)

    assert result is False
    mismatches = [entry for entry in logs if entry["event"] == "hash_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["expected"] == "cd" * 32
    assert mismatches[0]["computed"] == DEFAULT_DIGEST_HEX


async def test_verify_hashes_the_staged_path_not_the_output_path() -> None:
    hasher = FakeHasher(digest="00" * 32)
    store = FakeFileStore()
    spec = _spec(relpath="sub/file.bin")
    hasher.set_digest(store.staged_path("sub/file.bin"), DEFAULT_DIGEST_HEX)
    verifier = IntegrityVerifier(hasher, store)

    assert await verifier.verify(spec) is True


async def test_a_hashing_failure_propagates() -> None:
    hasher = FakeHasher()
    hasher.fail_next_compute_hash(OSError("truncated read"))
    verifier = IntegrityVerifier(hasher, FakeFileStore())

    with pytest.raises(OSError, match="truncated"):
        await verifier.verify(_spec())
