import pytest

from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import SessionSpec
from session_manager.ports.protocols import ShmReader, ShmWriter
from tests.fakes.fake_shm import (
    DEFAULT_FAKE_ARENA_BYTES,
    AdoptDecision,
    FakeShm,
    SegmentState,
)

SESSION = SessionId("s-1")


def _spec(total_blocks: int = 16) -> SessionSpec:
    return SessionSpec(
        session_id=SESSION,
        relpath="sub/file.bin",
        file_size=1_000_000,
        file_hash=b"\x00" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=total_blocks,
    )


def _prepared() -> FakeShm:
    fake = FakeShm()
    fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)
    fake.init_session(_spec(), block_table_offset=64, bitmap_offset=128)
    return fake


def test_fake_shm_satisfies_both_ports() -> None:
    fake = FakeShm()
    reader: ShmReader = fake
    writer: ShmWriter = fake
    assert isinstance(reader, FakeShm)
    assert isinstance(writer, FakeShm)


def test_bitmap_for_returns_a_readonly_view_that_rejects_writes() -> None:
    view = _prepared().bitmap_for(SESSION)

    assert view.readonly is True
    with pytest.raises(TypeError):
        view[0] = 1


def test_bitmap_for_reflects_blocks_a_receiver_marked_decoded() -> None:
    fake = _prepared()
    fake.mark_block_decoded(SESSION, BlockId(0))
    fake.mark_block_decoded(SESSION, BlockId(9))

    bitmap = fake.bitmap_for(SESSION)

    assert bitmap[0] == 0b0000_0001
    assert bitmap[1] == 0b0000_0010


def test_block_table_seen_reads_back_marked_blocks() -> None:
    fake = _prepared()

    assert fake.block_table_seen(SESSION, BlockId(3)) is False
    fake.mark_block_table_seen(SESSION, BlockId(3))
    assert fake.block_table_seen(SESSION, BlockId(3)) is True


@pytest.mark.parametrize("state", list(SegmentState))
def test_each_starting_state_is_reachable_and_reports_itself(state: SegmentState) -> None:
    fake = FakeShm()
    fake.set_existing_segment(state)
    assert fake.current_segment_state() is state


def test_set_receiver_alive_changes_what_the_liveness_probe_sees() -> None:
    fake = FakeShm()
    fake.set_existing_segment(SegmentState.STALE)
    assert fake.probe_receiver_alive() is False
    assert fake.current_segment_state() is SegmentState.STALE

    fake.set_receiver_alive(True)
    assert fake.probe_receiver_alive() is True
    assert fake.current_segment_state() is SegmentState.LIVE


def test_absent_segment_is_created_and_zeroed() -> None:
    fake = FakeShm()
    fake.set_existing_segment(SegmentState.ABSENT)

    adopted = fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)

    assert adopted is False
    assert fake.last_decision is AdoptDecision.CREATED
    assert fake.payload_is_zeroed() is True


def test_live_segment_is_adopted_and_the_bytes_a_receiver_held_survive() -> None:
    fake = FakeShm()
    fake.set_existing_segment(SegmentState.LIVE)
    fake.seed_payload(b"receiver-owned-slot-state")
    before = fake.payload_bytes()

    adopted = fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)

    assert adopted is True
    assert fake.last_decision is AdoptDecision.ADOPTED
    assert fake.payload_is_zeroed() is False
    assert fake.payload_bytes() == before


def test_stale_segment_is_reinitialised_and_zeroed() -> None:
    fake = FakeShm()
    fake.set_existing_segment(SegmentState.STALE)
    fake.seed_payload(b"leftover from a dead run")

    adopted = fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)

    assert adopted is False
    assert fake.last_decision is AdoptDecision.REINITIALISED
    assert fake.payload_is_zeroed() is True


def test_incompatible_segment_is_reinitialised_even_when_a_receiver_answers() -> None:
    fake = FakeShm()
    fake.set_existing_segment(SegmentState.INCOMPATIBLE)
    fake.set_receiver_alive(True)
    fake.seed_payload(b"garbage from an older build")

    adopted = fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)

    assert adopted is False
    assert fake.last_decision is AdoptDecision.REINITIALISED
    assert fake.payload_is_zeroed() is True


def test_create_or_adopt_can_be_made_to_fail() -> None:
    fake = FakeShm()
    fake.fail_next_create_or_adopt(OSError("shm_open: ENOMEM"))

    with pytest.raises(OSError, match="ENOMEM"):
        fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)


def test_init_session_can_be_made_to_fail() -> None:
    fake = FakeShm()
    fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)
    fake.fail_next_init_session(RuntimeError("ftruncate failed"))

    with pytest.raises(RuntimeError, match="ftruncate"):
        fake.init_session(_spec(), block_table_offset=64, bitmap_offset=128)


def test_init_session_rejects_a_bitmap_that_does_not_fit_the_arena() -> None:
    fake = FakeShm()
    fake.create_or_adopt("seg", DEFAULT_FAKE_ARENA_BYTES)

    with pytest.raises(ValueError, match="does not fit"):
        fake.init_session(
            _spec(total_blocks=8), block_table_offset=0, bitmap_offset=DEFAULT_FAKE_ARENA_BYTES
        )


def test_close_with_unlink_drops_the_segment() -> None:
    fake = _prepared()

    fake.close(unlink=True)

    assert fake.closed is True
    assert fake.unlinked is True
    assert fake.current_segment_state() is SegmentState.ABSENT
