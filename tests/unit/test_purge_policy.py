import pytest

from session_manager.domain.purge_policy import (
    PurgeReason,
    SessionProgress,
    is_complete,
    is_stalled,
    terminal_reason,
)

# The policy has no default -- config is the only source of truth. A stand-in
# for that config value in these unit tests.
TIMEOUT = 60.0


def prog(
    decoded: int, total: int = 10, opened: float = 0.0, last: float | None = None
) -> SessionProgress:
    return SessionProgress(
        total_blocks=total, decoded_blocks=decoded, opened_at=opened, last_block_at=last
    )


# --- live sessions are not terminal ----------------------------------------


def test_a_session_making_progress_is_not_terminal() -> None:
    assert terminal_reason(prog(4, last=100.0), now=101.0, stall_timeout=TIMEOUT) is None


def test_a_complete_session_awaiting_verification_is_not_terminal() -> None:
    # hash_ok is None: the verifier is still running. Purging here would
    # free the receiver's resources under a live read.
    assert (
        terminal_reason(prog(10, last=100.0), now=100.0, hash_ok=None, stall_timeout=TIMEOUT)
        is None
    )


# --- the two reasons that already existed ----------------------------------


def test_complete_and_hash_matches_is_published() -> None:
    assert (
        terminal_reason(prog(10), now=100.0, hash_ok=True, stall_timeout=TIMEOUT)
        is PurgeReason.PUBLISHED
    )


def test_complete_and_hash_mismatches_is_quarantined() -> None:
    assert (
        terminal_reason(prog(10), now=100.0, hash_ok=False, stall_timeout=TIMEOUT)
        is PurgeReason.QUARANTINED
    )


def test_a_complete_session_is_never_stalled_however_old() -> None:
    # It has nowhere left to progress to; calling it stalled would race
    # the verifier and produce INCOMPLETE for a file we actually have.
    assert not is_stalled(prog(10, last=0.0), now=1_000_000.0, stall_timeout=TIMEOUT)
    assert terminal_reason(
        prog(10, last=0.0), now=1_000_000.0, hash_ok=True, stall_timeout=TIMEOUT
    ) is (PurgeReason.PUBLISHED)


# --- INCOMPLETE: the reason that was previously undefined ------------------


def test_an_unfinished_session_past_the_stall_timeout_is_incomplete() -> None:
    p = prog(7, last=100.0)
    assert terminal_reason(p, now=100.0 + TIMEOUT, stall_timeout=TIMEOUT) is PurgeReason.INCOMPLETE


def test_an_unfinished_session_just_inside_the_stall_timeout_is_live() -> None:
    p = prog(7, last=100.0)
    assert terminal_reason(p, now=100.0 + TIMEOUT - 0.001, stall_timeout=TIMEOUT) is None


def test_a_session_that_never_received_a_block_stalls_from_opened_at() -> None:
    # Manifest arrived, then nothing ever did. Without this the session
    # sits open forever holding a staging file and a session-table slot.
    p = prog(0, opened=50.0, last=None)
    assert terminal_reason(p, now=50.0 + TIMEOUT, stall_timeout=TIMEOUT) is (PurgeReason.INCOMPLETE)
    assert terminal_reason(p, now=50.0 + 1.0, stall_timeout=TIMEOUT) is None


def test_stall_timeout_is_caller_supplied() -> None:
    p = prog(7, last=100.0)
    assert terminal_reason(p, now=105.0, stall_timeout=1.0) is PurgeReason.INCOMPLETE
    assert terminal_reason(p, now=105.0, stall_timeout=600.0) is None


def test_a_late_block_resets_the_stall_clock() -> None:
    # Fold BlockDecoded, then re-evaluate: the same wall-clock instant that
    # was terminal before the block is live after it.
    now = 200.0
    assert (
        terminal_reason(prog(7, last=100.0), now=now, stall_timeout=TIMEOUT)
        is PurgeReason.INCOMPLETE
    )
    assert terminal_reason(prog(8, last=199.0), now=now, stall_timeout=TIMEOUT) is None


# --- guards ----------------------------------------------------------------


def test_completion_is_exact_not_a_threshold() -> None:
    assert is_complete(prog(10))
    assert not is_complete(prog(9))


@pytest.mark.parametrize(
    "total,decoded",
    [(0, 0), (-1, 0), (10, -1), (10, 11)],
)
def test_nonsensical_progress_is_rejected_at_construction(total: int, decoded: int) -> None:
    with pytest.raises(ValueError):
        SessionProgress(total_blocks=total, decoded_blocks=decoded, opened_at=0.0)


def test_stall_timeout_is_required_not_defaulted() -> None:
    # A silent 60s fallback that only fires when config wiring is broken is
    # worse than a TypeError -- config is the only source of truth.
    with pytest.raises(TypeError):
        terminal_reason(prog(7, last=100.0), now=200.0)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        is_stalled(prog(7, last=100.0), now=200.0)  # type: ignore[call-arg]
