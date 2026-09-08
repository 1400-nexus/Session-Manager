import asyncio
import io
from collections.abc import Sequence

from rich.console import Console
from structlog.testing import capture_logs

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import (
    ReceiverCounters,
    SessionSnapshot,
    SessionSpec,
    SessionState,
)
from session_manager.services.constants import LOSS_CRITICAL_PCT, LOSS_WARNING_PCT
from session_manager.services.status_display import (
    StatusDisplay,
    _idle_style,
    _loss_style,
    _prominent_if_nonzero,
    log_status,
    render,
)

PURGE_STALL_TIMEOUT_S = 60.0


class _NeverResolvingClock:
    def now(self) -> float:
        return 0.0

    async def sleep(self, seconds: float) -> None:
        await asyncio.get_running_loop().create_future()


def _spec(session_id: str = "s-1", total_blocks: int = 4) -> SessionSpec:
    return SessionSpec(
        session_id=SessionId(session_id),
        relpath="sub/file.bin",
        file_size=1_000_000,
        file_hash=b"\x00" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=total_blocks,
    )


def _snapshot(
    *,
    session_id: str = "s-1",
    total_blocks: int = 4,
    blocks_decoded: int = 4,
    state: SessionState = SessionState.COMPLETE,
    observed_loss_pct: float = 0.0,
    per_receiver: tuple[tuple[ReceiverId, ReceiverCounters], ...] = (),
    live_receivers: frozenset[ReceiverId] = frozenset(),
    missing_blocks: tuple[BlockId, ...] = (),
    missing_block_count: int = 0,
    seconds_since_progress: float = 0.0,
) -> SessionSnapshot:
    return SessionSnapshot(
        spec=_spec(session_id, total_blocks),
        state=state,
        blocks_decoded=blocks_decoded,
        total_blocks=total_blocks,
        observed_loss_pct=observed_loss_pct,
        per_receiver=per_receiver,
        live_receivers=live_receivers,
        missing_blocks=missing_blocks,
        missing_block_count=missing_block_count,
        seconds_since_progress=seconds_since_progress,
    )


def _rendered_text(snapshots: Sequence[SessionSnapshot], *, force_terminal: bool = False) -> str:
    buffer = io.StringIO()
    console = Console(file=buffer, width=120, force_terminal=force_terminal)
    console.print(render(snapshots, PURGE_STALL_TIMEOUT_S))
    return buffer.getvalue()


def test_render_with_no_sessions_shows_a_placeholder_with_no_terminal() -> None:
    text = _rendered_text([])

    assert "no active sessions" in text
    assert "no receivers" in text


def test_render_shows_a_complete_session() -> None:
    snapshot = _snapshot(state=SessionState.COMPLETE, blocks_decoded=4, total_blocks=4)

    text = _rendered_text([snapshot])

    assert "s-1" in text
    assert "COMPLETE" in text
    assert "4/4" in text
    assert "100.0%" in text


def test_render_shows_a_stalled_session_with_its_missing_block_count() -> None:
    snapshot = _snapshot(
        state=SessionState.INCOMPLETE,
        blocks_decoded=2,
        total_blocks=5,
        missing_blocks=(BlockId(1), BlockId(3), BlockId(4)),
        missing_block_count=3,
    )

    text = _rendered_text([snapshot])

    assert "INCOMPLETE" in text
    assert "2/5" in text
    assert "3" in text


def test_render_shows_a_dead_receiver() -> None:
    receiver_id = ReceiverId(2)
    counters = ReceiverCounters(pkts_ok=100, crc_fail=3)
    snapshot = _snapshot(per_receiver=((receiver_id, counters),), live_receivers=frozenset())

    text = _rendered_text([snapshot])

    assert "dead" in text


def test_render_shows_an_alive_receiver() -> None:
    receiver_id = ReceiverId(1)
    snapshot = _snapshot(
        per_receiver=((receiver_id, ReceiverCounters(pkts_ok=500)),),
        live_receivers=frozenset({receiver_id}),
    )

    text = _rendered_text([snapshot])

    assert "alive" in text


def test_loss_style_escalates_at_the_configured_thresholds() -> None:
    assert _loss_style(0.0) == "bold green"
    assert _loss_style(LOSS_WARNING_PCT) == "bold yellow"
    assert _loss_style(LOSS_CRITICAL_PCT) == "bold red"


def test_idle_style_escalates_relative_to_the_stall_timeout() -> None:
    timeout = 60.0
    assert _idle_style(0.0, timeout) == ""
    assert _idle_style(29.0, timeout) == ""  # under 50%
    assert _idle_style(30.0, timeout) == "yellow"  # 50%
    assert _idle_style(53.0, timeout) == "yellow"  # under 90%
    assert _idle_style(54.0, timeout) == "bold red"  # 90%


def test_an_open_session_going_quiet_shows_a_styled_idle_time() -> None:
    # The regression this replaces: the aggregator used to flip State to
    # INCOMPLETE early; now it only marks INCOMPLETE at the timeout, so the
    # live view must warn on its own before then.
    snapshot = _snapshot(
        state=SessionState.OPEN, blocks_decoded=2, total_blocks=5, seconds_since_progress=55.0
    )

    forced = _rendered_text([snapshot], force_terminal=True)

    assert "55s" in forced
    assert "\x1b[1;31m" in forced  # bold red, 55s > 0.9 * 60s


def test_a_terminal_session_shows_no_idle_time() -> None:
    for state in (SessionState.COMPLETE, SessionState.INCOMPLETE, SessionState.VERIFIED):
        text = _rendered_text([_snapshot(state=state, seconds_since_progress=999.0)])
        assert "999s" not in text


def test_crc_fail_kernel_drops_and_arena_exhausted_are_prominent_when_nonzero() -> None:
    assert _prominent_if_nonzero(0).style == ""
    assert _prominent_if_nonzero(7).style == "bold red"


def test_render_emits_ansi_when_force_terminal_and_plain_text_otherwise() -> None:
    snapshot = _snapshot(observed_loss_pct=20.0)

    forced = _rendered_text([snapshot], force_terminal=True)
    plain = _rendered_text([snapshot], force_terminal=False)

    assert "\x1b[" in forced
    assert "\x1b[" not in plain


async def _run_briefly(display: StatusDisplay) -> None:
    run_task = asyncio.create_task(display.run())
    try:
        await asyncio.sleep(0)
    finally:
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)


async def test_run_emits_status_immediately_then_blocks_when_not_a_terminal() -> None:
    provider_calls = 0

    def provider() -> Sequence[SessionSnapshot]:
        nonlocal provider_calls
        provider_calls += 1
        return ()

    console = Console(file=io.StringIO(), force_terminal=False, width=80)
    display = StatusDisplay(
        console,
        _NeverResolvingClock(),
        refresh_interval_s=5.0,
        snapshots_provider=provider,
        stall_timeout_s=PURGE_STALL_TIMEOUT_S,
    )

    run_task = asyncio.create_task(display.run())
    try:
        await asyncio.sleep(0)
        assert provider_calls == 1  # one status line up front, then it blocks on the interval
        assert not run_task.done()
    finally:
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)


def test_log_status_emits_one_event_with_the_same_fields_as_the_tables() -> None:
    receiver = ReceiverId(1)
    snapshot = _snapshot(
        session_id="m4",
        blocks_decoded=3,
        total_blocks=10,
        state=SessionState.OPEN,
        observed_loss_pct=1.234,
        per_receiver=((receiver, ReceiverCounters(pkts_ok=42, crc_fail=1)),),
        live_receivers=frozenset({receiver}),
        seconds_since_progress=12.34,
    )
    with capture_logs() as logs:
        log_status([snapshot])

    events = [entry for entry in logs if entry["event"] == "status"]
    assert len(events) == 1
    assert events[0]["sessions"] == [
        {
            "session_id": "m4",
            "state": "OPEN",
            "blocks_decoded": 3,
            "total_blocks": 10,
            "observed_loss_pct": 1.23,
            "seconds_since_last_block": 12.3,
            "missing_block_count": 0,
        }
    ]
    assert events[0]["receivers"] == [
        {
            "receiver_id": 1,
            "status": "alive",
            "pkts_ok": 42,
            "crc_fail": 1,
            "duplicates": 0,
            "kernel_drops": 0,
            "arena_exhausted": 0,
            "arena_high_water_pct": 0,
        }
    ]


async def test_run_uses_live_and_logs_nothing_when_a_terminal() -> None:
    buffer = io.StringIO()
    console = Console(file=buffer, force_terminal=True, width=80)
    display = StatusDisplay(
        console,
        _NeverResolvingClock(),
        refresh_interval_s=5.0,
        snapshots_provider=lambda: (),
        stall_timeout_s=PURGE_STALL_TIMEOUT_S,
    )

    with capture_logs() as logs:
        await _run_briefly(display)

    assert not any(entry["event"] == "status" for entry in logs)
    assert "Sessions" in buffer.getvalue()  # rich.Live drew the tables instead
