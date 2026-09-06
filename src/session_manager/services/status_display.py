from collections.abc import Callable, Sequence
from typing import Any

import structlog
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.table import Table
from rich.text import Text

from session_manager.domain.ids import ReceiverId
from session_manager.domain.models import ReceiverCounters, SessionSnapshot
from session_manager.domain.progress import completion_ratio
from session_manager.ports.protocols import Clock
from session_manager.services.constants import (
    LOSS_CRITICAL_PCT,
    LOSS_WARNING_PCT,
    STATUS_LOG_INTERVAL_SECONDS,
)

logger = structlog.get_logger(__name__)

SnapshotsProvider = Callable[[], Sequence[SessionSnapshot]]

_PERCENT = 100.0
_LOSS_PCT_DIGITS = 2


def _loss_style(observed_loss_pct: float) -> str:
    if observed_loss_pct >= LOSS_CRITICAL_PCT:
        return "bold red"
    if observed_loss_pct >= LOSS_WARNING_PCT:
        return "bold yellow"
    return "bold green"


def _prominent_if_nonzero(value: int) -> Text:
    # crc_fail, kernel_drops, and arena_exhausted are three unrelated
    # failure modes -- a corrupting router, a host too slow to read packets,
    # and decode falling behind the wire -- whose fixes have nothing in
    # common. This is what tells an operator which one they have.
    return Text(str(value), style="bold red" if value else "")


def _merged_receivers(
    snapshots: Sequence[SessionSnapshot],
) -> tuple[frozenset[ReceiverId], dict[ReceiverId, ReceiverCounters]]:
    # ReceiverStats is receiver-level health, not session-level, so every
    # snapshot in one poll cycle carries the same live set and counters --
    # merge rather than pick one, so render() stays correct even if that
    # ever stops being true.
    live: set[ReceiverId] = set()
    counters: dict[ReceiverId, ReceiverCounters] = {}
    for snapshot in snapshots:
        live |= snapshot.live_receivers
        counters.update(dict(snapshot.per_receiver))
    return frozenset(live), counters


def _sessions_table(snapshots: Sequence[SessionSnapshot]) -> Table:
    table = Table(title="Sessions", expand=True)
    table.add_column("Session")
    table.add_column("State")
    table.add_column("Progress", justify="right")
    table.add_column("Loss %", justify="right")
    table.add_column("Missing", justify="right")

    if not snapshots:
        table.add_row("(no active sessions)", "", "", "", "")
        return table

    for snapshot in snapshots:
        percent_done = _PERCENT * completion_ratio(snapshot.blocks_decoded, snapshot.total_blocks)
        progress = f"{snapshot.blocks_decoded}/{snapshot.total_blocks} ({percent_done:.1f}%)"
        loss = Text(
            f"{snapshot.observed_loss_pct:.2f}%", style=_loss_style(snapshot.observed_loss_pct)
        )
        missing = str(snapshot.missing_block_count) if snapshot.missing_block_count else ""
        table.add_row(str(snapshot.spec.session_id), snapshot.state.name, progress, loss, missing)

    return table


def _receivers_table(snapshots: Sequence[SessionSnapshot]) -> Table:
    live, counters = _merged_receivers(snapshots)
    # Not expand=True: stretching eight columns to fill a wide console just
    # wastes space, and on a narrow one (an 80-column docker compose logs
    # pane) it squeezes the two longest headers below their own length,
    # which Rich truncates to unreadable mid-word fragments -- shortened
    # headers here trade a little precision for staying legible at 80 cols.
    table = Table(title="Receivers")
    table.add_column("Receiver")
    table.add_column("Status")
    table.add_column("pkts_ok", justify="right")
    table.add_column("crc_fail", justify="right")
    table.add_column("dup", justify="right")
    table.add_column("kdrops", justify="right")
    table.add_column("aexh", justify="right")
    table.add_column("hwm%", justify="right")

    receiver_ids = sorted(set(counters) | live)
    if not receiver_ids:
        table.add_row("(no receivers)", "", "", "", "", "", "", "")
        return table

    for receiver_id in receiver_ids:
        receiver_counters = counters.get(receiver_id, ReceiverCounters())
        status = (
            Text("alive", style="green") if receiver_id in live else Text("dead", style="bold red")
        )
        table.add_row(
            str(receiver_id),
            status,
            str(receiver_counters.pkts_ok),
            _prominent_if_nonzero(receiver_counters.crc_fail),
            str(receiver_counters.duplicates),
            _prominent_if_nonzero(receiver_counters.kernel_drops),
            _prominent_if_nonzero(receiver_counters.arena_exhausted),
            str(receiver_counters.arena_high_water_pct),
        )

    return table


def render(snapshots: Sequence[SessionSnapshot]) -> RenderableType:
    """Pure: snapshots in, a `rich` renderable out. No clock, no state, no I/O."""
    return Group(_sessions_table(snapshots), _receivers_table(snapshots))


def _session_fields(snapshot: SessionSnapshot) -> dict[str, Any]:
    return {
        "session_id": str(snapshot.spec.session_id),
        "state": snapshot.state.name,
        "blocks_decoded": snapshot.blocks_decoded,
        "total_blocks": snapshot.total_blocks,
        "observed_loss_pct": round(snapshot.observed_loss_pct, _LOSS_PCT_DIGITS),
        "missing_block_count": snapshot.missing_block_count,
    }


def _receiver_fields(
    receiver_id: ReceiverId, counters: ReceiverCounters, alive: bool
) -> dict[str, Any]:
    return {
        "receiver_id": int(receiver_id),
        "status": "alive" if alive else "dead",
        "pkts_ok": counters.pkts_ok,
        "crc_fail": counters.crc_fail,
        "duplicates": counters.duplicates,
        "kernel_drops": counters.kernel_drops,
        "arena_exhausted": counters.arena_exhausted,
        "arena_high_water_pct": counters.arena_high_water_pct,
    }


def log_status(snapshots: Sequence[SessionSnapshot]) -> None:
    """The no-TTY equivalent of `render`: one `status` event, same fields."""
    live, counters = _merged_receivers(snapshots)
    receivers = [
        _receiver_fields(
            receiver_id, counters.get(receiver_id, ReceiverCounters()), receiver_id in live
        )
        for receiver_id in sorted(set(counters) | live)
    ]
    logger.info(
        "status",
        sessions=[_session_fields(snapshot) for snapshot in snapshots],
        receivers=receivers,
    )


class StatusDisplay:
    """Thin driver: `render`/`log_status` own the content, this owns the cadence.

    On a TTY it drives `rich.Live` at `refresh_interval_s`. With no TTY --
    `docker compose logs`, a pipe, a test -- `Live` would repaint the whole
    tables into the captured stream every refresh and bury every other line,
    so it emits one structured `status` line every `log_interval_s` instead.
    Both paths only ever see the immutable snapshots they are handed, which
    is what keeps `render` and `log_status` unit-testable with no terminal.
    """

    def __init__(
        self,
        console: Console,
        clock: Clock,
        refresh_interval_s: float,
        snapshots_provider: SnapshotsProvider,
        log_interval_s: float = STATUS_LOG_INTERVAL_SECONDS,
    ) -> None:
        self._console: Console = console
        self._clock: Clock = clock
        self._refresh_interval_s: float = refresh_interval_s
        self._snapshots_provider: SnapshotsProvider = snapshots_provider
        self._log_interval_s: float = log_interval_s

    async def run(self) -> None:
        if self._console.is_terminal:
            await self._run_live()
        else:
            await self._run_logged()

    async def _run_live(self) -> None:
        with Live(render(self._snapshots_provider()), console=self._console, screen=False) as live:
            while True:
                await self._clock.sleep(self._refresh_interval_s)
                live.update(render(self._snapshots_provider()))

    async def _run_logged(self) -> None:
        while True:
            log_status(self._snapshots_provider())
            await self._clock.sleep(self._log_interval_s)
