import os
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from session_manager.constants import (
    AGGREGATION_SECTION,
    ARENA_BYTES_ENV_VAR,
    ARENA_BYTES_KEY,
    DEFAULT_ARENA_BYTES,
    DEFAULT_FORCE_TERMINAL,
    DEFAULT_JOURNAL_DIR,
    DEFAULT_LOCK_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_POLL_INTERVAL_S,
    DEFAULT_RECEIVER_BINARY_PATH,
    DEFAULT_RECEIVER_COUNT,
    DEFAULT_RECEIVER_PORTS,
    DEFAULT_REFRESH_INTERVAL_S,
    DEFAULT_RUN_DIR,
    DEFAULT_SHM_CROSSCHECK,
    DEFAULT_SHM_NAME,
    DEFAULT_SLOT_BYTES,
    DEFAULT_SOCKET_PATH,
    DEFAULT_STAGING_DIR,
    DEFAULT_STALL_TIMEOUT_S,
    FORCE_TERMINAL_ENV_VAR,
    FORCE_TERMINAL_KEY,
    JOURNAL_DIR_ENV_VAR,
    JOURNAL_DIR_KEY,
    LOCK_PATH_ENV_VAR,
    LOCK_PATH_KEY,
    MAX_PORT,
    MIN_ARENA_SLOTS,
    MIN_PORT,
    OUTPUT_DIR_ENV_VAR,
    OUTPUT_DIR_KEY,
    PATHS_SECTION,
    POLL_INTERVAL_S_ENV_VAR,
    POLL_INTERVAL_S_KEY,
    RECEIVER_BINARY_PATH_ENV_VAR,
    RECEIVER_BINARY_PATH_KEY,
    RECEIVER_COUNT_ENV_VAR,
    RECEIVER_COUNT_KEY,
    RECEIVER_PORTS_ENV_SEPARATOR,
    RECEIVER_PORTS_ENV_VAR,
    RECEIVER_PORTS_KEY,
    RECEIVERS_SECTION,
    REFRESH_INTERVAL_S_ENV_VAR,
    REFRESH_INTERVAL_S_KEY,
    RUN_DIR_ENV_VAR,
    RUN_DIR_KEY,
    SHM_CROSSCHECK_ENV_VAR,
    SHM_CROSSCHECK_KEY,
    SHM_NAME_ENV_VAR,
    SHM_NAME_KEY,
    SHM_SECTION,
    SLOT_BYTES_ENV_VAR,
    SLOT_BYTES_KEY,
    SOCKET_PATH_ENV_VAR,
    SOCKET_PATH_KEY,
    STAGING_DIR_ENV_VAR,
    STAGING_DIR_KEY,
    STALL_TIMEOUT_S_ENV_VAR,
    STALL_TIMEOUT_S_KEY,
    STATUS_SECTION,
)
from session_manager.supervision.constants import (
    CRASH_LOOP_MAX_RESTARTS,
    CRASH_LOOP_WINDOW_SECONDS,
    DEFAULT_BACKOFF_SCHEDULE_SECONDS,
    SHUTDOWN_TIMEOUT_SECONDS,
)


def _parse_bool(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"cannot parse a boolean from {raw!r}")


def _parse_port_list(raw: str) -> list[int]:
    return [int(part) for part in raw.split(RECEIVER_PORTS_ENV_SEPARATOR) if part.strip()]


ENV_OVERRIDES: tuple[tuple[str, str, str, Callable[[str], Any]], ...] = (
    (STAGING_DIR_ENV_VAR, PATHS_SECTION, STAGING_DIR_KEY, str),
    (OUTPUT_DIR_ENV_VAR, PATHS_SECTION, OUTPUT_DIR_KEY, str),
    (JOURNAL_DIR_ENV_VAR, PATHS_SECTION, JOURNAL_DIR_KEY, str),
    (RUN_DIR_ENV_VAR, PATHS_SECTION, RUN_DIR_KEY, str),
    (SOCKET_PATH_ENV_VAR, PATHS_SECTION, SOCKET_PATH_KEY, str),
    (LOCK_PATH_ENV_VAR, PATHS_SECTION, LOCK_PATH_KEY, str),
    (SHM_NAME_ENV_VAR, SHM_SECTION, SHM_NAME_KEY, str),
    (ARENA_BYTES_ENV_VAR, SHM_SECTION, ARENA_BYTES_KEY, int),
    (SLOT_BYTES_ENV_VAR, SHM_SECTION, SLOT_BYTES_KEY, int),
    (POLL_INTERVAL_S_ENV_VAR, AGGREGATION_SECTION, POLL_INTERVAL_S_KEY, float),
    (STALL_TIMEOUT_S_ENV_VAR, AGGREGATION_SECTION, STALL_TIMEOUT_S_KEY, float),
    (SHM_CROSSCHECK_ENV_VAR, AGGREGATION_SECTION, SHM_CROSSCHECK_KEY, _parse_bool),
    (RECEIVER_COUNT_ENV_VAR, RECEIVERS_SECTION, RECEIVER_COUNT_KEY, int),
    (RECEIVER_PORTS_ENV_VAR, RECEIVERS_SECTION, RECEIVER_PORTS_KEY, _parse_port_list),
    (RECEIVER_BINARY_PATH_ENV_VAR, RECEIVERS_SECTION, RECEIVER_BINARY_PATH_KEY, str),
    (REFRESH_INTERVAL_S_ENV_VAR, STATUS_SECTION, REFRESH_INTERVAL_S_KEY, float),
    (FORCE_TERMINAL_ENV_VAR, STATUS_SECTION, FORCE_TERMINAL_KEY, _parse_bool),
)


@dataclass(frozen=True)
class PathsConfig:
    staging_dir: Path
    output_dir: Path
    journal_dir: Path
    run_dir: Path
    socket_path: Path
    lock_path: Path


@dataclass(frozen=True)
class ShmConfig:
    name: str
    arena_bytes: int
    slot_bytes: int


@dataclass(frozen=True)
class AggregationConfig:
    poll_interval_s: float
    stall_timeout_s: float
    shm_crosscheck: bool


@dataclass(frozen=True)
class ReceiversConfig:
    count: int
    ports: tuple[int, ...]
    binary_path: str


@dataclass(frozen=True)
class SupervisionConfig:
    backoff_schedule_s: tuple[float, ...] = DEFAULT_BACKOFF_SCHEDULE_SECONDS
    crash_loop_max_restarts: int = CRASH_LOOP_MAX_RESTARTS
    crash_loop_window_s: float = CRASH_LOOP_WINDOW_SECONDS
    shutdown_timeout_s: float = SHUTDOWN_TIMEOUT_SECONDS


@dataclass(frozen=True)
class StatusConfig:
    refresh_interval_s: float
    force_terminal: bool


@dataclass(frozen=True)
class AppConfig:
    paths: PathsConfig
    shm: ShmConfig
    aggregation: AggregationConfig
    receivers: ReceiversConfig
    supervision: SupervisionConfig
    status: StatusConfig


def _resolve_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else base_dir / path


def _resolve_optional_path(base_dir: Path, value: str) -> str:
    return str(_resolve_path(base_dir, value)) if value else ""


def _device_of(path: Path) -> int:
    return path.stat().st_dev


def _ensure_directory(path: Path, config_key: str) -> None:
    if path.is_dir():
        return
    if path.exists():
        raise ValueError(f"{config_key} exists and is not a directory: {path}")
    path.mkdir(parents=True, exist_ok=True)


def load_config(config_path: Path) -> AppConfig:
    with open(config_path, "rb") as file_handle:
        data = tomllib.load(file_handle)

    config_dir = config_path.resolve().parent
    environment = os.environ

    section_data_by_name: dict[str, dict[str, Any]] = {
        PATHS_SECTION: dict(data.get(PATHS_SECTION, {})),
        SHM_SECTION: dict(data.get(SHM_SECTION, {})),
        AGGREGATION_SECTION: dict(data.get(AGGREGATION_SECTION, {})),
        RECEIVERS_SECTION: dict(data.get(RECEIVERS_SECTION, {})),
        STATUS_SECTION: dict(data.get(STATUS_SECTION, {})),
    }

    for env_var_name, section, key, cast_value in ENV_OVERRIDES:
        if env_var_name in environment:
            section_data_by_name[section][key] = cast_value(environment[env_var_name])

    paths_data = section_data_by_name[PATHS_SECTION]
    shm_data = section_data_by_name[SHM_SECTION]
    aggregation_data = section_data_by_name[AGGREGATION_SECTION]
    receivers_data = section_data_by_name[RECEIVERS_SECTION]
    status_data = section_data_by_name[STATUS_SECTION]

    app_config = AppConfig(
        paths=PathsConfig(
            staging_dir=_resolve_path(
                config_dir, str(paths_data.get(STAGING_DIR_KEY, DEFAULT_STAGING_DIR))
            ),
            output_dir=_resolve_path(
                config_dir, str(paths_data.get(OUTPUT_DIR_KEY, DEFAULT_OUTPUT_DIR))
            ),
            journal_dir=_resolve_path(
                config_dir, str(paths_data.get(JOURNAL_DIR_KEY, DEFAULT_JOURNAL_DIR))
            ),
            run_dir=_resolve_path(config_dir, str(paths_data.get(RUN_DIR_KEY, DEFAULT_RUN_DIR))),
            socket_path=_resolve_path(
                config_dir, str(paths_data.get(SOCKET_PATH_KEY, DEFAULT_SOCKET_PATH))
            ),
            lock_path=_resolve_path(
                config_dir, str(paths_data.get(LOCK_PATH_KEY, DEFAULT_LOCK_PATH))
            ),
        ),
        shm=ShmConfig(
            name=str(shm_data.get(SHM_NAME_KEY, DEFAULT_SHM_NAME)),
            arena_bytes=int(shm_data.get(ARENA_BYTES_KEY, DEFAULT_ARENA_BYTES)),
            slot_bytes=int(shm_data.get(SLOT_BYTES_KEY, DEFAULT_SLOT_BYTES)),
        ),
        aggregation=AggregationConfig(
            poll_interval_s=float(
                aggregation_data.get(POLL_INTERVAL_S_KEY, DEFAULT_POLL_INTERVAL_S)
            ),
            stall_timeout_s=float(
                aggregation_data.get(STALL_TIMEOUT_S_KEY, DEFAULT_STALL_TIMEOUT_S)
            ),
            shm_crosscheck=bool(aggregation_data.get(SHM_CROSSCHECK_KEY, DEFAULT_SHM_CROSSCHECK)),
        ),
        receivers=ReceiversConfig(
            count=int(receivers_data.get(RECEIVER_COUNT_KEY, DEFAULT_RECEIVER_COUNT)),
            ports=tuple(
                int(port) for port in receivers_data.get(RECEIVER_PORTS_KEY, DEFAULT_RECEIVER_PORTS)
            ),
            binary_path=_resolve_optional_path(
                config_dir,
                str(receivers_data.get(RECEIVER_BINARY_PATH_KEY, DEFAULT_RECEIVER_BINARY_PATH)),
            ),
        ),
        supervision=SupervisionConfig(),
        status=StatusConfig(
            refresh_interval_s=float(
                status_data.get(REFRESH_INTERVAL_S_KEY, DEFAULT_REFRESH_INTERVAL_S)
            ),
            force_terminal=bool(status_data.get(FORCE_TERMINAL_KEY, DEFAULT_FORCE_TERMINAL)),
        ),
    )

    validate_config(app_config)
    return app_config


def validate_config(app_config: AppConfig) -> None:
    paths = app_config.paths
    _ensure_directory(paths.staging_dir, "paths.staging_dir")
    _ensure_directory(paths.output_dir, "paths.output_dir")
    _ensure_directory(paths.journal_dir, "paths.journal_dir")
    _ensure_directory(paths.run_dir, "paths.run_dir")
    _ensure_directory(paths.socket_path.parent, "paths.socket_path parent")
    _ensure_directory(paths.lock_path.parent, "paths.lock_path parent")

    if _device_of(paths.staging_dir) != _device_of(paths.output_dir):
        raise ValueError(
            "paths.staging_dir and paths.output_dir must be on the same filesystem, since "
            f"publish is an atomic rename: {paths.staging_dir} vs {paths.output_dir}"
        )

    shm = app_config.shm
    if shm.slot_bytes <= 0:
        raise ValueError(f"shm.slot_bytes must be > 0, got {shm.slot_bytes}")
    if shm.arena_bytes <= 0:
        raise ValueError(f"shm.arena_bytes must be > 0, got {shm.arena_bytes}")
    if shm.arena_bytes % shm.slot_bytes != 0:
        raise ValueError(
            f"shm.arena_bytes ({shm.arena_bytes}) must be a multiple of "
            f"shm.slot_bytes ({shm.slot_bytes})"
        )
    if shm.arena_bytes < shm.slot_bytes * MIN_ARENA_SLOTS:
        raise ValueError(
            f"shm.arena_bytes ({shm.arena_bytes}) must be at least "
            f"shm.slot_bytes * {MIN_ARENA_SLOTS} ({shm.slot_bytes * MIN_ARENA_SLOTS})"
        )

    receivers = app_config.receivers
    if receivers.count < 0:
        raise ValueError(f"receivers.count must be >= 0, got {receivers.count}")
    if len(receivers.ports) < receivers.count:
        raise ValueError(
            f"receivers.ports has {len(receivers.ports)} entries, "
            f"fewer than receivers.count ({receivers.count})"
        )
    if len(set(receivers.ports)) != len(receivers.ports):
        raise ValueError(f"receivers.ports contains duplicates: {sorted(receivers.ports)}")
    for port in receivers.ports:
        if not (MIN_PORT <= port <= MAX_PORT):
            raise ValueError(f"receivers.ports entry {port} is outside [{MIN_PORT}, {MAX_PORT}]")
    if receivers.count > 0 and not receivers.binary_path:
        raise ValueError("receivers.binary_path must not be empty when receivers.count > 0")

    aggregation = app_config.aggregation
    if aggregation.poll_interval_s <= 0:
        raise ValueError(
            f"aggregation.poll_interval_s must be > 0, got {aggregation.poll_interval_s}"
        )
    if aggregation.stall_timeout_s <= 0:
        raise ValueError(
            f"aggregation.stall_timeout_s must be > 0, got {aggregation.stall_timeout_s}"
        )
    if aggregation.poll_interval_s >= aggregation.stall_timeout_s:
        raise ValueError(
            f"aggregation.poll_interval_s ({aggregation.poll_interval_s}) must be < "
            f"aggregation.stall_timeout_s ({aggregation.stall_timeout_s}) or a stall can "
            "never be observed"
        )

    if app_config.status.refresh_interval_s <= 0:
        raise ValueError(
            f"status.refresh_interval_s must be > 0, got {app_config.status.refresh_interval_s}"
        )
