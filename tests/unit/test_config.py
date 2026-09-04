import dataclasses
from pathlib import Path

import pytest

from session_manager import config


def _toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f'"{value}"'
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    return str(value)


def _valid_sections() -> dict[str, dict[str, object]]:
    return {
        "paths": {
            "staging_dir": "staging",
            "output_dir": "output",
            "journal_dir": "journal",
            "run_dir": "run",
            "socket_path": "run/session-manager.sock",
            "lock_path": "run/session-manager.lock",
        },
        "shm": {"name": "t", "arena_bytes": 40, "slot_bytes": 10},
        "aggregation": {
            "poll_interval_s": 1.0,
            "stall_timeout_s": 8.0,
            "shm_crosscheck": True,
        },
        "receivers": {"count": 0, "ports": [9100, 9101, 9102], "binary_path": "bin/rx"},
        "status": {"refresh_interval_s": 0.5, "force_terminal": False},
    }


def _write_config(directory: Path, sections: dict[str, dict[str, object]]) -> Path:
    lines: list[str] = []
    for section, values in sections.items():
        lines.append(f"[{section}]")
        for key, value in values.items():
            lines.append(f"{key} = {_toml_value(value)}")
        lines.append("")
    path = directory / "config.toml"
    path.write_text("\n".join(lines))
    return path


def test_valid_config_round_trips(tmp_path: Path) -> None:
    app_config = config.load_config(_write_config(tmp_path, _valid_sections()))
    assert app_config.paths.staging_dir == tmp_path / "staging"
    assert app_config.shm.slot_bytes == 10
    assert app_config.receivers.ports == (9100, 9101, 9102)
    assert app_config.aggregation.shm_crosscheck is True
    assert app_config.supervision.crash_loop_max_restarts > 0


def test_relative_paths_resolve_against_config_dir_not_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "etc" / "nexus"
    config_dir.mkdir(parents=True)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    app_config = config.load_config(_write_config(config_dir, _valid_sections()))

    assert app_config.paths.staging_dir == config_dir / "staging"
    assert app_config.paths.socket_path == config_dir / "run" / "session-manager.sock"


def test_env_override_replaces_the_toml_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NEXUS_SHM_SLOT_BYTES", "20")
    monkeypatch.setenv("NEXUS_SHM_ARENA_BYTES", "80")
    monkeypatch.setenv("NEXUS_RECEIVERS_PORTS", "7000,7001")
    monkeypatch.setenv("NEXUS_AGGREGATION_SHM_CROSSCHECK", "off")

    app_config = config.load_config(_write_config(tmp_path, _valid_sections()))

    assert app_config.shm.slot_bytes == 20
    assert app_config.receivers.ports == (7000, 7001)
    assert app_config.aggregation.shm_crosscheck is False


def test_same_filesystem_passes_the_device_check(tmp_path: Path) -> None:
    app_config = config.load_config(_write_config(tmp_path, _valid_sections()))
    assert app_config.paths.staging_dir.stat().st_dev == app_config.paths.output_dir.stat().st_dev


def test_staging_and_output_on_different_filesystems_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_device_of(path: Path) -> int:
        return 1 if path.name == "staging" else 2

    monkeypatch.setattr(config, "_device_of", fake_device_of)

    with pytest.raises(ValueError, match="same filesystem"):
        config.load_config(_write_config(tmp_path, _valid_sections()))


def test_a_path_that_exists_as_a_file_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "staging").write_text("i am a file, not a directory")

    with pytest.raises(ValueError, match="paths.staging_dir.*not a directory"):
        config.load_config(_write_config(tmp_path, _valid_sections()))


def test_arena_bytes_must_be_a_multiple_of_slot_bytes(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["shm"]["arena_bytes"] = 45

    with pytest.raises(ValueError, match="multiple of"):
        config.load_config(_write_config(tmp_path, sections))


def test_arena_bytes_must_hold_the_minimum_slot_count(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["shm"]["arena_bytes"] = 30

    with pytest.raises(ValueError, match=r"shm.arena_bytes.*at least"):
        config.load_config(_write_config(tmp_path, sections))


def test_slot_bytes_must_be_positive(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["shm"]["slot_bytes"] = 0

    with pytest.raises(ValueError, match="shm.slot_bytes"):
        config.load_config(_write_config(tmp_path, sections))


def test_receiver_count_may_not_be_negative(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["receivers"]["count"] = -1

    with pytest.raises(ValueError, match="receivers.count"):
        config.load_config(_write_config(tmp_path, sections))


def test_ports_must_cover_the_receiver_count(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["receivers"]["count"] = 3
    sections["receivers"]["ports"] = [9100, 9101]

    with pytest.raises(ValueError, match="receivers.ports"):
        config.load_config(_write_config(tmp_path, sections))


def test_duplicate_ports_are_rejected(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["receivers"]["ports"] = [9100, 9100, 9101]

    with pytest.raises(ValueError, match="duplicate"):
        config.load_config(_write_config(tmp_path, sections))


def test_a_port_outside_the_valid_range_is_rejected(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["receivers"]["ports"] = [9100, 70000, 9101]

    with pytest.raises(ValueError, match="outside"):
        config.load_config(_write_config(tmp_path, sections))


def test_binary_path_is_required_when_supervising_receivers(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["receivers"]["count"] = 2
    sections["receivers"]["binary_path"] = ""

    with pytest.raises(ValueError, match="receivers.binary_path"):
        config.load_config(_write_config(tmp_path, sections))


def test_poll_interval_must_be_shorter_than_the_stall_timeout(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["aggregation"]["poll_interval_s"] = 8.0
    sections["aggregation"]["stall_timeout_s"] = 8.0

    with pytest.raises(ValueError, match="poll_interval_s"):
        config.load_config(_write_config(tmp_path, sections))


def test_status_refresh_interval_must_be_positive(tmp_path: Path) -> None:
    sections = _valid_sections()
    sections["status"]["refresh_interval_s"] = 0.0

    with pytest.raises(ValueError, match="status.refresh_interval_s"):
        config.load_config(_write_config(tmp_path, sections))


def test_fec_parameters_are_not_configurable_on_the_rx_side() -> None:
    field_names = {field.name for field in dataclasses.fields(config.AppConfig)}
    assert "fec" not in field_names
    assert not any("symbol" in name or name in {"k", "n"} for name in field_names)
