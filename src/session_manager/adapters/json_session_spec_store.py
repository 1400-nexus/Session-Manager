import json
from pathlib import Path
from typing import Any

import structlog

from session_manager.adapters.atomic_write import write_json_atomically
from session_manager.adapters.constants import SESSION_SPEC_FILENAME_SUFFIX
from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec
from session_manager.domain.paths import is_unsafe_filename_component

logger = structlog.get_logger(__name__)


def _to_json(spec: SessionSpec) -> dict[str, object]:
    return {
        "session_id": str(spec.session_id),
        "relpath": spec.relpath,
        "file_size": spec.file_size,
        "file_hash": spec.file_hash.hex(),
        "k": spec.k,
        "n": spec.n,
        "symbol_bytes": spec.symbol_bytes,
        "total_blocks": spec.total_blocks,
    }


def _from_json(data: dict[str, Any]) -> SessionSpec:
    return SessionSpec(
        session_id=SessionId(data["session_id"]),
        relpath=data["relpath"],
        file_size=data["file_size"],
        file_hash=bytes.fromhex(data["file_hash"]),
        k=data["k"],
        n=data["n"],
        symbol_bytes=data["symbol_bytes"],
        total_blocks=data["total_blocks"],
    )


class JsonSessionSpecStore:
    """`SessionSpec` sidecar, one JSON file per session next to its journal.

    Persisting the full spec here -- rather than widening the on-segment shm
    session table -- keeps that table a cross-language contract only the C++
    receivers need to parse; this file is Python-only and can change shape
    without coordinating with them.
    """

    def __init__(self, spec_dir: Path) -> None:
        self._spec_dir: Path = spec_dir

    def save(self, spec: SessionSpec) -> None:
        # Atomic: milestone 4's whole recovery path reads this file, and it
        # simulates a kill -9 -- a bare write interrupted mid-flush would
        # leave a truncated sidecar and an unrecoverable session.
        write_json_atomically(self._path_for(spec.session_id), _to_json(spec))

    def load(self, session_id: SessionId) -> SessionSpec | None:
        path = self._path_for(session_id)
        if not path.is_file():
            return None
        try:
            return _from_json(json.loads(path.read_text()))
        except (json.JSONDecodeError, KeyError, ValueError) as error:
            logger.error(
                "session_spec_corrupt", session_id=session_id, path=str(path), error=str(error)
            )
            return None

    def delete(self, session_id: SessionId) -> None:
        self._path_for(session_id).unlink(missing_ok=True)

    def _path_for(self, session_id: SessionId) -> Path:
        # Same hazard as AppendJournal._path_for: session_id becomes a
        # filename here, so it gets the same shared check.
        if is_unsafe_filename_component(session_id):
            raise ValueError(f"unsafe session_id for spec filename: {session_id!r}")
        return self._spec_dir / f"{session_id}{SESSION_SPEC_FILENAME_SUFFIX}"
