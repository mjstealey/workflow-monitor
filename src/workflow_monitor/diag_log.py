"""Append-only JSONL writer for diagnostic events.

Writes a sidecar file (``diagnostics-events.jsonl``) alongside the main
workflow event log. Diagnostic events are interpretive (stall detection,
hold/failure remediation) and intentionally kept separate from the
authoritative ``workflow-events.jsonl`` replay stream so the diagnostic
schema can evolve independently.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional


DIAG_SCHEMA_VERSION = 1


class DiagnosticLogger:
    """Append-only JSONL writer for diagnostic events."""

    def __init__(
        self,
        wf_uuid: str,
        log_path: Path,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._wf_uuid = wf_uuid
        self._path = log_path
        self._fh = open(self._path, "a")
        self._emit_raw({
            "event_type": "diag_start",
            "timestamp": time.time(),
            "wf_uuid": self._wf_uuid,
            "schema_version": DIAG_SCHEMA_VERSION,
            "engine_config": engine_config or {},
        })

    def emit(self, event: Dict[str, Any]) -> None:
        """Write a diagnostic event. Adds wf_uuid and timestamp if missing."""
        event.setdefault("wf_uuid", self._wf_uuid)
        event.setdefault("timestamp", time.time())
        self._emit_raw(event)

    def _emit_raw(self, event: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(event, default=str) + "\n")
        self._fh.flush()

    def close(self) -> None:
        try:
            self._emit_raw({
                "event_type": "diag_end",
                "timestamp": time.time(),
                "wf_uuid": self._wf_uuid,
            })
        finally:
            self._fh.close()

    @property
    def path(self) -> Path:
        return self._path

    @staticmethod
    def path_from_event_log(event_log_path: Path) -> Path:
        """Derive diagnostics path from the main event log path."""
        return event_log_path.parent / "diagnostics-events.jsonl"
