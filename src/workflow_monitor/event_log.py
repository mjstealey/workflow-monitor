"""JSONL event logger for workflow replay."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .braindump import WorkflowInfo
from .db import StampedeDB, WorkflowSnapshot


class EventLogger:
    """Writes workflow events to a JSONL file for later replay.

    Each line is a self-contained JSON object with at minimum:
        {"event_type": "...", "timestamp": <unix_float>, "wf_uuid": "..."}
    """

    def __init__(
        self,
        info: WorkflowInfo,
        db: StampedeDB,
        log_path: Optional[Path] = None,
    ) -> None:
        self._info = info
        self._db = db
        self._wf_uuid = info.wf_uuid

        if log_path is None:
            log_path = info.submit_dir / "workflow-events.jsonl"
        self._path = log_path

        self._high_water_ts: float = 0.0
        self._last_wf_state: Optional[str] = None
        self._last_condor_ids: set[str] = set()
        self._jobs_init_emitted: bool = False

        self._fh = open(self._path, "a")
        self._write_header()

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _emit(self, event: Dict[str, Any]) -> None:
        event.setdefault("wf_uuid", self._wf_uuid)
        self._fh.write(json.dumps(event, default=str) + "\n")
        self._fh.flush()

    def _write_header(self) -> None:
        wf_times = self._db.get_workflow_times()
        self._emit({
            "event_type": "workflow_start",
            "timestamp": time.time(),
            "dax_label": self._info.dax_label,
            "user": self._info.user,
            "planner_version": self._info.planner_version,
            "submit_dir": str(self._info.submit_dir),
            "wf_start": wf_times.get("start"),
        })

    # ── Public API ───────────────────────────────────────────────────────────

    def record(
        self,
        snapshot: WorkflowSnapshot,
        condor_jobs: Optional[List[Dict]] = None,
    ) -> None:
        """Record new events from the latest poll cycle."""
        if not self._jobs_init_emitted:
            self._emit_jobs_init(snapshot)
            self._jobs_init_emitted = True
        self._record_workflow_state(snapshot)
        self._record_job_events()
        self._record_condor_poll(condor_jobs)

    def close(self, snapshot: Optional[WorkflowSnapshot] = None) -> None:
        """Write a workflow_end event and close the file."""
        end_event: Dict[str, Any] = {
            "event_type": "workflow_end",
            "timestamp": time.time(),
        }
        if snapshot is not None:
            end_event["wf_state"] = snapshot.wf_state
            end_event["wf_status"] = snapshot.wf_status
            end_event["total_jobs"] = snapshot.total_jobs()
            end_event["done"] = snapshot.done_count()
            end_event["failed"] = snapshot.failed_count()
            end_event["elapsed"] = snapshot.elapsed
        self._emit(end_event)
        self._fh.close()

    @property
    def path(self) -> Path:
        return self._path

    # ── Event detection ──────────────────────────────────────────────────────

    def _emit_jobs_init(self, snapshot: WorkflowSnapshot) -> None:
        """Emit a jobs_init event listing the full job roster from the first snapshot."""
        jobs = []
        for j in snapshot.jobs:
            jobs.append({
                "job_id": j.job_id,
                "exec_job_id": j.exec_job_id,
                "type_desc": j.type_desc,
            })
        self._emit({
            "event_type": "jobs_init",
            "timestamp": time.time(),
            "total_jobs": len(jobs),
            "jobs": jobs,
        })

    def _record_workflow_state(self, snapshot: WorkflowSnapshot) -> None:
        if snapshot.wf_state != self._last_wf_state:
            self._emit({
                "event_type": "workflow_state",
                "timestamp": time.time(),
                "state": snapshot.wf_state,
                "status": snapshot.wf_status,
            })
            self._last_wf_state = snapshot.wf_state

    def _record_job_events(self) -> None:
        events = self._db.get_events_since(self._high_water_ts)
        for ev in events:
            self._emit({
                "event_type": "job_state",
                "timestamp": ev["timestamp"],
                "exec_job_id": ev["exec_job_id"],
                "type_desc": ev["type_desc"],
                "state": ev["state"],
                "job_id": ev["job_id"],
            })
            if ev["timestamp"] > self._high_water_ts:
                self._high_water_ts = ev["timestamp"]

    def _record_condor_poll(self, condor_jobs: Optional[List[Dict]]) -> None:
        if condor_jobs is None:
            return
        current_ids = {
            cj.get("ClusterId", cj.get("DAGNodeName", ""))
            for cj in condor_jobs
        }
        if current_ids != self._last_condor_ids:
            self._emit({
                "event_type": "htcondor_poll",
                "timestamp": time.time(),
                "jobs": condor_jobs,
            })
            self._last_condor_ids = current_ids
