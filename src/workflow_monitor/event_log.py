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

    If an existing log file is detected for the same workflow, the logger
    resumes from where it left off — no duplicate header, no re-emitted
    events — so stop/restart of the monitor produces a single continuous
    series.
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
        self._last_condor_fingerprint: frozenset[tuple] = frozenset()
        self._jobs_init_emitted: bool = False
        self._resumed: bool = False

        # Try to resume from an existing log before opening for append
        self._try_resume()

        self._fh = open(self._path, "a")

        if not self._resumed:
            self._write_header()

    # ── Resume from existing log ──────────────────────────────────────────────

    def _try_resume(self) -> None:
        """Scan an existing log file to recover state for seamless continuation.

        If the log ends with a ``workflow_end`` event (from a previous server
        session), the file is truncated to remove it so that new events
        append cleanly and clients never see a premature end marker.
        """
        if not self._path.exists() or self._path.stat().st_size == 0:
            return

        header_uuid: Optional[str] = None
        last_end_offset: Optional[int] = 0  # byte offset of last workflow_end line

        try:
            with open(self._path) as fh:
                offset = 0
                for line in fh:
                    raw = line.strip()
                    if not raw:
                        offset = fh.tell()
                        continue
                    ev = json.loads(raw)
                    etype = ev.get("event_type")

                    if etype == "workflow_start":
                        header_uuid = ev.get("wf_uuid")

                    elif etype == "workflow_state":
                        self._last_wf_state = ev.get("state")

                    elif etype == "job_state":
                        ts = ev.get("timestamp", 0)
                        if ts > self._high_water_ts:
                            self._high_water_ts = ts

                    elif etype == "jobs_init":
                        self._jobs_init_emitted = True

                    elif etype == "htcondor_poll":
                        jobs = ev.get("jobs", [])
                        self._last_condor_fingerprint = self._condor_fingerprint(jobs)

                    elif etype == "workflow_end":
                        last_end_offset = offset

                    offset = fh.tell()
        except (json.JSONDecodeError, OSError):
            # Corrupted or unreadable — start fresh
            return

        # Only resume if the existing log belongs to the same workflow
        if header_uuid is not None and header_uuid == self._wf_uuid:
            self._resumed = True
            # Truncate any trailing workflow_end so new events append cleanly
            if last_end_offset is not None and last_end_offset > 0:
                with open(self._path, "r+") as fh:
                    fh.seek(last_end_offset)
                    rest = fh.read().strip()
                    # Only truncate if workflow_end is the last event
                    try:
                        ev = json.loads(rest)
                        if ev.get("event_type") == "workflow_end":
                            fh.seek(last_end_offset)
                            fh.truncate()
                    except (json.JSONDecodeError, ValueError):
                        pass

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
            end_event["wf_end"] = snapshot.wf_end
            end_event["total_jobs"] = snapshot.total_jobs()
            end_event["done"] = snapshot.done_count()
            end_event["failed"] = snapshot.failed_count()
            end_event["elapsed"] = snapshot.elapsed
        self._emit(end_event)
        self._fh.close()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def resumed(self) -> bool:
        return self._resumed

    # ── Event detection ──────────────────────────────────────────────────────

    def _emit_jobs_init(self, snapshot: WorkflowSnapshot) -> None:
        """Emit a jobs_init event listing the full job roster from the first snapshot."""
        jobs = []
        for j in snapshot.jobs:
            entry: dict = {
                "job_id": j.job_id,
                "exec_job_id": j.exec_job_id,
                "type_desc": j.type_desc,
            }
            if j.transformation:
                entry["transformation"] = j.transformation
            if j.task_argv:
                entry["task_argv"] = j.task_argv
            jobs.append(entry)
        self._emit({
            "event_type": "jobs_init",
            "timestamp": time.time(),
            "total_jobs": len(jobs),
            "jobs": jobs,
        })

    def _record_workflow_state(self, snapshot: WorkflowSnapshot) -> None:
        if snapshot.wf_state != self._last_wf_state:
            ev: Dict[str, Any] = {
                "event_type": "workflow_state",
                "timestamp": time.time(),
                "state": snapshot.wf_state,
                "status": snapshot.wf_status,
            }
            # Include actual DB timestamps so remote clients can compute
            # accurate elapsed time (time.time() is logger wall clock).
            if snapshot.wf_state == "WORKFLOW_STARTED" and snapshot.wf_start:
                ev["wf_start"] = snapshot.wf_start
            elif snapshot.wf_state == "WORKFLOW_TERMINATED" and snapshot.wf_end:
                ev["wf_end"] = snapshot.wf_end
            self._emit(ev)
            self._last_wf_state = snapshot.wf_state

    def _record_job_events(self) -> None:
        events = self._db.get_events_since(self._high_water_ts)
        for ev in events:
            record: Dict[str, Any] = {
                "event_type": "job_state",
                "timestamp": ev["timestamp"],
                "exec_job_id": ev["exec_job_id"],
                "type_desc": ev["type_desc"],
                "state": ev["state"],
                "job_id": ev["job_id"],
            }
            if ev.get("exitcode") is not None:
                record["exitcode"] = ev["exitcode"]
            if ev.get("stdout_file"):
                record["stdout_file"] = ev["stdout_file"]
            if ev.get("stderr_file"):
                record["stderr_file"] = ev["stderr_file"]
            if ev.get("maxrss") is not None:
                record["maxrss"] = ev["maxrss"]
            self._emit(record)
            if ev["timestamp"] > self._high_water_ts:
                self._high_water_ts = ev["timestamp"]

    @staticmethod
    def _condor_fingerprint(jobs: List[Dict]) -> frozenset:
        """Build a fingerprint that captures job identity AND key attributes.

        This ensures an htcondor_poll event is emitted when a job's status
        or hold reason changes, not just when the set of job IDs changes.
        """
        parts = []
        for cj in jobs:
            key = cj.get("ClusterId", cj.get("DAGNodeName", ""))
            status = cj.get("JobStatus", "")
            hold = cj.get("HoldReason", "")
            parts.append((str(key), str(status), hold))
        return frozenset(parts)

    def _record_condor_poll(self, condor_jobs: Optional[List[Dict]]) -> None:
        if condor_jobs is None:
            return
        fp = self._condor_fingerprint(condor_jobs)
        if fp != self._last_condor_fingerprint:
            self._emit({
                "event_type": "htcondor_poll",
                "timestamp": time.time(),
                "jobs": condor_jobs,
            })
            self._last_condor_fingerprint = fp
