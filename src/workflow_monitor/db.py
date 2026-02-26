"""Query the Pegasus stampede SQLite database for workflow monitoring data."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


# ─── State helpers ────────────────────────────────────────────────────────────

# Map raw pegasus jobstate -> simplified display category
_STATE_MAP: Dict[str, str] = {
    "POST_SCRIPT_SUCCESS": "SUCCESS",
    "JOB_SUCCESS": "SUCCESS",
    "POST_SCRIPT_FAILURE": "FAILED",
    "JOB_FAILURE": "FAILED",
    "EXECUTE": "RUNNING",
    "SUBMIT": "QUEUED",
    "PRE_SCRIPT_STARTED": "PRE",
    "PRE_SCRIPT_SUCCESS": "PRE",
    "POST_SCRIPT_STARTED": "POST",
    "POST_SCRIPT_TERMINATED": "POST",
    "JOB_TERMINATED": "DONE",
    "JOB_HELD": "HELD",
}

# Rich color per display state
STATE_STYLE: Dict[str, str] = {
    "SUCCESS": "bold green",
    "FAILED": "bold red",
    "RUNNING": "bold cyan",
    "QUEUED": "yellow",
    "PRE": "blue",
    "POST": "blue",
    "HELD": "magenta",
    "DONE": "green",
    "UNKNOWN": "dim",
}

# Job type labels for display
JOB_TYPE_LABEL: Dict[str, str] = {
    "compute": "compute",
    "stage-in-tx": "stage-in",
    "stage-out-tx": "stage-out",
    "create-dir": "dir-create",
    "stage-worker": "stage-worker",
    "cleanup": "cleanup",
    "registration": "register",
}


def display_state(raw_state: Optional[str]) -> str:
    if raw_state is None:
        return "UNSUBMITTED"
    return _STATE_MAP.get(raw_state, raw_state)


def fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0:
        return "-"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def fmt_timestamp(ts: Optional[float]) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class JobRecord:
    job_id: int
    exec_job_id: str
    type_desc: str
    raw_state: Optional[str]
    exitcode: Optional[int]
    site: Optional[str]
    submit_time: Optional[float]
    start_time: Optional[float]
    end_time: Optional[float]

    @property
    def disp_state(self) -> str:
        return display_state(self.raw_state)

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        if self.start_time and self.disp_state == "RUNNING":
            return datetime.now().timestamp() - self.start_time
        return None

    @property
    def is_compute(self) -> bool:
        return self.type_desc == "compute"

    @property
    def short_name(self) -> str:
        """Strip the run-specific ID suffix for cleaner display."""
        name = self.exec_job_id
        # e.g. preprocess_ID0000001 -> preprocess_ID0000001 (keep as-is)
        return name


@dataclass
class WorkflowSnapshot:
    wf_state: str          # WORKFLOW_STARTED | WORKFLOW_TERMINATED | UNKNOWN
    wf_status: Optional[int]   # exit status (0=success)
    wf_start: Optional[float]
    wf_end: Optional[float]
    jobs: List[JobRecord]
    recent_events: List[Dict]
    poll_time: float = field(default_factory=lambda: datetime.now().timestamp())

    @property
    def is_running(self) -> bool:
        return self.wf_state == "WORKFLOW_STARTED"

    @property
    def is_complete(self) -> bool:
        return self.wf_state == "WORKFLOW_TERMINATED"

    @property
    def succeeded(self) -> bool:
        return self.is_complete and self.wf_status == 0

    @property
    def failed(self) -> bool:
        return self.is_complete and self.wf_status != 0

    @property
    def elapsed(self) -> Optional[float]:
        if self.wf_start is None:
            return None
        end = self.wf_end if self.wf_end else datetime.now().timestamp()
        return end - self.wf_start

    def job_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for job in self.jobs:
            s = job.disp_state
            counts[s] = counts.get(s, 0) + 1
        return counts

    def compute_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if j.is_compute]

    def infra_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if not j.is_compute]

    def total_jobs(self) -> int:
        return len(self.jobs)

    def done_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "SUCCESS")

    def failed_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "FAILED")

    def running_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "RUNNING")

    def queued_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state in ("QUEUED", "PRE", "POST"))

    def progress_pct(self) -> float:
        total = self.total_jobs()
        if total == 0:
            return 0.0
        return 100.0 * self.done_count() / total


# ─── Database class ───────────────────────────────────────────────────────────

class StampedeDB:
    """Read-only interface to the Pegasus stampede SQLite database."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    # ── Connection management ─────────────────────────────────────────────────

    def connect(self) -> None:
        uri = f"file:{self.db_path}?mode=ro"
        self._conn = sqlite3.connect(
            uri, uri=True, timeout=5.0, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "StampedeDB":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _conn_or_raise(self) -> sqlite3.Connection:
        if self._conn is None:
            self.connect()
        return self._conn  # type: ignore[return-value]

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_workflow_state(self) -> Dict:
        cur = self._conn_or_raise().cursor()
        cur.execute(
            """
            SELECT state, timestamp, status
            FROM workflowstate
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return {"state": "UNKNOWN", "timestamp": None, "status": None}
        return dict(row)

    def get_workflow_times(self) -> Dict:
        cur = self._conn_or_raise().cursor()
        cur.execute(
            "SELECT state, timestamp FROM workflowstate ORDER BY timestamp"
        )
        start = end = None
        for row in cur.fetchall():
            if row["state"] == "WORKFLOW_STARTED":
                start = row["timestamp"]
            elif row["state"] == "WORKFLOW_TERMINATED":
                end = row["timestamp"]
        return {"start": start, "end": end}

    def get_jobs(self) -> List[JobRecord]:
        """Return all jobs with their latest observed state."""
        cur = self._conn_or_raise().cursor()
        cur.execute(
            """
            SELECT
                j.job_id,
                j.exec_job_id,
                j.type_desc,
                (
                    SELECT js.state
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id
                    ORDER BY js.timestamp DESC, js.jobstate_submit_seq DESC
                    LIMIT 1
                ) AS current_state,
                (
                    SELECT ji2.exitcode
                    FROM job_instance ji2
                    WHERE ji2.job_id = j.job_id
                    ORDER BY ji2.job_submit_seq DESC
                    LIMIT 1
                ) AS exitcode,
                (
                    SELECT ji2.site
                    FROM job_instance ji2
                    WHERE ji2.job_id = j.job_id
                    ORDER BY ji2.job_submit_seq DESC
                    LIMIT 1
                ) AS site,
                (
                    SELECT MIN(js.timestamp)
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id AND js.state = 'SUBMIT'
                ) AS submit_time,
                (
                    SELECT MIN(js.timestamp)
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id AND js.state = 'EXECUTE'
                ) AS start_time,
                (
                    SELECT MAX(js.timestamp)
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id
                      AND js.state IN ('JOB_TERMINATED', 'JOB_SUCCESS', 'JOB_FAILURE')
                ) AS end_time
            FROM job j
            ORDER BY j.job_id
            """
        )
        jobs = []
        for row in cur.fetchall():
            jobs.append(
                JobRecord(
                    job_id=row["job_id"],
                    exec_job_id=row["exec_job_id"],
                    type_desc=row["type_desc"],
                    raw_state=row["current_state"],
                    exitcode=row["exitcode"],
                    site=row["site"],
                    submit_time=row["submit_time"],
                    start_time=row["start_time"],
                    end_time=row["end_time"],
                )
            )
        return jobs

    def get_recent_events(self, limit: int = 20) -> List[Dict]:
        """Return the *limit* most recent job-state events."""
        cur = self._conn_or_raise().cursor()
        cur.execute(
            """
            SELECT
                j.exec_job_id,
                j.type_desc,
                js.state,
                js.timestamp
            FROM job j
            JOIN job_instance ji ON j.job_id = ji.job_id
            JOIN jobstate js ON ji.job_instance_id = js.job_instance_id
            ORDER BY js.timestamp DESC, js.jobstate_submit_seq DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]

    def snapshot(self) -> WorkflowSnapshot:
        """Capture a full workflow snapshot in one call."""
        try:
            wf_row = self.get_workflow_state()
            wf_times = self.get_workflow_times()
            jobs = self.get_jobs()
            events = self.get_recent_events()
        except sqlite3.OperationalError:
            # DB may be locked momentarily; return a minimal snapshot
            wf_row = {"state": "UNKNOWN", "timestamp": None, "status": None}
            wf_times = {"start": None, "end": None}
            jobs = []
            events = []

        return WorkflowSnapshot(
            wf_state=wf_row.get("state", "UNKNOWN"),
            wf_status=wf_row.get("status"),
            wf_start=wf_times.get("start"),
            wf_end=wf_times.get("end"),
            jobs=jobs,
            recent_events=events,
        )
