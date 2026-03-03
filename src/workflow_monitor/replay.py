"""JSONL replay engine for workflow monitor."""
from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.live import Live

from .braindump import WorkflowInfo
from .db import JobRecord, WorkflowSnapshot
from .display import build_layout


class ReplayEngine:
    """Loads a JSONL event log and replays it through the Rich TUI."""

    def __init__(self, path: Path, speed: float = 1.0, events_n: int = 15) -> None:
        self._path = path
        self._speed = speed
        self._events_n = events_n

        self._events: List[Dict[str, Any]] = []
        self._info: Optional[WorkflowInfo] = None

        self._load()

    # ── Loading ──────────────────────────────────────────────────────────────

    def _load(self) -> None:
        raw: List[Dict[str, Any]] = []
        with open(self._path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    raw.append(json.loads(line))

        if not raw:
            raise ValueError(f"Empty event log: {self._path}")

        # Extract workflow info from header
        header = raw[0]
        if header.get("event_type") != "workflow_start":
            raise ValueError(
                f"First event must be workflow_start, got: {header.get('event_type')}"
            )

        self._info = WorkflowInfo(
            wf_uuid=header.get("wf_uuid", "unknown"),
            root_wf_uuid=header.get("wf_uuid", "unknown"),
            dax_label=header.get("dax_label", "unknown"),
            submit_dir=Path(header.get("submit_dir", ".")),
            user=header.get("user", "unknown"),
            planner_version=header.get("planner_version", "?"),
            dag_file="replay.dag",
            condor_log="replay.log",
            timestamp=str(header.get("timestamp", "")),
            basedir=Path(header.get("submit_dir", ".")),
        )

        # Keep all events except header for replay
        self._events = raw[1:]

    @property
    def info(self) -> WorkflowInfo:
        assert self._info is not None
        return self._info

    # ── Frame grouping ───────────────────────────────────────────────────────

    def _build_frames(self) -> List[List[Dict[str, Any]]]:
        """Group events into frames by integer-second timestamp."""
        if not self._events:
            return []

        frames: List[List[Dict[str, Any]]] = []
        current_frame: List[Dict[str, Any]] = []
        current_ts: Optional[int] = None

        for ev in self._events:
            ts = int(ev.get("timestamp", 0))
            if current_ts is None or ts == current_ts:
                current_frame.append(ev)
                current_ts = ts
            else:
                if current_frame:
                    frames.append(current_frame)
                current_frame = [ev]
                current_ts = ts

        if current_frame:
            frames.append(current_frame)

        return frames

    # ── Snapshot reconstruction ──────────────────────────────────────────────

    def _reconstruct(
        self,
        frame_events: List[Dict[str, Any]],
        job_state: Dict[int, Dict[str, Any]],
        wf_state: str,
        wf_status: Optional[int],
        wf_start: Optional[float],
        wf_end: Optional[float],
        recent_events: List[Dict],
    ) -> tuple:
        """Apply a frame's events and return updated state + snapshot."""
        for ev in frame_events:
            etype = ev.get("event_type")

            if etype == "workflow_state":
                wf_state = ev.get("state", wf_state)
                wf_status = ev.get("status")
                if wf_state == "WORKFLOW_STARTED" and wf_start is None:
                    wf_start = ev.get("timestamp")
                elif wf_state == "WORKFLOW_TERMINATED":
                    wf_end = ev.get("timestamp")

            elif etype == "job_state":
                jid = ev.get("job_id")
                if jid is not None:
                    if jid not in job_state:
                        job_state[jid] = {
                            "exec_job_id": ev.get("exec_job_id", ""),
                            "type_desc": ev.get("type_desc", "compute"),
                            "raw_state": None,
                            "exitcode": None,
                            "site": None,
                            "submit_time": None,
                            "start_time": None,
                            "end_time": None,
                        }
                    js = job_state[jid]
                    state = ev.get("state")
                    js["raw_state"] = state
                    ts = ev.get("timestamp")

                    if state == "SUBMIT" and js["submit_time"] is None:
                        js["submit_time"] = ts
                    elif state == "EXECUTE" and js["start_time"] is None:
                        js["start_time"] = ts
                    elif state in (
                        "JOB_TERMINATED", "JOB_SUCCESS", "JOB_FAILURE",
                    ):
                        js["end_time"] = ts

                    # Add to recent events list
                    recent_events.append({
                        "exec_job_id": ev.get("exec_job_id"),
                        "type_desc": ev.get("type_desc"),
                        "state": state,
                        "timestamp": ts,
                    })

            elif etype == "workflow_end":
                wf_state = ev.get("wf_state", wf_state)
                wf_status = ev.get("wf_status", wf_status)
                if ev.get("wf_state") == "WORKFLOW_TERMINATED" and wf_end is None:
                    wf_end = ev.get("timestamp")

        # Trim recent events to last N
        recent_events[:] = recent_events[-self._events_n:]

        # Build snapshot
        jobs = [
            JobRecord(
                job_id=jid,
                exec_job_id=js["exec_job_id"],
                type_desc=js["type_desc"],
                raw_state=js["raw_state"],
                exitcode=js["exitcode"],
                site=js["site"],
                submit_time=js["submit_time"],
                start_time=js["start_time"],
                end_time=js["end_time"],
            )
            for jid, js in sorted(job_state.items())
        ]

        snap = WorkflowSnapshot(
            wf_state=wf_state,
            wf_status=wf_status,
            wf_start=wf_start,
            wf_end=wf_end,
            jobs=jobs,
            # Recent events in reverse order (newest first) to match db.get_recent_events
            recent_events=list(reversed(recent_events)),
        )

        return snap, wf_state, wf_status, wf_start, wf_end

    # ── Replay loop ──────────────────────────────────────────────────────────

    def run(self, show_all: bool = False) -> None:
        """Run the replay in the terminal."""
        frames = self._build_frames()
        if not frames:
            print("No events to replay.")
            return

        info = self.info
        console = Console()

        replay_info = {"speed": self._speed}

        # Mutable state across frames
        job_state: Dict[int, Dict[str, Any]] = {}
        wf_state = "UNKNOWN"
        wf_status: Optional[int] = None
        wf_start: Optional[float] = None
        wf_end: Optional[float] = None
        recent_events: List[Dict] = []

        with Live(
            console=console,
            screen=True,
            refresh_per_second=4,
            redirect_stderr=False,
        ) as live:
            try:
                for i, frame in enumerate(frames):
                    snap, wf_state, wf_status, wf_start, wf_end = (
                        self._reconstruct(
                            frame, job_state, wf_state, wf_status,
                            wf_start, wf_end, recent_events,
                        )
                    )

                    frame_ts = frame[0].get("timestamp", time.time())
                    layout = build_layout(
                        info, snap, show_all, None,
                        self._events_n, frame_ts,
                        replay_info=replay_info,
                    )
                    live.update(layout)

                    # Sleep between frames
                    if i < len(frames) - 1:
                        next_ts = frames[i + 1][0].get("timestamp", frame_ts)
                        delta = max(0, float(next_ts) - float(frame_ts))
                        if self._speed > 0:
                            time.sleep(delta / self._speed)

                # Hold final frame for 2 seconds
                time.sleep(2.0)

            except KeyboardInterrupt:
                pass

        # Final summary
        console.print()
        if snap.succeeded:
            console.print(
                f"[bold green]✔ Replay complete — workflow succeeded[/bold green]"
            )
        elif snap.failed:
            console.print(
                f"[bold red]✖ Replay complete — workflow FAILED[/bold red]"
            )
        else:
            console.print(
                f"[yellow]◌ Replay stopped[/yellow]  (state: {snap.wf_state})"
            )
        console.print()
