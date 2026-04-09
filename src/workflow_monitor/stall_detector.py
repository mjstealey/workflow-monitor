"""Stall detection state machine for the diagnostics engine.

A ``StallDetector`` is instantiated once per monitoring session. The engine
calls ``check(snap, high_water_ts)`` once per poll cycle. The detector
returns ``None`` (no event), or a dict describing a confirmed stall or a
stall resolution.

State machine: ``normal`` -> ``suspected`` -> ``confirmed``. A heuristic
must trigger on two consecutive cycles before a stall is confirmed,
absorbing transient blips. When progress resumes, ``stall_resolved`` is
emitted and state is reset.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from .db import WorkflowSnapshot


@dataclass
class StallState:
    status: str = "normal"  # "normal" | "suspected" | "confirmed"
    stall_type: Optional[str] = None
    since: Optional[float] = None
    last_emit_time: float = 0.0
    last_done_count: int = -1
    last_high_water_ts: float = 0.0
    last_progress_time: float = 0.0
    workflow_start_time: Optional[float] = None
    diagnosed_jobs: Set[str] = field(default_factory=set)


class StallDetector:
    """Detect workflow stalls across poll cycles."""

    def __init__(
        self,
        poll_interval: float = 2.0,
        cooldown_seconds: float = 120.0,
        no_transition_seconds: float = 60.0,
        progress_plateau_seconds: float = 120.0,
        idle_threshold_seconds: float = 120.0,
        startup_grace_seconds: float = 60.0,
    ) -> None:
        self.poll_interval = poll_interval
        self.cooldown = cooldown_seconds
        self.no_transition_seconds = no_transition_seconds
        self.progress_plateau_seconds = progress_plateau_seconds
        self.idle_threshold_seconds = idle_threshold_seconds
        self.startup_grace_seconds = startup_grace_seconds
        self.state = StallState()
        self._suspected_type: Optional[str] = None
        self._suspected_since: Optional[float] = None

    @property
    def config(self) -> Dict[str, Any]:
        return {
            "poll_interval": self.poll_interval,
            "cooldown_seconds": self.cooldown,
            "no_transition_seconds": self.no_transition_seconds,
            "progress_plateau_seconds": self.progress_plateau_seconds,
            "idle_threshold_seconds": self.idle_threshold_seconds,
            "startup_grace_seconds": self.startup_grace_seconds,
        }

    def is_stalled(self) -> bool:
        return self.state.status == "confirmed"

    def check(
        self, snap: WorkflowSnapshot, high_water_ts: float
    ) -> Optional[Dict[str, Any]]:
        """Run one detection cycle. Returns an event dict or None."""
        now = time.time()

        # Initialize workflow start time on first call
        if self.state.workflow_start_time is None:
            self.state.workflow_start_time = now
            self.state.last_progress_time = now
            self.state.last_high_water_ts = high_water_ts
            self.state.last_done_count = snap.done_count()
            return None

        # ── False-positive guards ───────────────────────────────────────────
        if snap.is_complete:
            return self._maybe_resolve(now, "Workflow completed")

        # No jobs submitted yet — DAG hasn't started
        total = snap.total_jobs()
        done = snap.done_count()
        running = snap.running_count() if hasattr(snap, "running_count") else sum(
            1 for j in snap.jobs if j.disp_state == "RUNNING"
        )
        queued = snap.queued_count()
        held = snap.held_count()
        unsubmitted = sum(1 for j in snap.jobs if j.disp_state == "UNSUBMITTED")

        if total == 0 or unsubmitted == total:
            return None

        # Startup grace
        if (now - self.state.workflow_start_time) < self.startup_grace_seconds:
            # Still update progress markers so the grace window starts cleanly
            if done > self.state.last_done_count or high_water_ts > self.state.last_high_water_ts:
                self.state.last_done_count = done
                self.state.last_high_water_ts = high_water_ts
                self.state.last_progress_time = now
            return None

        # ── Progress detection ──────────────────────────────────────────────
        progressed = (
            done > self.state.last_done_count
            or high_water_ts > self.state.last_high_water_ts
        )
        if progressed:
            self.state.last_done_count = done
            self.state.last_high_water_ts = high_water_ts
            self.state.last_progress_time = now
            return self._maybe_resolve(now, "Progress resumed")

        seconds_since_progress = now - self.state.last_progress_time

        # ── Heuristics in priority order ────────────────────────────────────
        active = total - done - snap.failed_count()
        all_held = held > 0 and held == active

        triggered: Optional[str] = None
        details: str = ""

        if all_held:
            triggered = "all_held"
            details = f"All {held} active job(s) are HELD"
        elif (queued > 0 or running > 0) and seconds_since_progress >= self.no_transition_seconds:
            triggered = "no_transitions"
            details = f"No job state transitions for {seconds_since_progress:.0f}s"
        elif running > 0 and seconds_since_progress >= self.progress_plateau_seconds:
            triggered = "progress_plateau"
            details = (
                f"Done count flat at {done}/{total} for {seconds_since_progress:.0f}s "
                f"with {running} job(s) running"
            )
        elif queued > 0 and running == 0 and seconds_since_progress >= self.idle_threshold_seconds:
            triggered = "idle_too_long"
            details = (
                f"{queued} job(s) queued with 0 running for {seconds_since_progress:.0f}s"
            )

        if triggered is None:
            return None

        # ── Two-strike state machine ────────────────────────────────────────
        if self._suspected_type != triggered:
            # First strike (or stall type changed)
            self._suspected_type = triggered
            self._suspected_since = now
            return None

        # Second consecutive strike — confirm
        if self.state.status == "confirmed" and self.state.stall_type == triggered:
            # Already confirmed with same type — respect cooldown
            if (now - self.state.last_emit_time) < self.cooldown:
                return None

        # Confirm (or re-confirm with new type)
        self.state.status = "confirmed"
        self.state.stall_type = triggered
        if self.state.since is None:
            self.state.since = self._suspected_since or now
        self.state.last_emit_time = now

        return {
            "event_type": "stall_detected",
            "timestamp": now,
            "stall_type": triggered,
            "details": details,
            "total_jobs": total,
            "done_jobs": done,
            "running_jobs": running,
            "queued_jobs": queued,
            "held_jobs": held,
            "failed_jobs": snap.failed_count(),
            "seconds_since_progress": round(seconds_since_progress, 1),
        }

    def _maybe_resolve(self, now: float, reason: str) -> Optional[Dict[str, Any]]:
        """If currently in a confirmed stall, emit stall_resolved and reset."""
        was_confirmed = self.state.status == "confirmed"
        stall_type = self.state.stall_type
        since = self.state.since

        # Reset suspicion regardless
        self._suspected_type = None
        self._suspected_since = None

        if not was_confirmed:
            self.state.status = "normal"
            self.state.stall_type = None
            self.state.since = None
            return None

        duration = (now - since) if since else 0.0
        self.state.status = "normal"
        self.state.stall_type = None
        self.state.since = None
        self.state.diagnosed_jobs.clear()

        return {
            "event_type": "stall_resolved",
            "timestamp": now,
            "stall_type": stall_type,
            "stall_duration_seconds": round(duration, 1),
            "resolution": reason,
        }
