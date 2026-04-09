"""Diagnostics engine — single integration point for stall detection,
hold/failure analysis, and idle-job diagnosis.

The engine is opt-in (activated by ``--diagnose``) and runs as an additive
layer on top of the normal monitoring loop. It owns:

  • A ``StallDetector`` state machine
  • A per-job dedup set for hold/failure diagnoses
  • A ``DiagnosticLogger`` writing to ``diagnostics-events.jsonl``

Server, local live, and (future) replay modes all instantiate the engine
the same way and call ``tick()`` once per poll cycle.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .braindump import WorkflowInfo
from .db import WorkflowSnapshot
from .diag_log import DiagnosticLogger
from .htcondor_poll import PoolSummary
from .stall_detector import StallDetector


class DiagnosticsEngine:
    """Stateful engine that emits diagnostic events to a JSONL sidecar."""

    def __init__(
        self,
        info: WorkflowInfo,
        diag_log_path: Path,
        poll_interval: float = 2.0,
        condor_constraint: Optional[str] = None,
        condor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._info = info
        self._condor_constraint = condor_constraint
        self._ck = condor_kwargs or {}
        self._detector = StallDetector(poll_interval=poll_interval)
        self._logger = DiagnosticLogger(
            wf_uuid=info.wf_uuid,
            log_path=diag_log_path,
            engine_config=self._detector.config,
        )

    @property
    def path(self) -> Path:
        return self._logger.path

    def tick(
        self,
        snap: WorkflowSnapshot,
        condor_jobs: Optional[List[Dict[str, Any]]],
        pool: Optional[PoolSummary],
        high_water_ts: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Run one diagnostic cycle. Returns the list of events emitted."""
        emitted: List[Dict[str, Any]] = []

        # 1. Stall detection
        stall_event = self._detector.check(snap, high_water_ts)
        if stall_event is not None:
            self._logger.emit(stall_event)
            emitted.append(stall_event)

            if stall_event["event_type"] == "stall_detected":
                # Auto-diagnose on confirmed stall
                emitted.extend(self._auto_diagnose(snap, condor_jobs, pool))

        # 2. Continuous held/failed job diagnostics (independent of stall state)
        held_failed_events = self._diagnose_held_failed(snap, condor_jobs)
        emitted.extend(held_failed_events)

        return emitted

    def close(self) -> None:
        self._logger.close()

    # ── Internal helpers ────────────────────────────────────────────────────

    def _auto_diagnose(
        self,
        snap: WorkflowSnapshot,
        condor_jobs: Optional[List[Dict[str, Any]]],
        pool: Optional[PoolSummary],
    ) -> List[Dict[str, Any]]:
        """Run idle diagnosis when a stall is confirmed."""
        emitted: List[Dict[str, Any]] = []

        if snap.queued_count() <= 0:
            return emitted

        try:
            from .why_idle import (
                _analyze,
                _get_idle_job_details,
                _query_negotiator,
                _query_userprio,
            )

            idle_condor = _get_idle_job_details(
                constraint=self._condor_constraint,
                schedd_name=self._ck.get("schedd_name"),
            )
            user_prio = _query_userprio(
                collector_host=self._ck.get("collector_host"),
            )
            negotiator = _query_negotiator(
                collector_host=self._ck.get("collector_host"),
            )
            diag = _analyze(
                snapshot=snap,
                idle_condor_jobs=idle_condor,
                pool_summary=pool,
                user_prio=user_prio,
                negotiator=negotiator,
                current_user=self._info.user,
            )

            event = {
                "event_type": "idle_diagnosis",
                "timestamp": time.time(),
                "idle_job_count": len(diag.idle_jobs),
                "pool_available": diag.pool_available,
                "pool_idle_cpus": diag.pool_idle_cpus,
                "pool_total_cpus": diag.pool_total_cpus,
                "pool_idle_memory_mb": diag.pool_idle_memory_mb,
                "pool_total_memory_mb": diag.pool_total_memory_mb,
                "findings": diag.findings,
                "suggestions": diag.suggestions,
                "requirement_mismatches": diag.requirement_mismatches,
            }
            self._logger.emit(event)
            emitted.append(event)
        except Exception as exc:
            err = {
                "event_type": "diag_error",
                "timestamp": time.time(),
                "stage": "idle_diagnosis",
                "error": str(exc),
            }
            self._logger.emit(err)
            emitted.append(err)

        return emitted

    def _diagnose_held_failed(
        self,
        snap: WorkflowSnapshot,
        condor_jobs: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Run hold/failure diagnostics for any newly problematic jobs."""
        held = snap.held_jobs() if hasattr(snap, "held_jobs") else [
            j for j in snap.jobs if j.disp_state == "HELD"
        ]
        failed = snap.failed_jobs() if hasattr(snap, "failed_jobs") else [
            j for j in snap.jobs if j.disp_state == "FAILED"
        ]
        if not held and not failed:
            return []

        try:
            from .diagnostics import collect_diagnostics

            results = collect_diagnostics(
                held_jobs=held,
                failed_jobs=failed,
                condor_jobs=condor_jobs,
                submit_dir=self._info.submit_dir,
            )
        except Exception as exc:
            err = {
                "event_type": "diag_error",
                "timestamp": time.time(),
                "stage": "held_failed",
                "error": str(exc),
            }
            self._logger.emit(err)
            return [err]

        emitted: List[Dict[str, Any]] = []
        diagnosed = self._detector.state.diagnosed_jobs
        for d in results:
            key = f"{d.severity}:{d.job_name}"
            if key in diagnosed:
                continue
            diagnosed.add(key)
            event_type = (
                "hold_diagnosis" if d.severity == "held" else "failure_diagnosis"
            )
            event = {
                "event_type": event_type,
                "timestamp": time.time(),
                "job_name": d.job_name,
                "severity": d.severity,
                "summary": d.summary,
                "reason": d.reason,
                "suggestions": d.suggestions,
            }
            self._logger.emit(event)
            emitted.append(event)

        return emitted
