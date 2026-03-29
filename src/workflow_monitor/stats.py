"""Workflow statistics computation.

Aggregates end-of-workflow metrics from the stampede database snapshot
and optional HTCondor history/pool data.  Designed for graceful
degradation — missing data sources produce fewer stats, never errors.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, asdict
from statistics import median
from typing import Any, Dict, List, Optional

from .db import WorkflowSnapshot, fmt_duration, fmt_memory
from .htcondor_poll import (
    PoolSummary,
    cpu_efficiency,
    memory_efficiency,
    queue_wait_seconds,
    format_transfer,
)


@dataclass
class WorkflowStats:
    """Aggregated end-of-workflow statistics."""

    # Job counts
    total_jobs: int = 0
    compute_jobs: int = 0
    infra_jobs: int = 0
    succeeded: int = 0
    failed: int = 0
    held: int = 0

    # Timing (seconds)
    wall_time: Optional[float] = None
    total_compute_time: Optional[float] = None
    parallelism: Optional[float] = None

    # Duration distribution (compute jobs only)
    dur_min: Optional[float] = None
    dur_max: Optional[float] = None
    dur_mean: Optional[float] = None
    dur_median: Optional[float] = None
    longest_job_name: Optional[str] = None
    shortest_job_name: Optional[str] = None

    # Memory from stampede (KB)
    peak_maxrss_kb: Optional[int] = None
    peak_maxrss_job: Optional[str] = None
    mean_maxrss_kb: Optional[float] = None

    # HTCondor efficiency (from condor_history)
    cpu_eff_min: Optional[float] = None
    cpu_eff_max: Optional[float] = None
    cpu_eff_mean: Optional[float] = None
    mem_eff_min: Optional[float] = None
    mem_eff_max: Optional[float] = None
    mem_eff_mean: Optional[float] = None

    # Queue wait (seconds, from condor_history)
    wait_min: Optional[float] = None
    wait_max: Optional[float] = None
    wait_mean: Optional[float] = None

    # CPU-hours consumed (from condor_history)
    cpu_seconds: Optional[float] = None

    # Transfer I/O (bytes, from condor_history)
    transfer_bytes: Optional[int] = None

    # Retries
    retry_count: Optional[int] = None

    # Host distribution
    hosts: Optional[List[str]] = None

    # Pool snapshot
    pool_machines: Optional[int] = None
    pool_total_cpus: Optional[int] = None
    pool_total_gpus: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-safe dict, omitting None values."""
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowStats":
        """Reconstruct from a serialized dict (ignores unknown keys)."""
        field_names = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in d.items() if k in field_names}
        return cls(**filtered)


def compute_workflow_stats(
    snapshot: WorkflowSnapshot,
    condor_history: Optional[List[Dict[str, Any]]] = None,
    pool_status: Optional[PoolSummary] = None,
) -> WorkflowStats:
    """Compute aggregate statistics from a completed workflow snapshot.

    Graceful degradation: condor_history and pool_status are optional.
    Missing data means corresponding stats fields stay None and are
    omitted from the JSONL event and console output.
    """
    s = WorkflowStats()

    # ── Job counts ──────────────────────────────────────────────────────
    s.total_jobs = snapshot.total_jobs()
    compute = snapshot.compute_jobs()
    s.compute_jobs = len(compute)
    s.infra_jobs = s.total_jobs - s.compute_jobs
    s.succeeded = snapshot.done_count()
    s.failed = snapshot.failed_count()
    s.held = snapshot.held_count()

    # ── Timing ──────────────────────────────────────────────────────────
    s.wall_time = snapshot.elapsed

    durations: List[tuple[float, str]] = []
    for j in compute:
        dur = j.duration
        if dur is not None and dur > 0:
            durations.append((dur, j.display_name))

    if durations:
        dur_vals = [d for d, _ in durations]
        s.total_compute_time = sum(dur_vals)
        s.dur_min = min(dur_vals)
        s.dur_max = max(dur_vals)
        s.dur_mean = s.total_compute_time / len(dur_vals)
        s.dur_median = median(dur_vals)

        longest = max(durations, key=lambda x: x[0])
        s.longest_job_name = longest[1]
        shortest = min(durations, key=lambda x: x[0])
        s.shortest_job_name = shortest[1]

        if s.wall_time and s.wall_time > 0:
            s.parallelism = s.total_compute_time / s.wall_time

    # ── Memory from stampede ────────────────────────────────────────────
    mem_records: List[tuple[int, str]] = []
    for j in compute:
        if j.maxrss is not None and j.maxrss > 0:
            mem_records.append((j.maxrss, j.display_name))

    if mem_records:
        peak = max(mem_records, key=lambda x: x[0])
        s.peak_maxrss_kb = peak[0]
        s.peak_maxrss_job = peak[1]
        s.mean_maxrss_kb = sum(m for m, _ in mem_records) / len(mem_records)

    # ── HTCondor efficiency ─────────────────────────────────────────────
    if condor_history:
        cpu_effs: List[float] = []
        mem_effs: List[float] = []
        waits: List[float] = []
        total_xfer = 0
        total_cpu = 0.0
        retries = 0
        host_counts: Dict[str, int] = {}

        for hj in condor_history:
            ce = cpu_efficiency(hj)
            if ce is not None:
                cpu_effs.append(ce)
            me = memory_efficiency(hj)
            if me is not None:
                mem_effs.append(me)
            w = queue_wait_seconds(hj)
            if w is not None:
                waits.append(w)
            total_xfer += (hj.get("BytesSent", 0) or 0) + (hj.get("BytesRecvd", 0) or 0)
            user_cpu = hj.get("RemoteUserCpu")
            if user_cpu is not None:
                try:
                    total_cpu += float(user_cpu)
                except (TypeError, ValueError):
                    pass
            starts = hj.get("NumJobStarts")
            if starts is not None:
                try:
                    n = int(starts)
                    if n > 1:
                        retries += n - 1
                except (TypeError, ValueError):
                    pass
            host = hj.get("LastRemoteHost", "")
            if host:
                # Shorten slot1@machine.domain -> machine
                short = host.split("@")[-1].split(".")[0] if "@" in host else host.split(".")[0]
                host_counts[short] = host_counts.get(short, 0) + 1

        if cpu_effs:
            s.cpu_eff_min = min(cpu_effs)
            s.cpu_eff_max = max(cpu_effs)
            s.cpu_eff_mean = sum(cpu_effs) / len(cpu_effs)
        if mem_effs:
            s.mem_eff_min = min(mem_effs)
            s.mem_eff_max = max(mem_effs)
            s.mem_eff_mean = sum(mem_effs) / len(mem_effs)
        if waits:
            s.wait_min = min(waits)
            s.wait_max = max(waits)
            s.wait_mean = sum(waits) / len(waits)
        if total_cpu > 0:
            s.cpu_seconds = total_cpu
        if total_xfer > 0:
            s.transfer_bytes = total_xfer
        if retries > 0:
            s.retry_count = retries
        if host_counts:
            # Sort by job count descending
            s.hosts = [
                f"{h} ({n})" for h, n in
                sorted(host_counts.items(), key=lambda x: -x[1])
            ]

    # ── Pool snapshot ───────────────────────────────────────────────────
    if pool_status is not None:
        s.pool_machines = pool_status.machines
        s.pool_total_cpus = pool_status.total_cpus
        if pool_status.total_gpus > 0:
            s.pool_total_gpus = pool_status.total_gpus

    return s
