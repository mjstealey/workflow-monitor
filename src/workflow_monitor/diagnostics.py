"""Diagnostics for held and failed jobs — pattern matching and remediation guidance."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .db import JobRecord


@dataclass
class Diagnostic:
    """A single diagnostic finding for a job."""
    job_name: str
    severity: str  # "held", "failed", "warning"
    summary: str
    reason: str
    suggestions: List[str]


# ─── Hold reason patterns ────────────────────────────────────────────────────
# Each entry: (compiled regex, short summary, list of suggestions)

_HOLD_PATTERNS: List[tuple] = [
    (
        re.compile(r"Transfer output files failure.*No such file or directory", re.I),
        "Output file transfer failed — expected file not created",
        [
            "Check that the job executable actually produces the declared output file(s)",
            "Review the .err and .out files in the submit dir for runtime errors",
            "Verify the output filename in the Pegasus workflow matches what the script writes",
            "Run `pegasus-analyzer <submit_dir>` for detailed failure info",
        ],
    ),
    (
        re.compile(r"Transfer output files failure", re.I),
        "Output file transfer failed",
        [
            "Check job stderr (.err file) for errors that prevented output creation",
            "Verify network connectivity between execute and submit hosts",
            "Check disk space on both execute and submit nodes",
            "Run `pegasus-analyzer <submit_dir>` for detailed failure info",
        ],
    ),
    (
        re.compile(r"Transfer input files failure", re.I),
        "Input file transfer failed",
        [
            "Verify that all input files exist in the staging directory",
            "Check network connectivity between submit and execute hosts",
            "Check disk space on the execute node",
            "Review the Replica Catalog for correct file paths",
        ],
    ),
    (
        re.compile(r"memory (usage|limit)|MEMORY_EXCEEDED|OOM|oom-kill", re.I),
        "Job exceeded memory limit",
        [
            "Increase request_memory in the Pegasus transformation catalog or profiles",
            "Add pegasus profile: memory=<MB> to the job in the workflow",
            "Check if the job has a memory leak or processes more data than expected",
        ],
    ),
    (
        re.compile(r"disk (usage|limit)|DISK_EXCEEDED", re.I),
        "Job exceeded disk limit",
        [
            "Increase request_disk in the transformation catalog or profiles",
            "Clean up temporary files within the job script",
            "Check if input data is larger than expected",
        ],
    ),
    (
        re.compile(r"wall.?time|TIME_EXCEEDED|exceeded.*time", re.I),
        "Job exceeded wall time limit",
        [
            "Increase +MaxWallTimeMins or periodic_hold conditions",
            "Check if the job is hanging or running slower than expected",
            "Consider breaking the job into smaller chunks",
        ],
    ),
    (
        re.compile(r"Cannot access myproxy|credential|proxy|certificate", re.I),
        "Credential or proxy issue",
        [
            "Check that grid credentials/proxies are valid and not expired",
            "Run `grid-proxy-info` or `voms-proxy-info` to verify",
            "Renew credentials with `grid-proxy-init` or `voms-proxy-init`",
        ],
    ),
    (
        re.compile(r"SHADOW_EXCEPTION|shadow", re.I),
        "Condor shadow exception",
        [
            "This is often a transient HTCondor issue — try `condor_release <job_id>`",
            "Check condor_schedd logs for more detail",
            "Verify network stability between submit and execute hosts",
        ],
    ),
    (
        re.compile(r"UNRESOLVABLE|cannot resolve|DNS", re.I),
        "DNS or hostname resolution failure",
        [
            "Check DNS configuration on execute nodes",
            "Verify that COLLECTOR_HOST and submit host are resolvable",
        ],
    ),
    (
        re.compile(r"docker|container|singularity|apptainer", re.I),
        "Container runtime error",
        [
            "Verify the container image exists and is accessible from execute nodes",
            "Check container runtime logs on the execute node",
            "Ensure the container runtime (Docker/Singularity/Apptainer) is properly configured",
        ],
    ),
    (
        re.compile(r"periodic.*hold|policy", re.I),
        "Held by periodic hold policy",
        [
            "Review the submit file's periodic_hold expression",
            "Check resource usage (memory, disk, runtime) against policy limits",
            "Use `condor_q -l <job_id> | grep HoldReason` for the full reason",
        ],
    ),
]

# ─── Failed job patterns (based on stderr/exitcode) ─────────────────────────

_FAILURE_SUGGESTIONS: Dict[Optional[int], List[str]] = {
    None: [
        "Run `pegasus-analyzer <submit_dir>` for detailed failure information",
        "Check the .out and .err files in the submit directory",
    ],
    1: [
        "Exit code 1 — general error in the job executable",
        "Check the .err file for error messages",
        "Run `pegasus-analyzer <submit_dir>` for stderr output",
    ],
    2: [
        "Exit code 2 — often indicates misuse of a shell command or missing arguments",
        "Verify the job arguments in the transformation catalog",
    ],
    126: [
        "Exit code 126 — command found but not executable",
        "Check file permissions on the executable",
        "Ensure the executable has the correct shebang line",
    ],
    127: [
        "Exit code 127 — command not found",
        "Verify the executable path in the transformation catalog",
        "Check that required software is installed on execute nodes",
        "If using containers, ensure the executable exists inside the container",
    ],
    137: [
        "Exit code 137 — killed by SIGKILL (likely OOM killer)",
        "Increase memory allocation for this job",
        "Add pegasus profile: memory=<MB> to request more memory",
    ],
    139: [
        "Exit code 139 — segmentation fault",
        "Debug the executable — this is a code bug, not an infrastructure issue",
        "Check for memory corruption or invalid pointer access",
    ],
}


# ─── Public API ──────────────────────────────────────────────────────────────

def diagnose_held_job(
    job: JobRecord,
    hold_reason: Optional[str] = None,
) -> Diagnostic:
    """Generate a diagnostic for a held job."""
    reason = hold_reason or "No hold reason available"
    summary = "Job held by HTCondor"
    suggestions = [
        "Use `condor_q -hold <job_id>` to see the full hold reason",
        "Try `condor_release <job_id>` to release the job if the issue is resolved",
        "Run `pegasus-analyzer <submit_dir>` for Pegasus-level diagnostics",
    ]

    if hold_reason:
        for pattern, pat_summary, pat_suggestions in _HOLD_PATTERNS:
            if pattern.search(hold_reason):
                summary = pat_summary
                suggestions = pat_suggestions
                break

    return Diagnostic(
        job_name=job.exec_job_id,
        severity="held",
        summary=summary,
        reason=reason,
        suggestions=suggestions,
    )


def diagnose_failed_job(job: JobRecord) -> Diagnostic:
    """Generate a diagnostic for a failed job."""
    suggestions = _FAILURE_SUGGESTIONS.get(
        job.exitcode,
        _FAILURE_SUGGESTIONS[None],
    )

    if job.exitcode is not None:
        summary = f"Job failed with exit code {job.exitcode}"
    else:
        summary = "Job failed (no exit code recorded)"

    return Diagnostic(
        job_name=job.exec_job_id,
        severity="failed",
        summary=summary,
        reason=f"Exit code: {job.exitcode}",
        suggestions=list(suggestions),
    )


def collect_diagnostics(
    held_jobs: List[JobRecord],
    failed_jobs: List[JobRecord],
    condor_jobs: Optional[List[Dict[str, Any]]] = None,
) -> List[Diagnostic]:
    """Collect diagnostics for all problematic jobs."""
    # Build condor lookup: DAGNodeName -> HoldReason
    hold_reasons: Dict[str, str] = {}
    if condor_jobs:
        for cj in condor_jobs:
            node = cj.get("DAGNodeName", "")
            reason = cj.get("HoldReason", "")
            if node and reason:
                hold_reasons[node] = reason

    diagnostics: List[Diagnostic] = []

    for job in held_jobs:
        reason = hold_reasons.get(job.exec_job_id)
        diagnostics.append(diagnose_held_job(job, hold_reason=reason))

    for job in failed_jobs:
        diagnostics.append(diagnose_failed_job(job))

    return diagnostics
