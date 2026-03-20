"""Diagnostics for held and failed jobs — pattern matching and remediation guidance."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .db import JobRecord, real_exitcode as _raw_to_real


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

# ─── Stderr patterns for failed jobs ────────────────────────────────────────
# Each entry: (compiled regex, summary builder, suggestions)
# The regex is matched against the full stderr text from kickstart output.
# Groups captured by the regex are passed to the summary builder.

_RE_MISSING_FILE = re.compile(
    r"Expected local file does not exist:\s*(.+)", re.I
)
_RE_TRANSFER_ATTEMPT = re.compile(
    r"Starting transfers - attempt (\d+)", re.I
)
_RE_TRANSFERS_FAILED = re.compile(
    r"Some transfers failed", re.I
)
_RE_INTEGRITY_ERROR = re.compile(
    r"integrity verification failed", re.I
)
_RE_PERMISSION_DENIED = re.compile(
    r"Permission denied.*?:\s*(.+)", re.I
)
_RE_NO_SPACE = re.compile(
    r"No space left on device", re.I
)
_RE_CONNECTION_REFUSED = re.compile(
    r"Connection refused|Connection timed out|Network is unreachable", re.I
)


@dataclass
class StderrAnalysis:
    """Parsed findings from kickstart stderr content."""
    missing_files: List[str]
    transfer_attempts: int
    transfers_failed: bool
    integrity_error: bool
    permission_denied: List[str]
    no_space: bool
    connection_error: bool

    @property
    def has_findings(self) -> bool:
        return bool(
            self.missing_files
            or self.transfer_attempts > 1
            or self.transfers_failed
            or self.integrity_error
            or self.permission_denied
            or self.no_space
            or self.connection_error
        )


def _analyze_stderr(stderr: str) -> StderrAnalysis:
    """Extract structured findings from kickstart stderr."""
    missing = [m.group(1).strip() for m in _RE_MISSING_FILE.finditer(stderr)]
    # Deduplicate while preserving order (retries repeat the same error)
    seen = set()
    unique_missing = []
    for f in missing:
        if f not in seen:
            seen.add(f)
            unique_missing.append(f)

    attempts = [int(m.group(1)) for m in _RE_TRANSFER_ATTEMPT.finditer(stderr)]
    max_attempt = max(attempts) if attempts else 1

    return StderrAnalysis(
        missing_files=unique_missing,
        transfer_attempts=max_attempt,
        transfers_failed=bool(_RE_TRANSFERS_FAILED.search(stderr)),
        integrity_error=bool(_RE_INTEGRITY_ERROR.search(stderr)),
        permission_denied=[
            m.group(1).strip() for m in _RE_PERMISSION_DENIED.finditer(stderr)
        ],
        no_space=bool(_RE_NO_SPACE.search(stderr)),
        connection_error=bool(_RE_CONNECTION_REFUSED.search(stderr)),
    )


# ─── Kickstart output parsing ────────────────────────────────────────────────

@dataclass
class KickstartInfo:
    """Parsed info from a kickstart invocation record (.out.000)."""
    exitcode: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    executable: str = ""
    argv: List[str] = None  # type: ignore[assignment]
    duration: Optional[float] = None
    transformation: str = ""
    stderr_analysis: Optional[StderrAnalysis] = None

    def __post_init__(self):
        if self.argv is None:
            self.argv = []


def parse_kickstart_output(
    submit_dir: Path,
    exec_job_id: str,
    stdout_file: Optional[str] = None,
) -> Optional[KickstartInfo]:
    """Parse the kickstart .out.000 file for a job, extracting stdout/stderr."""
    if stdout_file:
        out_file = submit_dir / stdout_file
    else:
        out_file = submit_dir / "00" / "00" / f"{exec_job_id}.out.000"
    if not out_file.exists():
        return None

    try:
        with open(out_file) as f:
            data = yaml.safe_load(f)
    except Exception:
        return None

    # Kickstart records load as a list with one dict entry
    if isinstance(data, list):
        data = data[0] if data else {}
    if not isinstance(data, dict):
        return None

    mainjob = data.get("mainjob", {})
    status = mainjob.get("status", {})
    executable = mainjob.get("executable", {})

    # stdout/stderr are in the top-level 'files' section, not in mainjob
    files = data.get("files", {})
    stdout_data = (files.get("stdout", {}).get("data") or "").strip()
    stderr_data = (files.get("stderr", {}).get("data") or "").strip()

    stderr_findings = _analyze_stderr(stderr_data) if stderr_data else None

    return KickstartInfo(
        exitcode=status.get("regular_exitcode"),
        stdout=stdout_data,
        stderr=stderr_data,
        executable=executable.get("file_name", ""),
        argv=mainjob.get("argument_vector", []),
        duration=mainjob.get("duration"),
        transformation=data.get("transformation", ""),
        stderr_analysis=stderr_findings,
    )


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


def _real_exitcode(raw: Optional[int], kickstart: Optional[KickstartInfo] = None) -> Optional[int]:
    """Convert raw wait status to real exit code, using kickstart if available."""
    if kickstart and kickstart.exitcode is not None:
        return kickstart.exitcode
    return _raw_to_real(raw)


def diagnose_failed_job(
    job: JobRecord,
    kickstart: Optional[KickstartInfo] = None,
) -> Diagnostic:
    """Generate a diagnostic for a failed job."""
    exitcode = _real_exitcode(job.exitcode, kickstart)
    analysis = kickstart.stderr_analysis if kickstart else None

    # Try stderr-driven diagnostics first, fall back to exit-code-based
    summary, suggestions = _diagnose_from_stderr(analysis, exitcode)

    if summary is None:
        suggestions = list(
            _FAILURE_SUGGESTIONS.get(exitcode, _FAILURE_SUGGESTIONS[None])
        )
        if exitcode is not None:
            summary = f"Job failed with exit code {exitcode}"
        else:
            summary = "Job failed (no exit code recorded)"

    # Build reason from kickstart output when available
    reason_parts: List[str] = [f"Exit code: {exitcode}"]
    if kickstart:
        if kickstart.stdout:
            reason_parts.append(f"stdout: {kickstart.stdout}")
        if kickstart.stderr:
            reason_parts.append(f"stderr: {kickstart.stderr}")
        if kickstart.executable:
            reason_parts.append(f"executable: {kickstart.executable}")
    reason = " | ".join(reason_parts)

    return Diagnostic(
        job_name=job.exec_job_id,
        severity="failed",
        summary=summary,
        reason=reason,
        suggestions=suggestions,
    )


def _diagnose_from_stderr(
    analysis: Optional[StderrAnalysis],
    exitcode: Optional[int],
) -> tuple:
    """Derive summary + suggestions from stderr analysis.

    Returns (summary, suggestions) or (None, []) if no specific pattern matched.
    """
    if analysis is None or not analysis.has_findings:
        return None, []

    # Missing input file — most specific
    if analysis.missing_files:
        retry_note = ""
        if analysis.transfer_attempts > 1:
            retry_note = f" ({analysis.transfer_attempts} transfer attempts failed)"
        summary = f"Input file missing{retry_note}"
        suggestions = []
        for path in analysis.missing_files:
            suggestions.append(f"Missing: {path}")
        suggestions.extend([
            "Verify the file exists at the expected path",
            "Check the Replica Catalog for correct PFN entries",
            "Run `pegasus-analyzer <submit_dir>` for full transfer details",
        ])
        return summary, suggestions

    # Transfer failures without a specific missing file
    if analysis.transfers_failed:
        retry_note = ""
        if analysis.transfer_attempts > 1:
            retry_note = f" after {analysis.transfer_attempts} attempts"
        summary = f"File transfers failed{retry_note}"
        suggestions = [
            "Check the kickstart .out.000 stderr for specific transfer errors",
            "Verify network connectivity between submit and execute hosts",
            "Check disk space on both submit and execute nodes",
            "Run `pegasus-analyzer <submit_dir>` for full transfer details",
        ]
        return summary, suggestions

    # Integrity verification
    if analysis.integrity_error:
        summary = "File integrity verification failed"
        suggestions = [
            "A transferred file's checksum did not match the expected value",
            "Check for data corruption or incomplete transfers",
            "Verify the Replica Catalog checksums are correct",
            "Run `pegasus-analyzer <submit_dir>` for details",
        ]
        return summary, suggestions

    # Permission denied
    if analysis.permission_denied:
        summary = "Permission denied during file transfer"
        suggestions = []
        for path in analysis.permission_denied:
            suggestions.append(f"Permission denied: {path}")
        suggestions.extend([
            "Check file and directory permissions on both submit and execute hosts",
            "Verify the job runs as the expected user",
        ])
        return summary, suggestions

    # No space
    if analysis.no_space:
        summary = "No space left on device"
        suggestions = [
            "Check disk space on the execute node",
            "Clean up scratch or staging directories",
            "Increase request_disk in the transformation catalog",
        ]
        return summary, suggestions

    # Connection error
    if analysis.connection_error:
        retry_note = ""
        if analysis.transfer_attempts > 1:
            retry_note = f" after {analysis.transfer_attempts} attempts"
        summary = f"Network error during file transfer{retry_note}"
        suggestions = [
            "Check network connectivity between submit and execute hosts",
            "Verify that the file server or staging site is reachable",
            "Check firewall rules and proxy configuration",
        ]
        return summary, suggestions

    return None, []


def collect_diagnostics(
    held_jobs: List[JobRecord],
    failed_jobs: List[JobRecord],
    condor_jobs: Optional[List[Dict[str, Any]]] = None,
    submit_dir: Optional[Path] = None,
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
        kickstart = None
        if submit_dir:
            kickstart = parse_kickstart_output(
                submit_dir, job.exec_job_id, stdout_file=job.stdout_file,
            )
        diagnostics.append(diagnose_failed_job(job, kickstart=kickstart))

    return diagnostics
