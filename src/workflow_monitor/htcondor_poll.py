"""Live HTCondor queue polling.

Tries the Python bindings first (htcondor / htcondor2).  Falls back to
``condor_q -json`` subprocess when the bindings are unavailable or fail
(e.g. architecture mismatch on macOS).

Credentials can be supplied via the environment or passed explicitly.
Supported mechanisms (in priority order):
  1. IDTOKENS — set CONDOR_CONFIG or COLLECTOR_HOST as needed; token path
     may be supplied via the ``token_path`` parameter.
  2. GSI certificates — set X509_USER_CERT / X509_USER_KEY environment
     variables, or pass ``cert_path`` / ``key_path``.
  3. Username/password — not universally supported; set CONDOR_PASSWORD_FILE
     or pass ``password_file``.

If no credentials are needed (local pool with ALLOW_READ = *), nothing
special is required.
"""
from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Dict, List, Optional

# HTCondor JobStatus codes
JOB_STATUS: Dict[int, str] = {
    1: "Idle",
    2: "Running",
    3: "Removed",
    4: "Completed",
    5: "Held",
    6: "TransferOutput",
    7: "Suspended",
}


# ─── Python bindings (preferred) ─────────────────────────────────────────────

def _try_python_bindings(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
) -> Optional[List[Dict]]:
    """Attempt to query via htcondor or htcondor2 Python bindings."""
    # Configure token path before importing
    if token_path:
        os.environ.setdefault("_CONDOR_SEC_TOKEN_DIRECTORY", str(token_path))

    # Try legacy htcondor first, then htcondor2
    for mod_name in ("htcondor", "htcondor2"):
        try:
            import importlib
            ht = importlib.import_module(mod_name)
        except ImportError:
            continue

        try:
            if collector_host:
                ht.param["COLLECTOR_HOST"] = collector_host

            if schedd_name:
                try:
                    coll = ht.Collector(collector_host or "")
                    schedd_ad = coll.locate(ht.DaemonTypes.Schedd, schedd_name)
                    schedd = ht.Schedd(schedd_ad)
                except Exception:
                    schedd = ht.Schedd()
            else:
                schedd = ht.Schedd()

            projection = [
                # Identity & status
                "ClusterId", "ProcId", "JobStatus",
                "Cmd", "RemoteHost", "QDate", "JobStartDate",
                "DAGNodeName", "Owner",
                "HoldReason", "HoldReasonCode",
                # Resource requests (Tier 1)
                "RequestCpus", "RequestMemory", "RequestDisk",
                "RequestGpus",
                "ImageSize", "NumJobStarts", "AccountingGroup",
                # File transfer & I/O (Tier 2)
                "TransferInputSizeMB",
                "BytesSent", "BytesRecvd",
            ]
            q_args: Dict[str, Any] = {"projection": projection}
            if constraint:
                q_args["constraint"] = constraint

            jobs = [dict(ad) for ad in schedd.query(**q_args)]
            return jobs
        except Exception:
            continue

    return None


# ─── subprocess fallback ──────────────────────────────────────────────────────

_SUBPROCESS_ATTRS = [
    "ClusterId", "ProcId", "JobStatus",
    "Cmd", "RemoteHost", "QDate", "JobStartDate",
    "DAGNodeName", "Owner",
    "HoldReason", "HoldReasonCode",
    "RequestCpus", "RequestMemory", "RequestDisk",
    "RequestGpus",
    "ImageSize", "NumJobStarts", "AccountingGroup",
    "TransferInputSizeMB",
    "BytesSent", "BytesRecvd",
]


def _query_via_subprocess(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
) -> List[Dict]:
    """Query HTCondor queue via ``condor_q -json``."""
    cmd = ["condor_q", "-json", "-attributes", ",".join(_SUBPROCESS_ATTRS)]
    if schedd_name:
        cmd += ["-name", schedd_name]
    if constraint:
        cmd += ["-constraint", constraint]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else []
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return []


# ─── Public interface ─────────────────────────────────────────────────────────

def query_queue(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    password_file: Optional[str] = None,
) -> List[Dict]:
    """Query the HTCondor job queue.

    Returns a list of job ClassAd dicts (may be empty).
    Never raises; errors are swallowed silently.

    Parameters
    ----------
    constraint:      HTCondor ClassAd expression to filter jobs.
    schedd_name:     Name of a specific schedd to query.
    collector_host:  Host[:port] of the pool collector.
    token_path:      Path to an IDTOKEN file or directory.
    cert_path:       Path to GSI certificate.
    key_path:        Path to GSI private key.
    password_file:   Path to a password file.
    """
    # Apply credential environment variables
    if cert_path:
        os.environ["X509_USER_CERT"] = str(cert_path)
    if key_path:
        os.environ["X509_USER_KEY"] = str(key_path)
    if password_file:
        os.environ["_CONDOR_PASSWORD_FILE"] = str(password_file)
    if collector_host:
        os.environ.setdefault("_CONDOR_COLLECTOR_HOST", str(collector_host))

    # Try Python bindings first
    result = _try_python_bindings(
        constraint=constraint,
        schedd_name=schedd_name,
        collector_host=collector_host,
        token_path=token_path,
    )
    if result is not None:
        return result

    # Fall back to subprocess
    return _query_via_subprocess(constraint=constraint, schedd_name=schedd_name)


# ─── History attributes (Tier 3) ────────────────────────────────────────────

_HISTORY_ATTRS = [
    "ClusterId", "ProcId", "DAGNodeName", "Owner", "Cmd",
    "JobStatus", "ExitCode",
    "RemoteWallClockTime", "RemoteUserCpu", "RemoteSysCpu",
    "CumulativeRemoteUserCpu",
    "RequestCpus", "RequestMemory", "RequestDisk", "RequestGpus",
    "ImageSize", "DiskUsage",
    "LastRemoteHost",
    "BytesSent", "BytesRecvd",
    "NumJobStarts",
]


def _try_history_bindings(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    match: int = 200,
) -> Optional[List[Dict]]:
    """Attempt to query job history via Python bindings."""
    if token_path:
        os.environ.setdefault("_CONDOR_SEC_TOKEN_DIRECTORY", str(token_path))

    for mod_name in ("htcondor", "htcondor2"):
        try:
            import importlib
            ht = importlib.import_module(mod_name)
        except ImportError:
            continue

        try:
            if collector_host:
                ht.param["COLLECTOR_HOST"] = collector_host

            if schedd_name:
                try:
                    coll = ht.Collector(collector_host or "")
                    schedd_ad = coll.locate(ht.DaemonTypes.Schedd, schedd_name)
                    schedd = ht.Schedd(schedd_ad)
                except Exception:
                    schedd = ht.Schedd()
            else:
                schedd = ht.Schedd()

            h_args: Dict[str, Any] = {
                "projection": _HISTORY_ATTRS,
                "match": match,
            }
            if constraint:
                h_args["constraint"] = constraint

            jobs = [dict(ad) for ad in schedd.history(**h_args)]
            return jobs
        except Exception:
            continue

    return None


def _history_via_subprocess(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    match: int = 200,
) -> List[Dict]:
    """Query HTCondor history via ``condor_history -json``."""
    cmd = [
        "condor_history", "-json",
        "-attributes", ",".join(_HISTORY_ATTRS),
        "-match", str(match),
    ]
    if schedd_name:
        cmd += ["-name", schedd_name]
    if constraint:
        cmd += ["-constraint", constraint]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else []
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return []


def query_history(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    password_file: Optional[str] = None,
    match: int = 200,
) -> List[Dict]:
    """Query the HTCondor job history for completed jobs.

    Returns a list of job ClassAd dicts (may be empty).
    Never raises; errors are swallowed silently so the monitor
    continues even when condor_history is unavailable.

    Parameters
    ----------
    constraint:      HTCondor ClassAd expression to filter jobs.
    schedd_name:     Name of a specific schedd to query.
    collector_host:  Host[:port] of the pool collector.
    token_path:      Path to an IDTOKEN file or directory.
    cert_path:       Path to GSI certificate.
    key_path:        Path to GSI private key.
    password_file:   Path to a password file.
    match:           Maximum number of history records to return.
    """
    # Apply credential environment variables (same as query_queue)
    if cert_path:
        os.environ["X509_USER_CERT"] = str(cert_path)
    if key_path:
        os.environ["X509_USER_KEY"] = str(key_path)
    if password_file:
        os.environ["_CONDOR_PASSWORD_FILE"] = str(password_file)
    if collector_host:
        os.environ.setdefault("_CONDOR_COLLECTOR_HOST", str(collector_host))

    result = _try_history_bindings(
        constraint=constraint,
        schedd_name=schedd_name,
        collector_host=collector_host,
        token_path=token_path,
        match=match,
    )
    if result is not None:
        return result

    return _history_via_subprocess(
        constraint=constraint,
        schedd_name=schedd_name,
        match=match,
    )


def format_job_status(status_code: Any) -> str:
    try:
        return JOB_STATUS.get(int(status_code), str(status_code))
    except (TypeError, ValueError):
        return str(status_code)


def format_resources(ad: Dict) -> str:
    """Format resource requests as a compact string like '2c/4G' or '1c/2G/1gpu'."""
    parts = []
    cpus = ad.get("RequestCpus")
    if cpus is not None:
        parts.append(f"{int(cpus)}c")
    mem = ad.get("RequestMemory")
    if mem is not None:
        mem_mb = int(mem)
        if mem_mb >= 1024:
            parts.append(f"{mem_mb / 1024:.1f}G")
        else:
            parts.append(f"{mem_mb}M")
    gpus = ad.get("RequestGpus")
    if gpus is not None and int(gpus) > 0:
        parts.append(f"{int(gpus)}gpu")
    return "/".join(parts) if parts else ""


def format_transfer(ad: Dict) -> str:
    """Format transfer bytes as compact string."""
    sent = ad.get("BytesSent", 0) or 0
    recvd = ad.get("BytesRecvd", 0) or 0
    total = sent + recvd
    if total == 0:
        return ""
    if total < 1024:
        return f"{total}B"
    if total < 1024 * 1024:
        return f"{total / 1024:.1f}K"
    if total < 1024 * 1024 * 1024:
        return f"{total / (1024 * 1024):.1f}M"
    return f"{total / (1024 * 1024 * 1024):.2f}G"


def queue_wait_seconds(ad: Dict) -> Optional[float]:
    """Compute how long the job waited in queue before starting."""
    qdate = ad.get("QDate")
    start = ad.get("JobStartDate")
    if qdate is not None and start is not None:
        wait = float(start) - float(qdate)
        return max(0.0, wait)
    return None


def cpu_efficiency(ad: Dict) -> Optional[float]:
    """Compute CPU efficiency as a fraction (0.0–1.0+).

    Formula: RemoteUserCpu / (RemoteWallClockTime * RequestCpus)
    Returns None if insufficient data is available.
    """
    wall = ad.get("RemoteWallClockTime")
    user_cpu = ad.get("RemoteUserCpu")
    cpus = ad.get("RequestCpus", 1)
    if wall is None or user_cpu is None:
        return None
    try:
        wall_f = float(wall)
        cpu_f = float(user_cpu)
        cpus_f = float(cpus) if cpus else 1.0
        if wall_f <= 0 or cpus_f <= 0:
            return None
        return cpu_f / (wall_f * cpus_f)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def memory_efficiency(ad: Dict) -> Optional[float]:
    """Compute memory efficiency as a fraction (0.0–1.0+).

    Formula: ImageSize (KB) / (RequestMemory (MB) * 1024)
    Returns None if insufficient data is available.
    """
    image_kb = ad.get("ImageSize")
    req_mb = ad.get("RequestMemory")
    if image_kb is None or req_mb is None:
        return None
    try:
        image_f = float(image_kb)
        req_f = float(req_mb) * 1024.0  # convert MB to KB
        if req_f <= 0:
            return None
        return image_f / req_f
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def format_efficiency(eff: Optional[float]) -> str:
    """Format an efficiency fraction as a percentage string."""
    if eff is None:
        return "-"
    return f"{eff * 100:.0f}%"


def format_disk_usage(kb: Optional[Any]) -> str:
    """Format DiskUsage (KB) as a compact string."""
    if kb is None:
        return "-"
    try:
        kb_f = float(kb)
    except (TypeError, ValueError):
        return "-"
    if kb_f < 1024:
        return f"{kb_f:.0f}K"
    mb = kb_f / 1024
    if mb < 1024:
        return f"{mb:.1f}M"
    return f"{mb / 1024:.2f}G"
