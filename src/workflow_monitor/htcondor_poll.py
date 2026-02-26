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
                "ClusterId", "ProcId", "JobStatus",
                "Cmd", "RemoteHost", "QDate", "DAGNodeName",
                "Owner",
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

def _query_via_subprocess(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
) -> List[Dict]:
    """Query HTCondor queue via ``condor_q -json``."""
    cmd = ["condor_q", "-json"]
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


def format_job_status(status_code: Any) -> str:
    try:
        return JOB_STATUS.get(int(status_code), str(status_code))
    except (TypeError, ValueError):
        return str(status_code)
