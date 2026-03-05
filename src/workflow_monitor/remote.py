"""SSH client engine for remote workflow monitoring.

Periodically fetches a JSONL event log from a remote server via SSH and
displays it in the Rich TUI, following new events as they arrive.

Usage (from cli.py):
    workflow-monitor --remote user@host:/path/to/workflow-events.jsonl
"""
from __future__ import annotations

import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.live import Live

from .braindump import WorkflowInfo
from .db import JobRecord, WorkflowSnapshot
from .display import build_layout


def _parse_remote_spec(spec: str) -> tuple[str, str]:
    """Parse 'user@host:/path/to/file' into (ssh_target, remote_path).

    Handles IPv6 addresses in square brackets:
        user@[2001:db8::1]:/path/to/file

    Returns (ssh_host_part, remote_path) where ssh_host_part is
    everything before the path-separating colon.
    """
    # IPv6 in brackets: find the closing ']' then expect ':' after it
    if "[" in spec:
        bracket_close = spec.find("]")
        if bracket_close == -1:
            raise ValueError(
                f"Invalid remote spec: {spec!r}\n"
                "Unclosed bracket in IPv6 address.\n"
                "Expected format: user@[IPv6]:/path/to/workflow-events.jsonl"
            )
        # The colon separating host from path comes after ']'
        colon_pos = spec.find(":", bracket_close + 1)
        if colon_pos == -1:
            raise ValueError(
                f"Invalid remote spec: {spec!r}\n"
                "Expected format: user@[IPv6]:/path/to/workflow-events.jsonl"
            )
        host_part = spec[:colon_pos]
        remote_path = spec[colon_pos + 1:]
    else:
        if ":" not in spec:
            raise ValueError(
                f"Invalid remote spec: {spec!r}\n"
                "Expected format: user@host:/path/to/workflow-events.jsonl"
            )
        host_part, remote_path = spec.split(":", 1)

    if not host_part or not remote_path:
        raise ValueError(
            f"Invalid remote spec: {spec!r}\n"
            "Expected format: user@host:/path/to/workflow-events.jsonl"
        )
    return host_part, remote_path


def _build_ssh_base(
    ssh_config: Optional[str] = None,
    ssh_identity: Optional[str] = None,
) -> List[str]:
    """Build the base SSH command as a list of arguments."""
    parts = ["ssh"]
    if ssh_config:
        parts.extend(["-F", ssh_config])
    if ssh_identity:
        parts.extend(["-i", ssh_identity])
    return parts


def _strip_brackets(host: str) -> str:
    """Strip square brackets from an IPv6 host for SSH.

    Remote specs use brackets for IPv6 (user@[ipv6]:path) but SSH
    expects the raw address (user@ipv6).
    """
    # Extract user@ prefix if present
    if "@" in host:
        user, addr = host.rsplit("@", 1)
        addr = addr.strip("[]")
        return f"{user}@{addr}"
    return host.strip("[]")


def _fetch_file(
    host: str,
    remote_path: str,
    local_path: Path,
    ssh_base: List[str],
) -> tuple[bool, str]:
    """Fetch a remote file via ssh cat.

    Uses ``ssh [opts] host cat remote_path`` which works reliably with
    ProxyJump, bastion hosts, and IPv6 addresses.

    Returns (success, stderr_text).
    """
    ssh_host = _strip_brackets(host)
    cmd = ssh_base + [ssh_host, "cat", remote_path]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode == 0:
            local_path.write_bytes(result.stdout)
            return True, ""
        return False, result.stderr.decode(errors="replace").strip()
    except subprocess.TimeoutExpired:
        return False, "ssh timed out"
    except FileNotFoundError:
        return False, "ssh not found in PATH"


class RemoteEngine:
    """Rsyncs a remote JSONL log and displays it in the TUI."""

    def __init__(
        self,
        remote_spec: str,
        sync_interval: float = 5.0,
        events_n: int = 15,
        ssh_config: Optional[str] = None,
        ssh_identity: Optional[str] = None,
    ) -> None:
        self._remote_spec = remote_spec
        self._sync_interval = sync_interval
        self._events_n = events_n

        # Parse and validate remote spec
        self._host, self._remote_path = _parse_remote_spec(remote_spec)

        # Build base SSH command
        self._ssh_base = _build_ssh_base(ssh_config, ssh_identity)

        # Local temp file for the synced JSONL
        self._tmpdir = tempfile.mkdtemp(prefix="wfmon-remote-")
        filename = Path(self._remote_path).name
        self._local_path = Path(self._tmpdir) / filename

        # Replay state
        self._info: Optional[WorkflowInfo] = None
        self._job_state: Dict[int, Dict[str, Any]] = {}
        self._wf_state: str = "UNKNOWN"
        self._wf_status: Optional[int] = None
        self._wf_start: Optional[float] = None
        self._wf_end: Optional[float] = None
        self._recent_events: List[Dict] = []
        self._header_wf_start: Optional[float] = None
        self._prescan_jobs: Dict[int, Dict[str, str]] = {}
        self._processed_lines: int = 0
        self._workflow_complete: bool = False

    def _do_sync(self) -> tuple[bool, str]:
        """Fetch the remote file via SSH."""
        return _fetch_file(
            self._host, self._remote_path, self._local_path, self._ssh_base,
        )

    def _load_new_events(self) -> List[Dict[str, Any]]:
        """Read any new lines from the local JSONL file since last read."""
        if not self._local_path.exists():
            return []

        new_events: List[Dict[str, Any]] = []
        try:
            with open(self._local_path) as fh:
                total_lines = 0
                for i, line in enumerate(fh):
                    total_lines = i + 1
                    if i < self._processed_lines:
                        continue
                    line = line.strip()
                    if line:
                        new_events.append(json.loads(line))
                self._processed_lines = total_lines
        except (json.JSONDecodeError, OSError):
            pass

        return new_events

    def _process_header(self, ev: Dict[str, Any]) -> None:
        """Extract WorkflowInfo from a workflow_start event."""
        self._info = WorkflowInfo(
            wf_uuid=ev.get("wf_uuid", "unknown"),
            root_wf_uuid=ev.get("wf_uuid", "unknown"),
            dax_label=ev.get("dax_label", "unknown"),
            submit_dir=Path(ev.get("submit_dir", ".")),
            user=ev.get("user", "unknown"),
            planner_version=ev.get("planner_version", "?"),
            dag_file="remote.dag",
            condor_log="remote.log",
            timestamp=str(ev.get("timestamp", "")),
            basedir=Path(ev.get("submit_dir", ".")),
        )
        self._header_wf_start = ev.get("wf_start")

    def _apply_event(self, ev: Dict[str, Any]) -> None:
        """Apply a single event to the running state."""
        etype = ev.get("event_type")

        if etype == "workflow_start":
            if self._info is None:
                self._process_header(ev)
            return

        if etype == "workflow_state":
            self._wf_state = ev.get("state", self._wf_state)
            self._wf_status = ev.get("status")
            if self._wf_state == "WORKFLOW_STARTED" and self._wf_start is None:
                self._wf_start = ev.get("timestamp")
            elif self._wf_state == "WORKFLOW_TERMINATED":
                self._wf_end = ev.get("timestamp")

        elif etype == "jobs_init":
            for j in ev.get("jobs", []):
                jid = j.get("job_id")
                if jid is not None and jid not in self._job_state:
                    self._job_state[jid] = {
                        "exec_job_id": j.get("exec_job_id", ""),
                        "type_desc": j.get("type_desc", "compute"),
                        "raw_state": None,
                        "exitcode": None,
                        "site": None,
                        "submit_time": None,
                        "start_time": None,
                        "end_time": None,
                    }

        elif etype == "job_state":
            jid = ev.get("job_id")
            if jid is not None:
                if jid not in self._job_state:
                    self._job_state[jid] = {
                        "exec_job_id": ev.get("exec_job_id", ""),
                        "type_desc": ev.get("type_desc", "compute"),
                        "raw_state": None,
                        "exitcode": None,
                        "site": None,
                        "submit_time": None,
                        "start_time": None,
                        "end_time": None,
                    }
                js = self._job_state[jid]
                state = ev.get("state")
                js["raw_state"] = state
                ts = ev.get("timestamp")

                if state == "SUBMIT" and js["submit_time"] is None:
                    js["submit_time"] = ts
                elif state == "EXECUTE" and js["start_time"] is None:
                    js["start_time"] = ts
                elif state in ("JOB_TERMINATED", "JOB_SUCCESS", "JOB_FAILURE"):
                    js["end_time"] = ts

                self._recent_events.append({
                    "exec_job_id": ev.get("exec_job_id"),
                    "type_desc": ev.get("type_desc"),
                    "state": state,
                    "timestamp": ts,
                })

        elif etype == "workflow_end":
            self._wf_state = ev.get("wf_state", self._wf_state)
            self._wf_status = ev.get("wf_status", self._wf_status)
            if ev.get("wf_state") == "WORKFLOW_TERMINATED" and self._wf_end is None:
                self._wf_end = ev.get("timestamp")
            self._workflow_complete = True

        # Use header wf_start if not yet set from events
        if self._wf_start is None and self._header_wf_start is not None:
            self._wf_start = self._header_wf_start

        # Trim recent events
        self._recent_events = self._recent_events[-self._events_n:]

    def _build_snapshot(self) -> WorkflowSnapshot:
        """Build a WorkflowSnapshot from the current accumulated state."""
        now = time.time()
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
                _now=now,
            )
            for jid, js in sorted(self._job_state.items())
        ]

        return WorkflowSnapshot(
            wf_state=self._wf_state,
            wf_status=self._wf_status,
            wf_start=self._wf_start,
            wf_end=self._wf_end,
            jobs=jobs,
            recent_events=list(reversed(self._recent_events)),
            poll_time=now,
        )

    def run(self, show_all: bool = False) -> None:
        """Run the remote monitoring TUI."""
        console = Console()

        console.print(f"[dim]Connecting to {self._host}...[/dim]")

        # Initial sync — must succeed to get header
        ok, err = self._do_sync()
        if not ok:
            console.print(
                f"[bold red]Failed to fetch from {self._remote_spec}[/bold red]"
            )
            if err:
                console.print(f"[red]{err}[/red]")
            console.print(
                "[dim]Check that SSH access is configured and the file exists.[/dim]"
            )
            return

        # Load initial events
        events = self._load_new_events()
        if not events:
            console.print("[bold red]No events found in remote log file.[/bold red]")
            return

        for ev in events:
            self._apply_event(ev)

        if self._info is None:
            console.print(
                "[bold red]No workflow_start header found in remote log.[/bold red]"
            )
            return

        info = self._info
        remote_info = {"host": self._host}

        console.print(
            f"[dim]Monitoring {info.dax_label} via SSH "
            f"(sync every {self._sync_interval:.0f}s)[/dim]"
        )

        snap = self._build_snapshot()

        with Live(
            console=console,
            screen=True,
            refresh_per_second=2,
            redirect_stderr=False,
        ) as live:
            try:
                last_sync = time.time()

                while True:
                    # Update display with current state
                    snap = self._build_snapshot()
                    layout = build_layout(
                        info, snap, show_all, None,
                        self._events_n, snap.poll_time,
                        remote_info=remote_info,
                    )
                    live.update(layout)

                    if self._workflow_complete:
                        # Hold final state briefly then exit
                        time.sleep(2.0)
                        break

                    # Sleep a short interval, then check if sync is due
                    time.sleep(1.0)

                    if time.time() - last_sync >= self._sync_interval:
                        self._do_sync()  # ignore errors during live sync
                        new_events = self._load_new_events()
                        for ev in new_events:
                            self._apply_event(ev)
                        last_sync = time.time()

            except KeyboardInterrupt:
                pass

        # Final summary
        console.print()
        if snap.succeeded:
            console.print(
                "[bold green]✔ Remote workflow completed successfully[/bold green]"
            )
        elif snap.failed:
            console.print(
                "[bold red]✖ Remote workflow FAILED[/bold red]"
            )
        else:
            console.print(
                f"[yellow]◌ Remote monitoring stopped[/yellow]  "
                f"(state: {snap.wf_state})"
            )
        console.print()

        # Clean up temp file
        try:
            self._local_path.unlink(missing_ok=True)
            Path(self._tmpdir).rmdir()
        except OSError:
            pass
