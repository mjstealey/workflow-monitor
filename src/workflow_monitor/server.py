"""Headless server daemon for workflow-monitor.

Runs the monitoring loop without a terminal UI, writing JSONL event logs
continuously.  Designed to survive terminal disconnection via daemonization
(double-fork on Unix).

Usage (from cli.py):
    workflow-monitor --serve [--log PATH] TARGET
"""
from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from .braindump import WorkflowInfo
from .db import StampedeDB, WorkflowSnapshot, fmt_duration
from .event_log import EventLogger
from .htcondor_poll import query_queue


def _daemonize(pid_file: Path) -> None:
    """Classic double-fork to fully detach from the controlling terminal."""
    # First fork
    pid = os.fork()
    if pid > 0:
        # Parent: print info and exit
        print(f"Server started (PID {pid}), logging in background.")
        print(f"PID file: {pid_file}")
        sys.exit(0)

    # First child — become session leader
    os.setsid()

    # Second fork — prevent re-acquiring a terminal
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Grandchild — the actual daemon
    # Redirect standard file descriptors to /dev/null
    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    os.close(devnull)

    # Write PID file
    pid_file.write_text(str(os.getpid()))


def _cleanup_pid(pid_file: Path) -> None:
    try:
        pid_file.unlink(missing_ok=True)
    except OSError:
        pass


def run_server(
    info: WorkflowInfo,
    db: StampedeDB,
    poll_interval: float = 2.0,
    log_path: Optional[Path] = None,
    condor_kwargs: Optional[dict] = None,
    condor_constraint: Optional[str] = None,
    foreground: bool = False,
) -> None:
    """Run the headless monitoring server.

    Parameters
    ----------
    info:              WorkflowInfo from braindump.yml.
    db:                Connected StampedeDB instance.
    poll_interval:     Seconds between stampede.db refreshes.
    log_path:          Path to write JSONL event log.
    condor_kwargs:     Extra kwargs forwarded to htcondor_poll.query_queue().
    condor_constraint: Optional HTCondor ClassAd constraint for live queue.
    foreground:        If True, run in foreground (don't daemonize).
    """
    ck = condor_kwargs or {}

    if log_path is None:
        log_path = info.submit_dir / "workflow-events.jsonl"

    pid_file = log_path.with_suffix(".pid")

    if not foreground:
        # Print info before daemonizing (stdout still connected)
        print(f"Logging events to: {log_path}")
        _daemonize(pid_file)
    else:
        pid_file.write_text(str(os.getpid()))
        print(f"Server running in foreground (PID {os.getpid()})")
        print(f"Logging events to: {log_path}")
        print(f"PID file: {pid_file}")
        print("Press Ctrl+C to stop.")

    # Set up signal handling for graceful shutdown
    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger = EventLogger(info, db, log_path=log_path)

    def _poll_condor():
        try:
            return query_queue(constraint=condor_constraint, **ck)
        except Exception:
            return []

    snap: Optional[WorkflowSnapshot] = None

    try:
        while not shutdown:
            snap = db.snapshot()
            condor_jobs = _poll_condor()
            logger.record(snap, condor_jobs)

            if snap.is_complete and not snap.is_running:
                # One more poll for final DB flush
                time.sleep(poll_interval)
                snap = db.snapshot()
                condor_jobs = _poll_condor()
                logger.record(snap, condor_jobs)
                break

            time.sleep(poll_interval)
    finally:
        if snap is not None:
            logger.close(snap)
        else:
            logger.close()
        _cleanup_pid(pid_file)


def stop_server(pid_file: Path) -> bool:
    """Stop a running server by sending SIGTERM to the PID in the file."""
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal.SIGTERM)
        return True
    except (ValueError, OSError):
        return False
