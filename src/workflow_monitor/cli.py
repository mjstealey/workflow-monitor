"""Command-line entry point for workflow-monitor."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import __version__
from .braindump import load_braindump
from .db import StampedeDB
from .display import run_monitor


DESCRIPTION = """\
Real-time monitor for a running Pegasus WMS workflow.

TARGET may be:
  • A submit directory  (contains braindump.yml)
  • A workflow base dir (latest run is discovered automatically)
  • A braindump.yml file directly

The monitor reads the stampede SQLite database written by pegasus-monitord
and optionally queries the live HTCondor queue for additional job detail.
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="workflow-monitor",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument(
        "target",
        nargs="?",
        default=".",
        metavar="TARGET",
        help="Workflow submit dir, base dir, or braindump.yml (default: cwd)",
    )
    p.add_argument(
        "--version", "-V",
        action="version",
        version=f"workflow-monitor {__version__}",
    )
    p.add_argument(
        "--interval", "-i",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help="Refresh interval in seconds (default: 2.0)",
    )
    p.add_argument(
        "--all-jobs", "-a",
        action="store_true",
        default=False,
        help="Show all job types, not just compute jobs",
    )
    p.add_argument(
        "--events", "-e",
        type=int,
        default=15,
        metavar="N",
        help="Number of recent events to display (default: 15)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        default=False,
        help="Print current status once and exit (non-interactive)",
    )

    # ── HTCondor options ─────────────────────────────────────────────────────
    condor = p.add_argument_group("HTCondor options")
    condor.add_argument(
        "--schedd",
        metavar="NAME",
        help="Query a specific condor_schedd by name",
    )
    condor.add_argument(
        "--collector",
        metavar="HOST[:PORT]",
        help="Collector host for remote pool queries",
    )

    # ── Credential options ───────────────────────────────────────────────────
    creds = p.add_argument_group(
        "Credential options",
        "Credentials are never required for local pools with default security settings.",
    )
    creds.add_argument(
        "--token",
        metavar="PATH",
        help="Path to an HTCondor IDTOKEN file or directory",
    )
    creds.add_argument(
        "--cert",
        metavar="PATH",
        help="Path to an X.509 / GSI certificate file",
    )
    creds.add_argument(
        "--key",
        metavar="PATH",
        help="Path to an X.509 / GSI private key file",
    )
    creds.add_argument(
        "--password-file",
        metavar="PATH",
        help="Path to an HTCondor password file",
    )

    return p


def main(argv: list | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    target = Path(args.target)

    # ── Locate workflow ───────────────────────────────────────────────────────
    try:
        info = load_braindump(target)
    except FileNotFoundError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    db_path = info.stampede_db
    if db_path is None:
        print(
            f"[error] Stampede database not found in: {info.submit_dir}\n"
            "  Make sure pegasus-monitord is running (it is started automatically\n"
            "  by pegasus-run / pegasus-plan --submit).",
            file=sys.stderr,
        )
        return 1

    # ── Build condor kwargs ───────────────────────────────────────────────────
    condor_kwargs: dict = {}
    if args.schedd:
        condor_kwargs["schedd_name"] = args.schedd
    if args.collector:
        condor_kwargs["collector_host"] = args.collector
    if args.token:
        condor_kwargs["token_path"] = args.token
    if args.cert:
        condor_kwargs["cert_path"] = args.cert
    if args.key:
        condor_kwargs["key_path"] = args.key
    if getattr(args, "password_file", None):
        condor_kwargs["password_file"] = args.password_file

    # ── Run monitor ───────────────────────────────────────────────────────────
    with StampedeDB(db_path) as db:
        run_monitor(
            info=info,
            db=db,
            poll_interval=args.interval,
            show_all=args.all_jobs,
            events_n=args.events,
            condor_kwargs=condor_kwargs,
            once=args.once,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
