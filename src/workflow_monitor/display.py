"""Rich-based terminal dashboard for live workflow monitoring."""
from __future__ import annotations

import time
from datetime import datetime
from typing import List, Optional

from rich import box
from rich.align import Align
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .braindump import WorkflowInfo
from .db import (
    STATE_STYLE,
    JOB_TYPE_LABEL,
    StampedeDB,
    WorkflowSnapshot,
    fmt_duration,
    fmt_timestamp,
)
from .htcondor_poll import query_queue, format_job_status


# ─── Workflow state styling ───────────────────────────────────────────────────

def _wf_state_text(snap: WorkflowSnapshot) -> Text:
    if snap.is_running:
        return Text("● RUNNING", style="bold cyan")
    if snap.succeeded:
        return Text("✔ SUCCESS", style="bold green")
    if snap.failed:
        return Text("✖ FAILED", style="bold red")
    return Text(f"◌ {snap.wf_state}", style="dim")


# ─── Header panel ─────────────────────────────────────────────────────────────

def _make_header(info: WorkflowInfo, snap: WorkflowSnapshot, refresh_ts: float) -> Panel:
    grid = Table.grid(expand=True, padding=(0, 0))
    grid.add_column(ratio=1)
    grid.add_column(justify="right", no_wrap=True)

    # Row 1: title + refresh time
    title = Text()
    title.append("Pegasus Workflow Monitor", style="bold white")
    refresh = Text(no_wrap=True)
    refresh.append("Refreshed ", style="dim")
    refresh.append(datetime.fromtimestamp(refresh_ts).strftime("%H:%M:%S"), style="white")

    # Row 2: workflow details
    details = Text()
    details.append("workflow: ", style="dim")
    details.append(info.dax_label, style="bold yellow")
    details.append("  uuid: ", style="dim")
    details.append(info.wf_uuid[:8] + "…", style="cyan")
    details.append("  user: ", style="dim")
    details.append(info.user, style="cyan")

    version = Text(no_wrap=True)
    version.append("pegasus ", style="dim")
    version.append(info.planner_version, style="dim")

    grid.add_row(title, refresh)
    grid.add_row(details, version)
    return Panel(grid, style="on grey7", padding=(0, 1))


# ─── Status summary bar ───────────────────────────────────────────────────────

def _make_status_bar(snap: WorkflowSnapshot) -> Panel:
    done = snap.done_count()
    total = snap.total_jobs()
    pct = snap.progress_pct()
    elapsed = fmt_duration(snap.elapsed)
    failed = snap.failed_count()
    running = snap.running_count()
    queued = snap.queued_count()
    unsubmitted = sum(
        1 for j in snap.jobs if j.disp_state == "UNSUBMITTED"
    )

    # Progress bar (manual Rich progress widget would need a separate task;
    # we render a simple bar using block characters instead)
    bar_width = 40
    filled = int(bar_width * pct / 100)
    bar_text = Text()
    bar_text.append("[", style="dim")
    bar_text.append("█" * filled, style="bold green")
    if failed > 0:
        fail_filled = max(1, int(bar_width * failed / total)) if total else 0
        bar_text.append("█" * min(fail_filled, bar_width - filled), style="bold red")
        bar_text.append("░" * max(0, bar_width - filled - fail_filled), style="dim")
    else:
        bar_text.append("░" * (bar_width - filled), style="dim")
    bar_text.append("]", style="dim")

    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(no_wrap=True)
    grid.add_column(no_wrap=True)
    grid.add_column(no_wrap=True)
    grid.add_column(no_wrap=True, ratio=1)

    state_cell = _wf_state_text(snap)
    elapsed_cell = Text(f"Elapsed: {elapsed}", style="dim white")
    pct_cell = Text(f"{pct:.1f}%  {done}/{total} done", style="white")

    counts = Text()
    counts.append(f"Done:{done} ", style="green")
    counts.append(f"Run:{running} ", style="cyan")
    counts.append(f"Queued:{queued} ", style="yellow")
    if unsubmitted:
        counts.append(f"Wait:{unsubmitted} ", style="dim")
    if failed:
        counts.append(f"Fail:{failed}", style="bold red")

    grid.add_row(state_cell, elapsed_cell, pct_cell, counts)

    progress_row = Table.grid(expand=True)
    progress_row.add_column(ratio=1)
    progress_row.add_column(no_wrap=True)
    progress_row.add_row(bar_text, Text())

    combined = Table.grid(expand=True)
    combined.add_column()
    combined.add_row(grid)
    combined.add_row(progress_row)

    return Panel(combined, title="[bold]Workflow Status[/bold]", padding=(0, 1))


# ─── Job details table ────────────────────────────────────────────────────────

def _make_job_table(
    snap: WorkflowSnapshot,
    show_all: bool = False,
    condor_jobs: Optional[List] = None,
) -> Panel:
    # Build a condor lookup by DAGNodeName -> status
    condor_map: dict = {}
    if condor_jobs:
        for cj in condor_jobs:
            node = cj.get("DAGNodeName", "")
            if node:
                condor_map[node] = cj

    jobs_to_show = snap.jobs if show_all else snap.compute_jobs()

    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Job Name", style="white", ratio=3, no_wrap=True)
    table.add_column("Type", style="dim", ratio=1, no_wrap=True)
    table.add_column("State", ratio=1, no_wrap=True)
    table.add_column("Exit", justify="right", no_wrap=True, ratio=0)
    table.add_column("Duration", justify="right", no_wrap=True, ratio=1)
    table.add_column("Live (Condor)", style="dim", ratio=1, no_wrap=True)

    for job in jobs_to_show:
        state_style = STATE_STYLE.get(job.disp_state, "")
        state_cell = Text(job.disp_state, style=state_style)
        exit_cell = (
            Text(str(job.exitcode), style="green" if job.exitcode == 0 else "red")
            if job.exitcode is not None
            else Text("-", style="dim")
        )
        dur_cell = Text(fmt_duration(job.duration), style="dim")

        # Condor live info
        condor_info = condor_map.get(job.exec_job_id, {})
        if condor_info:
            live = format_job_status(condor_info.get("JobStatus"))
            live_cell = Text(live, style="cyan")
        else:
            live_cell = Text("", style="dim")

        type_label = JOB_TYPE_LABEL.get(job.type_desc, job.type_desc)

        table.add_row(
            job.short_name,
            type_label,
            state_cell,
            exit_cell,
            dur_cell,
            live_cell,
        )

    if not jobs_to_show:
        table.add_row(
            Text("(no jobs yet)", style="dim italic"),
            "", "", "", "", "",
        )

    title = "[bold]Compute Jobs[/bold]" if not show_all else "[bold]All Jobs[/bold]"
    return Panel(table, title=title, padding=(0, 0))


# ─── Recent events panel ──────────────────────────────────────────────────────

def _make_events_panel(snap: WorkflowSnapshot, n: int = 15) -> Panel:
    table = Table(
        box=None,
        show_header=False,
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Time", style="dim", no_wrap=True, width=10)
    table.add_column("Job", no_wrap=True, ratio=3)
    table.add_column("State", no_wrap=True, ratio=2)

    events = snap.recent_events[:n]
    for ev in events:
        ts = fmt_timestamp(ev.get("timestamp"))
        raw = ev.get("state", "")
        style = STATE_STYLE.get(raw, "dim")
        job_name = ev.get("exec_job_id", "")
        table.add_row(
            ts,
            Text(job_name, style="dim white", no_wrap=True),
            Text(raw, style=style),
        )

    return Panel(table, title="[bold]Recent Events[/bold]", padding=(0, 0))


# ─── Infrastructure summary (compact) ────────────────────────────────────────

def _make_infra_summary(snap: WorkflowSnapshot) -> Panel:
    infra = snap.infra_jobs()
    counts: dict = {}
    for job in infra:
        t = JOB_TYPE_LABEL.get(job.type_desc, job.type_desc)
        s = job.disp_state
        key = (t, s)
        counts[key] = counts.get(key, 0) + 1

    table = Table(box=None, show_header=False, padding=(0, 1))
    table.add_column("Type", style="dim")
    table.add_column("State", style="dim")
    table.add_column("Count", justify="right", style="dim")

    for (t, s), count in sorted(counts.items()):
        style = STATE_STYLE.get(s, "dim")
        table.add_row(t, Text(s, style=style), str(count))

    if not counts:
        table.add_row("(none)", "", "")

    return Panel(table, title="[bold]Infrastructure[/bold]", padding=(0, 0))


# ─── Full layout assembly ─────────────────────────────────────────────────────

def build_layout(
    info: WorkflowInfo,
    snap: WorkflowSnapshot,
    show_all: bool,
    condor_jobs: Optional[List],
    events_n: int,
    refresh_ts: float,
) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="status", size=5),
        Layout(name="main"),
        Layout(name="events", size=events_n + 3),
    )
    layout["main"].split_row(
        Layout(name="jobs", ratio=3),
        Layout(name="infra", ratio=1),
    )

    layout["header"].update(_make_header(info, snap, refresh_ts))
    layout["status"].update(_make_status_bar(snap))
    layout["jobs"].update(_make_job_table(snap, show_all=show_all, condor_jobs=condor_jobs))
    layout["infra"].update(_make_infra_summary(snap))
    layout["events"].update(_make_events_panel(snap, n=events_n))

    return layout


# ─── Monitor loop ─────────────────────────────────────────────────────────────

def run_monitor(
    info: WorkflowInfo,
    db: StampedeDB,
    poll_interval: float = 2.0,
    show_all: bool = False,
    events_n: int = 15,
    condor_constraint: Optional[str] = None,
    condor_kwargs: Optional[dict] = None,
    once: bool = False,
) -> None:
    """Run the live terminal dashboard.

    Parameters
    ----------
    info:              WorkflowInfo from braindump.yml.
    db:                Connected StampedeDB instance.
    poll_interval:     Seconds between stampede.db refreshes.
    show_all:          When True, show all job types not just compute.
    events_n:          Number of recent events to show.
    condor_constraint: Optional HTCondor ClassAd constraint for live queue.
    condor_kwargs:     Extra kwargs forwarded to htcondor_poll.query_queue().
    once:              Print status once and exit (non-interactive).
    """
    ck = condor_kwargs or {}

    console = Console()

    def _poll_condor() -> List:
        try:
            return query_queue(constraint=condor_constraint, **ck)
        except Exception:
            return []

    def _refresh() -> tuple:
        snap = db.snapshot()
        condor_jobs = _poll_condor()
        ts = time.time()
        return snap, condor_jobs, ts

    if once:
        snap, condor_jobs, ts = _refresh()
        console.print(_make_header(info, snap, ts))
        console.print(_make_status_bar(snap))
        console.print(_make_job_table(snap, show_all=show_all, condor_jobs=condor_jobs))
        if snap.infra_jobs():
            console.print(_make_infra_summary(snap))
        console.print(_make_events_panel(snap, n=events_n))
        _print_final_summary(console, snap)
        return

    with Live(
        console=console,
        screen=True,
        refresh_per_second=2,
        redirect_stderr=False,
    ) as live:
        try:
            while True:
                snap, condor_jobs, ts = _refresh()
                layout = build_layout(
                    info, snap, show_all, condor_jobs, events_n, ts
                )
                live.update(layout)

                if snap.is_complete and not snap.is_running:
                    # Give one extra beat for final DB flush from monitord
                    time.sleep(poll_interval)
                    snap, condor_jobs, ts = _refresh()
                    live.update(
                        build_layout(info, snap, show_all, condor_jobs, events_n, ts)
                    )
                    break

                time.sleep(poll_interval)

        except KeyboardInterrupt:
            pass

    # After live session ends, print a brief final summary
    _print_final_summary(console, snap)


def _print_final_summary(console: Console, snap: WorkflowSnapshot) -> None:
    console.print()
    if snap.succeeded:
        console.print(
            f"[bold green]✔ Workflow completed successfully[/bold green]  "
            f"(elapsed: {fmt_duration(snap.elapsed)})"
        )
    elif snap.failed:
        console.print(
            f"[bold red]✖ Workflow FAILED[/bold red]  "
            f"(elapsed: {fmt_duration(snap.elapsed)}, "
            f"failed jobs: {snap.failed_count()})"
        )
    else:
        console.print(
            f"[yellow]◌ Monitoring stopped[/yellow]  "
            f"(state: {snap.wf_state})"
        )
    console.print()
