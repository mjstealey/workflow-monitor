"""Rich-based terminal dashboard for live workflow monitoring."""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
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
    fmt_memory,
    fmt_timestamp,
    real_exitcode,
)
from .diagnostics import collect_diagnostics
from .event_log import EventLogger
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

def _make_header(
    info: WorkflowInfo,
    snap: WorkflowSnapshot,
    refresh_ts: float,
    replay_info: Optional[dict] = None,
    remote_info: Optional[dict] = None,
) -> Panel:
    grid = Table.grid(expand=True, padding=(0, 0))
    grid.add_column(ratio=1)
    grid.add_column(justify="right", no_wrap=True)

    # Row 1: title + mode badge
    title = Text()
    title.append("Pegasus Workflow Monitor", style="bold white")
    if replay_info is not None:
        title.append("  ")
        title.append(" REPLAY ", style="bold white on dark_orange3")
        speed = replay_info.get("speed", 1.0)
        speed_str = f"{speed:g}x" if speed != int(speed) else f"{int(speed)}x"
        title.append(f" {speed_str}", style="bold dark_orange3")
    elif remote_info is not None:
        title.append("  ")
        title.append(" SSH ", style="bold white on blue")
        host = remote_info.get("host", "")
        if host:
            title.append(f" {host}", style="bold blue")
    else:
        title.append("  ")
        title.append(" LIVE ", style="bold white on green")

    right_col = Text(no_wrap=True)
    if replay_info is not None or remote_info is not None:
        right_col.append(
            datetime.fromtimestamp(refresh_ts).strftime("%H:%M:%S"), style="dim white"
        )
    else:
        right_col.append("Refreshed ", style="dim")
        right_col.append(
            datetime.fromtimestamp(refresh_ts).strftime("%H:%M:%S"), style="white"
        )

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

    grid.add_row(title, right_col)
    grid.add_row(details, version)
    return Panel(grid, style="on grey7", padding=(0, 1))


# ─── Status summary bar ───────────────────────────────────────────────────────

def _make_status_bar(snap: WorkflowSnapshot) -> Panel:
    done = snap.done_count()
    total = snap.total_jobs()
    pct = snap.progress_pct()
    elapsed = fmt_duration(snap.elapsed)
    failed = snap.failed_count()
    held = snap.held_count()
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
    if held:
        counts.append(f"Held:{held} ", style="bold magenta")
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
    table.add_column("Job", style="white", ratio=3, no_wrap=True)
    table.add_column("Type", style="dim", ratio=1, no_wrap=True)
    table.add_column("State", ratio=1, no_wrap=True)
    table.add_column("Exit", justify="right", no_wrap=True, ratio=0)
    table.add_column("Duration", justify="right", no_wrap=True, ratio=1)
    table.add_column("Args", style="dim", ratio=2, no_wrap=True, max_width=40)
    table.add_column("Mem", justify="right", style="dim", no_wrap=True, width=7)
    table.add_column("Live", style="dim", ratio=1, no_wrap=True)

    for job in jobs_to_show:
        state_style = STATE_STYLE.get(job.disp_state, "")
        state_cell = Text(job.disp_state, style=state_style)
        ec = real_exitcode(job.exitcode)
        exit_cell = (
            Text(str(ec), style="green" if ec == 0 else "red")
            if ec is not None
            else Text("-", style="dim")
        )
        dur_cell = Text(fmt_duration(job.duration), style="dim")

        # Task arguments (truncated)
        argv = job.task_argv or ""
        if len(argv) > 37:
            argv = argv[:37] + "..."
        argv_cell = Text(argv, style="dim")

        mem_cell = Text(fmt_memory(job.maxrss), style="dim")

        # Condor live info
        condor_info = condor_map.get(job.exec_job_id, {})
        if condor_info:
            live = format_job_status(condor_info.get("JobStatus"))
            live_cell = Text(live, style="cyan")
        else:
            live_cell = Text("", style="dim")

        type_label = JOB_TYPE_LABEL.get(job.type_desc, job.type_desc)

        table.add_row(
            job.display_name,
            type_label,
            state_cell,
            exit_cell,
            dur_cell,
            argv_cell,
            mem_cell,
            live_cell,
        )

    if not jobs_to_show:
        table.add_row(
            Text("(no jobs yet)", style="dim italic"),
            "", "", "", "", "", "", "",
        )

    title = "[bold]Compute Jobs[/bold]" if not show_all else "[bold]All Jobs[/bold]"
    return Panel(table, title=title, padding=(0, 0))


# ─── Recent events panel ──────────────────────────────────────────────────────

def _make_events_panel(snap: WorkflowSnapshot, n: int = 15) -> Panel:
    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Job", style="white", no_wrap=True, ratio=3)
    table.add_column("State", no_wrap=True, ratio=1)
    table.add_column("Start", style="dim", no_wrap=True, width=10)
    table.add_column("End", style="dim", no_wrap=True, width=10)
    table.add_column("Duration", justify="right", style="dim", no_wrap=True, width=9)
    table.add_column("Mem", justify="right", style="dim", no_wrap=True, width=7)

    # Show jobs sorted by most recent activity (end_time or start_time)
    active_jobs = [
        j for j in snap.jobs
        if j.raw_state is not None  # only jobs that have been submitted
    ]
    active_jobs.sort(
        key=lambda j: j.end_time or j.start_time or j.submit_time or 0,
        reverse=True,
    )

    for job in active_jobs[:n]:
        state_style = STATE_STYLE.get(job.disp_state, "dim")
        state_cell = Text(job.disp_state, style=state_style)
        start_cell = Text(
            fmt_timestamp(job.start_time or job.submit_time), style="dim"
        )
        end_cell = Text(
            fmt_timestamp(job.end_time) if job.end_time else "-", style="dim"
        )
        dur_cell = Text(fmt_duration(job.duration), style="dim")
        mem_cell = Text(fmt_memory(job.maxrss), style="dim")

        table.add_row(
            job.display_name,
            state_cell,
            start_cell,
            end_cell,
            dur_cell,
            mem_cell,
        )

    if not active_jobs:
        table.add_row(
            Text("(no activity yet)", style="dim italic"),
            "", "", "", "", "",
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

    return Panel(table, title="[bold]Auxiliary Jobs[/bold]", padding=(0, 0))


# ─── Diagnostics panel ────────────────────────────────────────────────────

def _make_diagnostics_panel(
    snap: WorkflowSnapshot,
    condor_jobs: Optional[List] = None,
    submit_dir: Optional[Path] = None,
) -> Panel:
    held = snap.held_jobs()
    failed = snap.failed_jobs()
    diags = collect_diagnostics(held, failed, condor_jobs, submit_dir=submit_dir)

    grid = Table.grid(expand=True, padding=(0, 1))
    grid.add_column(ratio=1)

    for diag in diags:
        severity_style = "bold magenta" if diag.severity == "held" else "bold red"
        icon = "⊘" if diag.severity == "held" else "✖"

        header = Text()
        header.append(f" {icon} ", style=severity_style)
        header.append(diag.job_name, style="bold white")
        header.append(f"  {diag.summary}", style=severity_style)
        grid.add_row(header)

        if diag.reason and diag.reason != f"Exit code: {None}":
            # When suggestions already carry structured findings (e.g. "Missing: ..."
            # from stderr analysis), skip the raw stderr dump to avoid noise.
            has_structured = any(
                s.startswith("Missing:") or s.startswith("Permission denied:")
                for s in diag.suggestions
            )
            reason_parts = diag.reason.split(" | ")
            for part in reason_parts:
                # Skip the huge raw stderr when structured findings cover it
                if has_structured and part.startswith("stderr:"):
                    continue
                reason_text = Text()
                reason_text.append("   ", style="dim")
                if part.startswith("stdout:"):
                    reason_text.append("stdout: ", style="dim")
                    msg = part[len("stdout:"):].strip()
                    if len(msg) > 120:
                        msg = msg[:117] + "..."
                    reason_text.append(msg, style="bold white")
                elif part.startswith("stderr:"):
                    reason_text.append("stderr: ", style="dim")
                    msg = part[len("stderr:"):].strip()
                    if len(msg) > 120:
                        msg = msg[:117] + "..."
                    reason_text.append(msg, style="bold yellow")
                elif part.startswith("executable:"):
                    reason_text.append("exec:   ", style="dim")
                    reason_text.append(part[len("executable:"):].strip(), style="dim white")
                else:
                    reason_text.append(part, style="dim white")
                grid.add_row(reason_text)

        for suggestion in diag.suggestions[:5]:
            sug_text = Text()
            if suggestion.startswith("Missing:"):
                sug_text.append("   ✗ ", style="bold red")
                sug_text.append(suggestion[len("Missing:"):].strip(), style="bold red")
            elif suggestion.startswith("Permission denied:"):
                sug_text.append("   ✗ ", style="bold red")
                sug_text.append(suggestion, style="bold red")
            else:
                sug_text.append("   → ", style="yellow")
                sug_text.append(suggestion, style="yellow")
            grid.add_row(sug_text)

        grid.add_row(Text(""))  # spacer between diagnostics

    if not diags:
        grid.add_row(Text("  No issues detected", style="dim green"))

    return Panel(
        grid,
        title="[bold yellow]Diagnostics[/bold yellow]",
        border_style="yellow",
        padding=(0, 0),
    )


# ─── Full layout assembly ─────────────────────────────────────────────────────

def build_layout(
    info: WorkflowInfo,
    snap: WorkflowSnapshot,
    show_all: bool,
    condor_jobs: Optional[List],
    events_n: int,
    refresh_ts: float,
    replay_info: Optional[dict] = None,
    remote_info: Optional[dict] = None,
    submit_dir: Optional[Path] = None,
) -> Layout:
    has_issues = snap.held_count() > 0 or snap.failed_count() > 0

    # Calculate diagnostics panel height based on number of issues
    diag_height = 0
    if has_issues:
        n_issues = snap.held_count() + snap.failed_count()
        # ~7 lines per issue (header + exitcode + stdout + stderr + exec + suggestions + spacer)
        diag_height = min(3 + n_issues * 7, 25)

    layout = Layout()
    parts = [
        Layout(name="header", size=4),
        Layout(name="status", size=5),
    ]
    if has_issues:
        parts.append(Layout(name="diagnostics", size=diag_height))
    parts.append(Layout(name="main"))
    parts.append(Layout(name="events", size=events_n + 3))
    layout.split_column(*parts)

    layout["main"].split_row(
        Layout(name="jobs", ratio=3),
        Layout(name="infra", ratio=1),
    )

    layout["header"].update(_make_header(info, snap, refresh_ts, replay_info=replay_info, remote_info=remote_info))
    layout["status"].update(_make_status_bar(snap))
    if has_issues:
        layout["diagnostics"].update(_make_diagnostics_panel(snap, condor_jobs=condor_jobs, submit_dir=submit_dir))
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
    log_path: Optional[Path] = None,
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
    log_path:          If set, write JSONL event log to this path.
    """
    ck = condor_kwargs or {}

    console = Console()

    logger: Optional[EventLogger] = None
    if log_path is not None:
        logger = EventLogger(info, db, log_path=log_path)
        if logger.resumed:
            console.print(f"[dim]Resuming event log at {logger.path}[/dim]")
        else:
            console.print(f"[dim]Logging events to {logger.path}[/dim]")

    def _poll_condor() -> List:
        try:
            return query_queue(constraint=condor_constraint, **ck)
        except Exception:
            return []

    def _refresh() -> tuple:
        snap = db.snapshot()
        condor_jobs = _poll_condor()
        ts = time.time()
        if logger is not None:
            logger.record(snap, condor_jobs)
        return snap, condor_jobs, ts

    if once:
        snap, condor_jobs, ts = _refresh()
        console.print(_make_header(info, snap, ts))
        console.print(_make_status_bar(snap))
        if snap.held_count() > 0 or snap.failed_count() > 0:
            console.print(_make_diagnostics_panel(snap, condor_jobs=condor_jobs, submit_dir=info.submit_dir))
        console.print(_make_job_table(snap, show_all=show_all, condor_jobs=condor_jobs))
        if snap.infra_jobs():
            console.print(_make_infra_summary(snap))
        console.print(_make_events_panel(snap, n=events_n))
        _print_final_summary(console, snap, condor_jobs=condor_jobs, submit_dir=info.submit_dir)
        if logger is not None:
            logger.close(snap)
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
                    info, snap, show_all, condor_jobs, events_n, ts,
                    submit_dir=info.submit_dir,
                )
                live.update(layout)

                if snap.is_complete and not snap.is_running:
                    # Give one extra beat for final DB flush from monitord
                    time.sleep(poll_interval)
                    snap, condor_jobs, ts = _refresh()
                    live.update(
                        build_layout(info, snap, show_all, condor_jobs, events_n, ts,
                                     submit_dir=info.submit_dir)
                    )
                    break

                time.sleep(poll_interval)

        except KeyboardInterrupt:
            pass

    # After live session ends, print a brief final summary
    _print_final_summary(console, snap, condor_jobs=condor_jobs, submit_dir=info.submit_dir)
    if logger is not None:
        logger.close(snap)


def _print_final_summary(
    console: Console,
    snap: WorkflowSnapshot,
    condor_jobs: Optional[List] = None,
    submit_dir: Optional[Path] = None,
) -> None:
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
            f"failed jobs: {snap.failed_count()}"
            f"{f', held jobs: {snap.held_count()}' if snap.held_count() else ''})"
        )
        # Print diagnostics summary
        held = snap.held_jobs()
        failed = snap.failed_jobs()
        if held or failed:
            diags = collect_diagnostics(held, failed, condor_jobs, submit_dir=submit_dir)
            for diag in diags:
                icon = "⊘" if diag.severity == "held" else "✖"
                style = "magenta" if diag.severity == "held" else "red"
                console.print(f"  [{style}]{icon} {diag.job_name}[/{style}]: {diag.summary}")
                has_structured = any(
                    s.startswith("Missing:") or s.startswith("Permission denied:")
                    for s in diag.suggestions
                )
                if diag.reason:
                    for part in diag.reason.split(" | "):
                        if has_structured and part.startswith("stderr:"):
                            continue
                        if part.startswith("stdout:"):
                            console.print(f"    [white]stdout: {part[7:].strip()}[/white]")
                        elif part.startswith("stderr:"):
                            msg = part[7:].strip()
                            if len(msg) > 200:
                                msg = msg[:197] + "..."
                            console.print(f"    [yellow]stderr: {msg}[/yellow]")
                        elif part.startswith("executable:"):
                            console.print(f"    [dim]exec: {part[11:].strip()}[/dim]")
                        else:
                            console.print(f"    [dim]{part}[/dim]")
                for sug in diag.suggestions[:4]:
                    if sug.startswith("Missing:"):
                        console.print(f"    [bold red]✗ {sug[8:].strip()}[/bold red]")
                    elif sug.startswith("Permission denied:"):
                        console.print(f"    [bold red]✗ {sug}[/bold red]")
                    else:
                        console.print(f"    [yellow]→ {sug}[/yellow]")
    else:
        held = snap.held_count()
        msg = f"[yellow]◌ Monitoring stopped[/yellow]  (state: {snap.wf_state}"
        if held:
            msg += f", held: {held}"
        msg += ")"
        console.print(msg)
        if held:
            diags = collect_diagnostics(snap.held_jobs(), [], condor_jobs, submit_dir=submit_dir)
            for diag in diags:
                console.print(f"  [magenta]⊘ {diag.job_name}[/magenta]: {diag.summary}")
                for sug in diag.suggestions[:2]:
                    console.print(f"    [yellow]→ {sug}[/yellow]")
    console.print()
