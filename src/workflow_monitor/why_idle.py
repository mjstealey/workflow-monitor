"""One-shot diagnostic: explain why workflow jobs are idle.

Combines data from:
  1. stampede.db — which jobs are idle/queued
  2. condor_status — pool capacity vs. job requirements
  3. condor_userprio — user's fair-share priority standing
  4. condor_status -negotiator — negotiation cycle timing (optional)

Prints a human-readable diagnosis and exits.
"""
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .db import WorkflowSnapshot, fmt_duration


# ─── Data structures ─────────────────────────────────────────────────────────

@dataclass
class IdleDiagnosis:
    """Complete diagnosis for why jobs are idle."""
    idle_jobs: List[Dict[str, Any]] = field(default_factory=list)
    pool_available: bool = False
    pool_total_cpus: int = 0
    pool_idle_cpus: int = 0
    pool_total_memory_mb: int = 0
    pool_idle_memory_mb: int = 0
    pool_total_gpus: int = 0
    pool_idle_gpus: int = 0
    pool_machines: int = 0
    pool_total_slots: int = 0
    pool_idle_slots: int = 0
    user_priority: Optional[Dict[str, Any]] = None
    other_users: List[Dict[str, Any]] = field(default_factory=list)
    negotiation_cycle_seconds: Optional[float] = None
    negotiation_matches: Optional[int] = None
    requirement_mismatches: List[str] = field(default_factory=list)
    findings: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)


# ─── condor_userprio ─────────────────────────────────────────────────────────

def _query_userprio(
    collector_host: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query condor_userprio for all users' priority information."""
    cmd = ["condor_userprio", "-long"]
    if collector_host:
        cmd += ["-pool", collector_host]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []

    # Parse the -long ClassAd output: blocks separated by blank lines
    users: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            if current:
                users.append(current)
                current = {}
            continue
        if " = " in line:
            key, _, val = line.partition(" = ")
            key = key.strip()
            val = val.strip().strip('"')
            # Try numeric conversion
            try:
                if "." in val:
                    current[key] = float(val)
                else:
                    current[key] = int(val)
            except ValueError:
                current[key] = val
    if current:
        users.append(current)

    return users


# ─── condor_status -negotiator ───────────────────────────────────────────────

def _query_negotiator(
    collector_host: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Query negotiator for cycle timing information."""
    attrs = [
        "LastNegotiationCycleDuration0",
        "LastNegotiationCycleMatches0",
        "LastNegotiationCycleDuration1",
        "LastNegotiationCycleMatches1",
    ]
    cmd = [
        "condor_status", "-negotiator", "-json",
        "-attributes", ",".join(attrs),
    ]
    if collector_host:
        cmd += ["-pool", collector_host]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        data = json.loads(result.stdout)
        if isinstance(data, list) and data:
            return data[0]
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return None


# ─── Idle job details from condor_q ──────────────────────────────────────────

def _get_idle_job_details(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query condor_q for idle job details (requirements, resource requests)."""
    attrs = [
        "ClusterId", "ProcId", "DAGNodeName", "JobStatus",
        "RequestCpus", "RequestMemory", "RequestDisk", "RequestGpus",
        "Requirements", "QDate", "HoldReason",
    ]
    idle_constraint = "JobStatus == 1"
    if constraint:
        idle_constraint = f"({constraint}) && JobStatus == 1"

    cmd = [
        "condor_q", "-json",
        "-attributes", ",".join(attrs),
        "-constraint", idle_constraint,
    ]
    if schedd_name:
        cmd += ["-name", schedd_name]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else []
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return []


# ─── Analysis engine ─────────────────────────────────────────────────────────

def _analyze(
    snapshot: WorkflowSnapshot,
    idle_condor_jobs: List[Dict[str, Any]],
    pool_summary: Any,  # Optional[PoolSummary]
    user_prio: List[Dict[str, Any]],
    negotiator: Optional[Dict[str, Any]],
    current_user: Optional[str] = None,
) -> IdleDiagnosis:
    """Analyze all data sources and produce a diagnosis."""
    diag = IdleDiagnosis()

    # ── Identify idle jobs from the workflow ──────────────────────────────
    for job in snapshot.jobs:
        if job.disp_state in ("QUEUED", "UNSUBMITTED"):
            diag.idle_jobs.append({
                "name": job.display_name,
                "exec_id": job.exec_job_id,
                "state": job.disp_state,
                "type": job.type_desc,
            })

    if not diag.idle_jobs:
        diag.findings.append("No idle or queued jobs found in this workflow.")
        return diag

    queued = [j for j in diag.idle_jobs if j["state"] == "QUEUED"]
    unsubmitted = [j for j in diag.idle_jobs if j["state"] == "UNSUBMITTED"]

    if unsubmitted:
        diag.findings.append(
            f"{len(unsubmitted)} job(s) are UNSUBMITTED — waiting on "
            f"upstream dependencies in the DAG."
        )

    if not queued:
        diag.findings.append(
            "All waiting jobs are UNSUBMITTED (DAG dependencies). "
            "No jobs are idle in the HTCondor queue."
        )
        diag.suggestions.append(
            "Unsubmitted jobs will be released by DAGMan as their parent "
            "jobs complete. No action needed."
        )
        return diag

    diag.findings.append(
        f"{len(queued)} job(s) are QUEUED in HTCondor (submitted but not yet matched to a slot)."
    )

    # ── Pool capacity check ──────────────────────────────────────────────
    if pool_summary is not None:
        diag.pool_available = True
        diag.pool_total_cpus = pool_summary.total_cpus
        diag.pool_idle_cpus = pool_summary.idle_cpus
        diag.pool_total_memory_mb = pool_summary.total_memory_mb
        diag.pool_idle_memory_mb = pool_summary.idle_memory_mb
        diag.pool_total_gpus = pool_summary.total_gpus
        diag.pool_idle_gpus = pool_summary.idle_gpus
        diag.pool_machines = pool_summary.machines
        diag.pool_total_slots = pool_summary.total_slots
        diag.pool_idle_slots = pool_summary.idle_slots

        if pool_summary.idle_cpus == 0 and pool_summary.total_cpus > 0:
            diag.findings.append(
                f"Pool is fully occupied: {pool_summary.total_cpus} CPUs across "
                f"{pool_summary.machines} machine(s), 0 idle."
            )
            diag.suggestions.append(
                "Wait for running jobs to finish, or add more resources to the pool."
            )
        elif pool_summary.idle_cpus > 0:
            diag.findings.append(
                f"Pool has idle capacity: {pool_summary.idle_cpus}/{pool_summary.total_cpus} "
                f"CPUs idle, {pool_summary.idle_memory_mb}MB memory available."
            )

        # ── Requirement mismatch check ───────────────────────────────────
        for cj in idle_condor_jobs:
            node = cj.get("DAGNodeName", f"cluster {cj.get('ClusterId', '?')}")
            req_cpus = int(cj.get("RequestCpus", 1))
            req_mem = int(cj.get("RequestMemory", 0))
            req_gpus = int(cj.get("RequestGpus", 0))

            mismatches = []
            if req_cpus > pool_summary.idle_cpus and pool_summary.idle_cpus >= 0:
                mismatches.append(
                    f"needs {req_cpus} CPUs but only {pool_summary.idle_cpus} idle"
                )
            if req_mem > pool_summary.idle_memory_mb and pool_summary.idle_memory_mb >= 0:
                mismatches.append(
                    f"needs {req_mem}MB RAM but only {pool_summary.idle_memory_mb}MB idle"
                )
            if req_gpus > 0 and req_gpus > pool_summary.idle_gpus:
                mismatches.append(
                    f"needs {req_gpus} GPU(s) but only {pool_summary.idle_gpus} idle"
                )

            if mismatches:
                diag.requirement_mismatches.append(
                    f"{node}: {'; '.join(mismatches)}"
                )

        if diag.requirement_mismatches:
            diag.findings.append(
                f"{len(diag.requirement_mismatches)} job(s) have resource requirements "
                f"that exceed available pool capacity."
            )
            diag.suggestions.append(
                "Reduce resource requests (RequestCpus, RequestMemory, RequestGpus) "
                "in the transformation catalog, or add larger machines to the pool."
            )
    else:
        diag.findings.append(
            "Could not query pool status (condor_status unavailable). "
            "Cannot check resource availability."
        )

    # ── User priority check ──────────────────────────────────────────────
    if user_prio:
        # Find current user's entry
        if current_user is None:
            current_user = os.environ.get("USER", "")

        my_entry = None
        for entry in user_prio:
            name = entry.get("Name", "")
            # condor_userprio names are user@domain or user@uid_domain
            if name.startswith(f"{current_user}@"):
                my_entry = entry
                break

        if my_entry:
            eff_prio = my_entry.get("EffectivePriority", my_entry.get("Priority"))
            real_prio = my_entry.get("RealPriority", my_entry.get("PriorityFactor"))
            resources_used = my_entry.get("ResourcesUsed", my_entry.get("WeightedResourcesUsed", 0))
            diag.user_priority = {
                "name": my_entry.get("Name", current_user),
                "effective_priority": eff_prio,
                "real_priority": real_prio,
                "resources_used": resources_used,
            }

            # Collect other users for comparison
            for entry in user_prio:
                name = entry.get("Name", "")
                if name and not name.startswith(f"{current_user}@"):
                    other_eff = entry.get("EffectivePriority", entry.get("Priority"))
                    other_used = entry.get("ResourcesUsed", entry.get("WeightedResourcesUsed", 0))
                    if other_eff is not None:
                        diag.other_users.append({
                            "name": name,
                            "effective_priority": other_eff,
                            "resources_used": other_used,
                        })

            # Higher priority number = lower priority in HTCondor
            if eff_prio is not None and diag.other_users:
                higher_prio_users = [
                    u for u in diag.other_users
                    if u["effective_priority"] is not None
                    and u["effective_priority"] < eff_prio
                ]
                if higher_prio_users:
                    diag.findings.append(
                        f"Your effective priority is {eff_prio:.2f} — "
                        f"{len(higher_prio_users)} user(s) have better (lower) priority "
                        f"and will be matched first."
                    )
                    diag.suggestions.append(
                        "Fair-share priority improves as you use fewer resources over time. "
                        "Consider submitting fewer concurrent jobs or adjusting your "
                        "accounting group."
                    )
                else:
                    diag.findings.append(
                        f"Your effective priority is {eff_prio:.2f} — "
                        f"you have the best (lowest) priority among active users."
                    )
        else:
            diag.findings.append(
                f"No priority entry found for user '{current_user}'. "
                f"You may not have submitted jobs yet, or the pool uses a "
                f"different accounting scheme."
            )
    else:
        diag.findings.append(
            "Could not query user priorities (condor_userprio unavailable)."
        )

    # ── Negotiation cycle check ──────────────────────────────────────────
    if negotiator:
        duration = negotiator.get("LastNegotiationCycleDuration0")
        matches = negotiator.get("LastNegotiationCycleMatches0")
        if duration is not None:
            diag.negotiation_cycle_seconds = float(duration)
            diag.findings.append(
                f"Last negotiation cycle took {float(duration):.1f}s."
            )
            if float(duration) > 60:
                diag.suggestions.append(
                    "Negotiation cycles over 60s indicate the negotiator is "
                    "overloaded or the pool is very large. Contact your pool admin."
                )
        if matches is not None:
            diag.negotiation_matches = int(matches)
            if int(matches) == 0:
                diag.findings.append(
                    "Last negotiation cycle produced 0 matches — no jobs were "
                    "matched to slots."
                )
                diag.suggestions.append(
                    "Zero matches with idle jobs suggests a requirements mismatch, "
                    "insufficient resources, or a negotiator configuration issue."
                )
            else:
                diag.findings.append(
                    f"Last negotiation cycle matched {int(matches)} job(s) to slots."
                )

    # ── Summary suggestions if none yet ──────────────────────────────────
    if not diag.suggestions:
        diag.suggestions.append(
            "Jobs may be waiting for the next negotiation cycle (typically "
            "every 60-300 seconds). If idle time persists, check resource "
            "requirements and pool capacity."
        )

    return diag


# ─── Rich output ─────────────────────────────────────────────────────────────

def _render(diag: IdleDiagnosis, console: Console, workflow_label: str) -> None:
    """Render the diagnosis to the console using Rich."""
    console.print()
    console.print(
        Panel(
            f"[bold]Why-Idle Diagnostic[/bold]  —  workflow: [cyan]{workflow_label}[/cyan]",
            style="blue",
        )
    )

    if not diag.idle_jobs:
        console.print("[green]No idle or queued jobs found.[/green] All jobs are either running, completed, or failed.")
        return

    # ── Idle job summary ─────────────────────────────────────────────────
    table = Table(title="Idle/Queued Jobs", show_header=True, show_lines=False)
    table.add_column("Job", style="cyan", no_wrap=True, max_width=40)
    table.add_column("State", style="yellow")
    table.add_column("Type")
    for j in diag.idle_jobs:
        state_style = "yellow" if j["state"] == "QUEUED" else "dim"
        table.add_row(j["name"], f"[{state_style}]{j['state']}[/]", j["type"])
    console.print(table)

    # ── Pool capacity ────────────────────────────────────────────────────
    if diag.pool_available:
        pool_table = Table(title="Pool Resources", show_header=True, show_lines=False)
        pool_table.add_column("Resource", style="bold")
        pool_table.add_column("Total", justify="right")
        pool_table.add_column("Idle", justify="right")
        pool_table.add_row("Machines", str(diag.pool_machines), "-")
        pool_table.add_row("Slots", str(diag.pool_total_slots), str(diag.pool_idle_slots))
        pool_table.add_row("CPUs", str(diag.pool_total_cpus), str(diag.pool_idle_cpus))
        mem_total = f"{diag.pool_total_memory_mb / 1024:.1f}G" if diag.pool_total_memory_mb >= 1024 else f"{diag.pool_total_memory_mb}M"
        mem_idle = f"{diag.pool_idle_memory_mb / 1024:.1f}G" if diag.pool_idle_memory_mb >= 1024 else f"{diag.pool_idle_memory_mb}M"
        pool_table.add_row("Memory", mem_total, mem_idle)
        if diag.pool_total_gpus > 0:
            pool_table.add_row("GPUs", str(diag.pool_total_gpus), str(diag.pool_idle_gpus))
        console.print(pool_table)

    # ── Requirement mismatches ───────────────────────────────────────────
    if diag.requirement_mismatches:
        console.print()
        console.print("[bold red]Resource Requirement Mismatches:[/bold red]")
        for mismatch in diag.requirement_mismatches:
            console.print(f"  [red]![/red] {mismatch}")

    # ── User priority ────────────────────────────────────────────────────
    if diag.user_priority:
        console.print()
        prio_table = Table(title="User Priority (Fair-Share)", show_header=True, show_lines=False)
        prio_table.add_column("User", style="cyan")
        prio_table.add_column("Effective Priority", justify="right")
        prio_table.add_column("Resources Used", justify="right")

        # Current user first (highlighted)
        eff = diag.user_priority.get("effective_priority")
        used = diag.user_priority.get("resources_used", 0)
        prio_table.add_row(
            f"[bold]{diag.user_priority['name']}[/bold] (you)",
            f"[bold]{eff:.2f}[/bold]" if eff is not None else "-",
            f"{used:.1f}" if isinstance(used, float) else str(used),
        )

        # Other users (sorted by priority — lower is better)
        sorted_others = sorted(
            diag.other_users,
            key=lambda u: u.get("effective_priority") or float("inf"),
        )
        for u in sorted_others[:5]:  # Show top 5
            u_eff = u.get("effective_priority")
            u_used = u.get("resources_used", 0)
            prio_table.add_row(
                u["name"],
                f"{u_eff:.2f}" if u_eff is not None else "-",
                f"{u_used:.1f}" if isinstance(u_used, float) else str(u_used),
            )
        if len(sorted_others) > 5:
            prio_table.add_row(f"... and {len(sorted_others) - 5} more", "", "")

        console.print(prio_table)
        console.print(
            "[dim]Lower effective priority = matched first. "
            "Priority increases (worsens) with resource usage.[/dim]"
        )

    # ── Negotiation cycle ────────────────────────────────────────────────
    if diag.negotiation_cycle_seconds is not None:
        console.print()
        cycle_text = f"Last negotiation cycle: [bold]{diag.negotiation_cycle_seconds:.1f}s[/bold]"
        if diag.negotiation_matches is not None:
            cycle_text += f"  |  Matches: [bold]{diag.negotiation_matches}[/bold]"
        console.print(cycle_text)

    # ── Findings ─────────────────────────────────────────────────────────
    console.print()
    console.print(Panel("[bold]Findings[/bold]", style="yellow"))
    for i, finding in enumerate(diag.findings, 1):
        console.print(f"  {i}. {finding}")

    # ── Suggestions ──────────────────────────────────────────────────────
    if diag.suggestions:
        console.print()
        console.print(Panel("[bold]Suggestions[/bold]", style="green"))
        for i, suggestion in enumerate(diag.suggestions, 1):
            console.print(f"  {i}. {suggestion}")

    console.print()


# ─── Public entry point ──────────────────────────────────────────────────────

def run_why_idle(
    info: Any,  # WorkflowInfo from braindump
    snapshot: WorkflowSnapshot,
    condor_constraint: Optional[str] = None,
    condor_kwargs: Optional[Dict[str, Any]] = None,
) -> int:
    """Run the --why-idle one-shot diagnostic and print results.

    Returns 0 on success, 1 on error.
    """
    from .htcondor_poll import query_slots

    condor_kwargs = condor_kwargs or {}
    console = Console()

    # 1. Pool capacity
    pool = query_slots(
        collector_host=condor_kwargs.get("collector_host"),
        token_path=condor_kwargs.get("token_path"),
        cert_path=condor_kwargs.get("cert_path"),
        key_path=condor_kwargs.get("key_path"),
        password_file=condor_kwargs.get("password_file"),
    )

    # 2. Idle job details from condor_q
    idle_condor_jobs = _get_idle_job_details(
        constraint=condor_constraint,
        schedd_name=condor_kwargs.get("schedd_name"),
    )

    # 3. User priority
    user_prio = _query_userprio(
        collector_host=condor_kwargs.get("collector_host"),
    )

    # 4. Negotiator cycle timing
    negotiator = _query_negotiator(
        collector_host=condor_kwargs.get("collector_host"),
    )

    # 5. Analyze
    diag = _analyze(
        snapshot=snapshot,
        idle_condor_jobs=idle_condor_jobs,
        pool_summary=pool,
        user_prio=user_prio,
        negotiator=negotiator,
        current_user=info.user,
    )

    # 6. Render
    _render(diag, console, info.dax_label or "unknown")

    return 0
