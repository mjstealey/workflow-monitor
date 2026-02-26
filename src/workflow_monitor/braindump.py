"""Parse Pegasus braindump.yml to locate workflow artifacts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class WorkflowInfo:
    wf_uuid: str
    root_wf_uuid: str
    dax_label: str
    submit_dir: Path
    user: str
    planner_version: str
    dag_file: str
    condor_log: str
    timestamp: str
    basedir: Path

    @property
    def stampede_db(self) -> Optional[Path]:
        """Path to the stampede SQLite database (written by pegasus-monitord)."""
        stem = self.dag_file.replace(".dag", "")
        p = self.submit_dir / f"{stem}.stampede.db"
        return p if p.exists() else None

    @property
    def jobstate_log(self) -> Optional[Path]:
        """Path to the jobstate.log file."""
        p = self.submit_dir / "jobstate.log"
        return p if p.exists() else None

    @property
    def condor_log_path(self) -> Optional[Path]:
        """Path to the HTCondor event log."""
        p = self.submit_dir / self.condor_log
        return p if p.exists() else None

    @property
    def dag_path(self) -> Path:
        return self.submit_dir / self.dag_file

    @property
    def dagman_out_path(self) -> Optional[Path]:
        p = self.submit_dir / f"{self.dag_file}.dagman.out"
        return p if p.exists() else None


def find_braindump(path: Path) -> Optional[Path]:
    """Locate braindump.yml from various starting points.

    Accepts:
    - A braindump.yml file directly
    - A run directory containing braindump.yml
    - A workflow base directory (returns the highest-numbered run)
    """
    path = path.resolve()

    if path.is_file() and path.name == "braindump.yml":
        return path

    direct = path / "braindump.yml"
    if direct.exists():
        return direct

    # Search recursively for braindump.yml files and pick the last (latest run)
    candidates = sorted(path.rglob("braindump.yml"))
    if candidates:
        return candidates[-1]

    return None


def load_braindump(path: Path) -> WorkflowInfo:
    """Load a braindump.yml and return a WorkflowInfo."""
    bd_path = find_braindump(path)
    if bd_path is None:
        raise FileNotFoundError(
            f"Cannot find braindump.yml at or under: {path}\n"
            "Ensure the workflow has been planned with pegasus-plan."
        )

    with open(bd_path) as f:
        data = yaml.safe_load(f)

    submit_dir = Path(data["submit_dir"])
    basedir = Path(data.get("basedir", submit_dir.parent))

    return WorkflowInfo(
        wf_uuid=data.get("wf_uuid", ""),
        root_wf_uuid=data.get("root_wf_uuid", ""),
        dax_label=data.get("dax_label", ""),
        submit_dir=submit_dir,
        user=data.get("user", ""),
        planner_version=data.get("planner_version", ""),
        dag_file=data.get("dag", ""),
        condor_log=data.get("condor_log", ""),
        timestamp=data.get("timestamp", ""),
        basedir=basedir,
    )
