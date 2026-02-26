# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Scope

This project lives in `/Users/stealey/GitHub/pegasusai/workflow-monitor`. All work should remain within this directory and its subdirectories. Do not read from or modify files outside of this directory tree without explicit user instruction.

## Reference Files

Before writing code or making recommendations for tasks involving Pegasus or HTCondor, read the relevant reference file(s) below. These contain distilled documentation, API signatures, and configuration patterns that are more reliable than training knowledge alone.

### Pegasus WMS — `/Users/stealey/claude/references/pegasus.md`

Covers Pegasus 5.1.2: Python API (`Pegasus.api`), the three catalogs (RC, TC, SC), YAML formats, data configuration modes, container support, CLI tools, `pegasus.properties`, and deployment scenarios.

**Consult for:** workflow definition, planning (`pegasus-plan`), submission, monitoring, and debugging.

### HTCondor — `/Users/stealey/claude/references/htcondor.md`

Covers HTCondor 24.x/25.x: submit file syntax, job universes, file transfer, DAGMan, Python bindings (`htcondor` package), ClassAds, CLI tools, and configuration.

**Consult for:** job submission, queue management, DAGMan authoring, Python bindings, pool administration, and matchmaking/scheduling issues.
