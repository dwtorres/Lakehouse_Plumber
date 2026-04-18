#!/usr/bin/env python3
"""Read-only Databricks verification helper for Lakehouse Plumber bundle projects.

This script intentionally requires explicit profile selection via ``--profile``
or ``DATABRICKS_CONFIG_PROFILE``. It does not mutate the workspace.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


def resolve_profile(explicit_profile: str | None) -> str:
    """Resolve the Databricks profile from CLI or environment."""
    profile = explicit_profile or os.getenv("DATABRICKS_CONFIG_PROFILE")
    if not profile:
        raise ValueError(
            "Databricks profile is required. Pass --profile or set "
            "DATABRICKS_CONFIG_PROFILE."
        )
    return profile


def discover_bundle_resources(project_root: Path) -> Dict[str, List[str]]:
    """Return local workflow and pipeline resource names from resources/lhp."""
    resources_dir = project_root / "resources" / "lhp"
    workflows: List[str] = []
    pipelines: List[str] = []
    if resources_dir.exists():
        workflows = sorted(path.stem for path in resources_dir.glob("*_workflow.yml"))
        pipelines = sorted(path.stem for path in resources_dir.glob("*.pipeline.yml"))
    return {"workflows": workflows, "pipelines": pipelines}


def _run_command(command: List[str], cwd: Path) -> Dict[str, Any]:
    """Run a command and capture structured output."""
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "command": command,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def run_verification(
    project_root: Path,
    profile: str,
    target: str,
    run_limit: int,
) -> Dict[str, Any]:
    """Run read-only Databricks verification commands and capture results."""
    resources = discover_bundle_resources(project_root)
    results: Dict[str, Any] = {
        "project_root": str(project_root),
        "profile": profile,
        "target": target,
        "resources": resources,
        "checks": {},
    }

    def run_named(name: str, command: List[str]) -> Dict[str, Any]:
        result = _run_command(command, project_root)
        results["checks"][name] = result
        return result

    base_bundle = ["databricks", "bundle"]
    base_jobs = ["databricks", "jobs"]
    base_pipelines = ["databricks", "pipelines"]

    run_named(
        "bundle_validate",
        base_bundle
        + ["validate", "--target", target, "--profile", profile, "--output", "json"],
    )
    run_named(
        "bundle_summary",
        base_bundle
        + ["summary", "--target", target, "--profile", profile, "--output", "json"],
    )
    run_named(
        "pipelines_inventory",
        base_pipelines
        + [
            "list-pipelines",
            "--profile",
            profile,
            "--output",
            "json",
            "--max-results",
            "100",
        ],
    )

    job_inspections: Dict[str, Any] = {}
    for workflow_name in resources["workflows"]:
        inventory = _run_command(
            base_jobs
            + [
                "list",
                "--profile",
                profile,
                "--output",
                "json",
                "--name",
                workflow_name,
                "--limit",
                "5",
            ],
            project_root,
        )
        job_record: Dict[str, Any] = {"inventory": inventory, "runs": []}
        if inventory["returncode"] == 0:
            try:
                payload = json.loads(inventory["stdout"] or "{}")
            except json.JSONDecodeError:
                payload = {}
            jobs = payload.get("jobs", [])
            if jobs:
                job_id = jobs[0].get("job_id")
                if job_id is not None:
                    list_runs = _run_command(
                        base_jobs
                        + [
                            "list-runs",
                            "--profile",
                            profile,
                            "--output",
                            "json",
                            "--job-id",
                            str(job_id),
                            "--limit",
                            str(run_limit),
                            "--expand-tasks",
                        ],
                        project_root,
                    )
                    job_record["runs"].append({"list_runs": list_runs})
                    if list_runs["returncode"] == 0:
                        try:
                            run_payload = json.loads(list_runs["stdout"] or "{}")
                        except json.JSONDecodeError:
                            run_payload = {}
                        for run in run_payload.get("runs", []):
                            run_id = run.get("run_id")
                            if run_id is None:
                                continue
                            job_record["runs"].append(
                                {
                                    "get_run": _run_command(
                                        base_jobs
                                        + [
                                            "get-run",
                                            str(run_id),
                                            "--profile",
                                            profile,
                                            "--output",
                                            "json",
                                        ],
                                        project_root,
                                    ),
                                    "get_run_output": _run_command(
                                        base_jobs
                                        + [
                                            "get-run-output",
                                            str(run_id),
                                            "--profile",
                                            profile,
                                            "--output",
                                            "json",
                                        ],
                                        project_root,
                                    ),
                                }
                            )
        job_inspections[workflow_name] = job_record
    results["checks"]["workflow_jobs"] = job_inspections
    return results


def _format_markdown(result: Dict[str, Any]) -> str:
    lines = [
        "# Databricks Bundle Verification",
        "",
        f"- project_root: {result['project_root']}",
        f"- profile: {result['profile']}",
        f"- target: {result['target']}",
        f"- workflows: {', '.join(result['resources']['workflows']) or '(none found)'}",
        f"- pipelines: {', '.join(result['resources']['pipelines']) or '(none found)'}",
        "",
    ]
    for name, payload in result["checks"].items():
        if name == "workflow_jobs":
            lines.append("## workflow_jobs")
            for workflow_name, workflow_payload in payload.items():
                lines.append(f"### {workflow_name}")
                inventory = workflow_payload["inventory"]
                lines.append(
                    f"- inventory rc={inventory['returncode']} cmd={' '.join(inventory['command'])}"
                )
                for entry in workflow_payload["runs"]:
                    if "list_runs" in entry:
                        list_runs = entry["list_runs"]
                        lines.append(
                            f"- list-runs rc={list_runs['returncode']} cmd={' '.join(list_runs['command'])}"
                        )
                    else:
                        get_run = entry["get_run"]
                        get_run_output = entry["get_run_output"]
                        lines.append(
                            f"- get-run rc={get_run['returncode']} cmd={' '.join(get_run['command'])}"
                        )
                        lines.append(
                            f"- get-run-output rc={get_run_output['returncode']} cmd={' '.join(get_run_output['command'])}"
                        )
            lines.append("")
            continue
        lines.append(f"## {name}")
        lines.append(f"- rc: {payload['returncode']}")
        lines.append(f"- cmd: {' '.join(payload['command'])}")
        if payload["stderr"].strip():
            lines.append(f"- stderr: {payload['stderr'].strip()}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run read-only Databricks bundle verification with an explicit profile."
        )
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile. Required unless DATABRICKS_CONFIG_PROFILE is set.",
    )
    parser.add_argument(
        "--target",
        default="dev",
        help="Bundle target to verify (default: dev).",
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Bundle project root (default: current directory).",
    )
    parser.add_argument(
        "--run-limit",
        type=int,
        default=3,
        help="Maximum recent runs to inspect per workflow job (default: 3).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of markdown.",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        profile = resolve_profile(args.profile)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    result = run_verification(
        project_root=Path(args.project_root).resolve(),
        profile=profile,
        target=args.target,
        run_limit=args.run_limit,
    )
    rendered = (
        json.dumps(result, indent=2, sort_keys=True)
        if args.json
        else _format_markdown(result)
    )
    sys.stdout.write(rendered)

    failed = [
        name
        for name, payload in result["checks"].items()
        if name != "workflow_jobs" and payload.get("returncode") != 0
    ]
    for workflow_payload in result["checks"].get("workflow_jobs", {}).values():
        inventory = workflow_payload["inventory"]
        if inventory.get("returncode") != 0:
            failed.append("workflow_jobs")
            break
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
