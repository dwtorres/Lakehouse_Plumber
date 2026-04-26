"""LakehousePlumber CLI - Main entry point."""

import logging
import sys
import warnings
from pathlib import Path
from typing import List, Optional

import click

from .error_boundary import cli_error_boundary

# Import for dynamic version detection
try:
    from importlib.metadata import version
except ImportError:
    # Fallback for older Python versions where importlib.metadata is not available
    try:
        from importlib_metadata import version  # type: ignore
    except Exception:  # pragma: no cover - best-effort fallback

        def version(package: str) -> str:  # type: ignore
            return "0.0.0"


def get_version():
    """Get the package version dynamically from package metadata."""
    try:
        # Try to get version from installed package metadata
        return version("lakehouse-plumber")
    except Exception:
        try:
            # Fallback: try reading from pyproject.toml (for development)
            import re
            from pathlib import Path

            # Find pyproject.toml - look up the directory tree
            current_dir = Path(__file__).parent
            for _ in range(5):  # Look up to 5 levels
                pyproject_path = current_dir / "pyproject.toml"
                if pyproject_path.exists():
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                    # Use regex to find version = "x.y.z"
                    version_match = re.search(
                        r'version\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if version_match:
                        return version_match.group(1)
                    break
                current_dir = current_dir.parent
        except Exception as e:
            logging.getLogger(__name__).debug(
                f"Could not read version from pyproject.toml: {e}"
            )

        # Final fallback
        return "0.2.11"


def configure_logging(verbose: bool, project_root: Optional[Path] = None):
    """Configure logging for LakehousePlumber.

    Returns:
        str: Path to log file if created, None otherwise
    """
    # Clear any existing handlers from previous runs
    cleanup_logging()

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING if not verbose else logging.DEBUG)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler (only if project root available)
    log_file_path = None
    if project_root:
        log_dir = project_root / ".lhp" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "lhp.log"

        file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    warnings.filterwarnings("default", category=DeprecationWarning, module=r"lhp\.")

    return str(log_file_path) if log_file_path else None


def cleanup_logging():
    """Clean up logging handlers."""
    root_logger = logging.getLogger()
    # Remove all existing handlers
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for lhp.yaml."""
    current = Path.cwd().resolve()

    # Check current directory and parent directories
    for path in [current] + list(current.parents):
        if (path / "lhp.yaml").exists():
            return path

    return None


def _ensure_project_root() -> Path:
    """Find project root or raise LHPError."""
    from ..utils.error_formatter import ErrorCategory, LHPError

    project_root = _find_project_root()
    if not project_root:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="011",
            title="Not in a LakehousePlumber project directory",
            details="No lhp.yaml file found in the current directory or any parent.",
            suggestions=[
                "Run 'lhp init <project_name>' to create a new project",
                "Navigate to an existing project directory",
            ],
        )

    return project_root


def _load_project_config(project_root: Path) -> dict:
    """Load project configuration from lhp.yaml."""
    import yaml  # noqa: F401

    config_file = project_root / "lhp.yaml"
    if not config_file.exists():
        return {}

    from ..utils.yaml_loader import safe_load_yaml_with_fallback

    return safe_load_yaml_with_fallback(
        config_file,
        fallback_value={},
        error_context="project configuration",
        log_errors=True,
    )


def _get_include_patterns(project_root: Path) -> List[str]:
    """Get include patterns from project configuration."""
    try:
        from ..core.project_config_loader import ProjectConfigLoader

        config_loader = ProjectConfigLoader(project_root)
        project_config = config_loader.load_project_config()

        if project_config and project_config.include:
            return project_config.include
        else:
            return []
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"Could not load project config for include patterns: {e}"
        )
        return []


def _discover_yaml_files_with_include(
    pipelines_dir: Path, include_patterns: List[str] = None
) -> List[Path]:
    """Discover YAML files with optional include pattern filtering."""
    if include_patterns:
        from ..utils.file_pattern_matcher import discover_files_with_patterns

        return discover_files_with_patterns(pipelines_dir, include_patterns)
    else:
        yaml_files = []
        yaml_files.extend(pipelines_dir.rglob("*.yaml"))
        yaml_files.extend(pipelines_dir.rglob("*.yml"))
        return yaml_files


# ============================================================================
# CLI Command Group and Routing
# ============================================================================


@click.group()
@click.version_option(version=get_version(), prog_name="lhp")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--perf", is_flag=True, hidden=True)
def cli(verbose, perf):
    """LakehousePlumber - Generate Lakeflow pipelines from YAML configs."""
    # Try to find project root for better logging setup
    project_root = _find_project_root()
    log_file = configure_logging(verbose, project_root)

    # Store logging info in context for subcommands
    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["log_file"] = log_file
    ctx.obj["perf"] = perf

    if perf:
        from ..utils.performance_timer import enable_perf_timing

        enable_perf_timing(project_root)


# ============================================================================
# Command Routing - Delegate to Command Classes
# ============================================================================


@cli.command()
@click.argument("project_name")
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Skip Databricks Asset Bundle setup (bundle is enabled by default)",
)
@cli_error_boundary("Project initialization")
def init(project_name, no_bundle):
    """Initialize a new LakehousePlumber project in the current directory.

    PROJECT_NAME is used for template rendering (e.g. bundle name, lhp.yaml).
    All files are created in the current working directory.
    """
    from .commands.init_command import InitCommand

    InitCommand().execute(project_name, bundle=not no_bundle)


@cli.command()
@click.option("--env", "-e", required=True, help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to generate")
@click.option("--output", "-o", help="Output directory (defaults to generated/{env})")
@click.option("--dry-run", is_flag=True, help="Preview without generating files")
@click.option(
    "--no-cleanup",
    is_flag=True,
    help="Disable cleanup of generated files when source YAML files are removed.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force regeneration of all files, even if unchanged",
)
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Disable bundle support even if databricks.yml exists",
)
@click.option(
    "--include-tests",
    is_flag=True,
    default=False,
    help="Include test actions in generation (skipped by default for faster builds)",
)
@click.option(
    "--pipeline-config",
    "-pc",
    help="Custom pipeline config file path (relative to project root)",
)
@cli_error_boundary("Code generation")
def generate(
    env,
    pipeline,
    output,
    dry_run,
    no_cleanup,
    force,
    no_bundle,
    include_tests,
    pipeline_config,
):
    """Generate DLT pipeline code"""
    from .commands.generate_command import GenerateCommand

    GenerateCommand().execute(
        env,
        pipeline,
        output,
        dry_run,
        no_cleanup,
        force,
        no_bundle,
        include_tests,
        pipeline_config,
    )


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to validate")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option(
    "--include-tests",
    is_flag=True,
    default=False,
    help="Include test actions in validation (matches generate behavior)",
)
@cli_error_boundary("Pipeline validation")
def validate(env, pipeline, verbose, include_tests):
    """Validate pipeline configurations"""
    from .commands.validate_command import ValidateCommand

    ValidateCommand().execute(env, pipeline, verbose, include_tests)


@cli.command()
@click.option("--env", "-e", help="Environment to show state for")
@click.option("--pipeline", "-p", help="Specific pipeline to show state for")
@click.option("--orphaned", is_flag=True, help="Show only orphaned files")
@click.option("--stale", is_flag=True, help="Show only stale files (YAML changed)")
@click.option("--new", is_flag=True, help="Show only new/untracked YAML files")
@click.option(
    "--dry-run", is_flag=True, help="Preview cleanup without actually deleting files"
)
@click.option("--cleanup", is_flag=True, help="Clean up orphaned files")
@click.option("--regen", is_flag=True, help="Regenerate stale files")
@cli_error_boundary("State management")
def state(env, pipeline, orphaned, stale, new, dry_run, cleanup, regen):
    """Show or manage the current state of generated files."""
    from .commands.state_command import StateCommand

    StateCommand().execute(env, pipeline, orphaned, stale, new, dry_run, cleanup, regen)


@cli.command()
@click.option("--pipeline", "-p", help="Specific pipeline to analyze")
@cli_error_boundary("Pipeline statistics")
def stats(pipeline):
    """Display pipeline statistics and complexity metrics."""
    from .commands.stats_command import StatsCommand

    StatsCommand().execute(pipeline)


@cli.command()
@cli_error_boundary("List presets")
def list_presets():
    """List available presets"""
    from .commands.list_commands import ListCommand

    ListCommand().list_presets()


@cli.command()
@cli_error_boundary("List templates")
def list_templates():
    """List available templates"""
    from .commands.list_commands import ListCommand

    ListCommand().list_templates()


@cli.command()
@click.argument("flowgroup")
@click.option("--env", "-e", default="dev", help="Environment")
@cli_error_boundary("Show flowgroup")
def show(flowgroup, env):
    """Show resolved configuration for a flowgroup in table format"""
    from .commands.show_command import ShowCommand

    ShowCommand().show_flowgroup(flowgroup, env)


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
@cli_error_boundary("Show substitutions")
def substitutions(env):
    """Show available substitution tokens for an environment"""
    from .commands.show_command import ShowCommand

    ShowCommand().show_substitutions(env)


@cli.command()
@cli_error_boundary("Project info")
def info():
    """Display project information and statistics."""
    from .commands.show_command import ShowCommand

    ShowCommand().show_project_info()


@cli.command()
@click.option(
    "--format",
    "-f",
    type=click.Choice(["dot", "json", "text", "job", "all"], case_sensitive=False),
    default="all",
    help="Output format(s) to generate (dot=GraphViz, json=structured data, text=readable report, job=orchestration job)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output directory (defaults to .lhp/dependencies/)",
)
@click.option("--pipeline", "-p", help="Analyze specific pipeline only")
@click.option(
    "--job-name",
    "-j",
    help="Custom name for generated orchestration job (only used with job format)",
)
@click.option(
    "--job-config",
    "-jc",
    help="Custom job config file path (relative to project root, defaults to templates/bundle/job_config.yaml)",
)
@click.option(
    "--bundle-output",
    "-b",
    is_flag=True,
    help="Save job file to resources/ directory for Databricks bundle integration",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@cli_error_boundary("Dependency analysis")
def deps(format, output, pipeline, job_name, job_config, bundle_output, verbose):
    """Analyze and visualize pipeline dependencies for orchestration planning."""
    from .commands.dependencies_command import DependenciesCommand

    DependenciesCommand().execute(
        format, output, pipeline, job_name, job_config, bundle_output, verbose
    )


@cli.command("sync-runtime")
@click.option(
    "--dest",
    "-d",
    default=None,
    help="Destination directory. Defaults to current working directory; "
    "the package lands at <dest>/lhp_watermark.",
)
@click.option(
    "--check",
    is_flag=True,
    help="Report whether destination matches installed lhp_watermark (exit 1 on drift). No files written.",
)
@cli_error_boundary("Sync lhp_watermark runtime")
def sync_runtime(dest, check):
    """Vendor the installed lhp_watermark package into a user bundle (ADR-002 Path 5 Option A)."""
    from .commands.sync_runtime_command import SyncRuntimeCommand

    SyncRuntimeCommand().execute(dest=dest, check=check)


@cli.command("seed-load-group")
@click.option(
    "--env",
    "-e",
    required=True,
    help="Environment name (must have a corresponding substitutions/<env>.yaml).",
)
@click.option(
    "--flowgroup",
    "-f",
    required=True,
    help="Path to the flowgroup YAML to seed (relative or absolute). "
    "Falls back to a recursive name-match under pipelines/.",
)
@click.option(
    "--apply",
    is_flag=True,
    default=False,
    help="Execute the INSERTs via the Databricks SQL Statement Execution "
    "API (requires databricks-sdk). Default is dry-run (SQL printed only).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=True,
    help="Print SQL to stdout without executing (default).",
)
@click.option(
    "--catalog",
    default=None,
    help="Override watermark registry catalog (default: ${watermark_catalog} "
    "from substitutions, falling back to 'metadata' per ADR-004).",
)
@click.option(
    "--schema",
    default=None,
    help="Override watermark registry schema (default: ${watermark_schema} "
    "from substitutions, falling back to '<env>_orchestration').",
)
@click.option(
    "--warehouse-id",
    default=None,
    help="Databricks SQL warehouse ID for --apply (or set "
    "DATABRICKS_WAREHOUSE_ID env var). Ignored on dry-run.",
)
@cli_error_boundary("Seed load_group SQL")
def seed_load_group(env, flowgroup, apply, dry_run, catalog, schema, warehouse_id):
    """Emit Step 4a SELECT-preview + INSERT-seed SQL for a Tier 2 flowgroup migration."""
    from .commands.seed_load_group_command import SeedLoadGroupCommand

    SeedLoadGroupCommand().execute(
        env=env,
        flowgroup=flowgroup,
        apply=apply,
        dry_run=dry_run,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
    )


@cli.command("init-registry")
@click.option(
    "--env",
    "-e",
    default=None,
    help="Environment name (resolves catalog/schema from substitutions/<env>.yaml). "
    "Mutually exclusive with --catalog/--schema for the standard path.",
)
@click.option(
    "--catalog",
    default=None,
    help="Override watermark registry catalog (rehearsal / B13 bypass).",
)
@click.option(
    "--schema",
    default=None,
    help="Override watermark registry schema (rehearsal / B13 bypass).",
)
@click.option(
    "--warehouse-id",
    default=None,
    help="Databricks SQL warehouse ID (or set DATABRICKS_WAREHOUSE_ID env var).",
)
@click.option(
    "--profile",
    default=None,
    help="Databricks CLI profile (default: dbc-8e058692-373e per project memory).",
)
@click.option(
    "--max-wait-seconds",
    type=int,
    default=600,
    help="Hard cap on per-statement poll duration (default 600s).",
)
@cli_error_boundary("Init Tier 2 watermark registry")
def init_registry(env, catalog, schema, warehouse_id, profile, max_wait_seconds):
    """Trigger Tier 2 auto-DDL on the watermark registry via SQL Statement API.

    Replaces the fragile pipeline-run trigger + workspace-cluster fallback
    notebook from plan v5 §Phase 2.2 (B4, H10). Issues the same conditional
    CREATE / ALTER SQL that WatermarkManager._ensure_table_exists would run,
    but without a Spark session.
    """
    from .commands.init_registry_command import InitRegistryCommand

    InitRegistryCommand().execute(
        env=env,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
        profile=profile,
        max_wait_seconds=max_wait_seconds,
    )


@cli.command("validate-tier2")
@click.option(
    "--env",
    "-e",
    default=None,
    help="Environment name (resolves catalog/schema from substitutions/<env>.yaml).",
)
@click.option(
    "--catalog",
    default=None,
    help="Override watermark registry catalog (rehearsal / B13 bypass).",
)
@click.option(
    "--schema",
    default=None,
    help="Override watermark registry schema (rehearsal / B13 bypass).",
)
@click.option(
    "--probe-schema",
    default=None,
    help="Probe-table schema (default: <catalog>.<env>_validation per plan H3). "
    "Format: <catalog>.<schema>.",
)
@click.option(
    "--probe-table-name",
    default="watermarks_v1_probe",
    help="Probe-table bare name (default: watermarks_v1_probe).",
)
@click.option(
    "--cluster-id",
    default=None,
    help="Existing cluster id to run the notebook on (or set DATABRICKS_CLUSTER_ID).",
)
@click.option(
    "--lhp-workspace-path",
    default=None,
    help="Workspace path containing vendored lhp_watermark/ (per ADR-002 + "
    "`lhp sync-runtime`). Notebook prepends this to sys.path before importing. "
    "Or set LHP_WORKSPACE_PATH env var. Example: "
    "/Workspace/Users/<principal>/.bundle/<bundle>/<env>/files",
)
@click.option(
    "--workspace-target",
    default=None,
    help="Workspace path for the imported notebook "
    "(default: /Shared/lhp_validation/validate_tier2_load_group).",
)
@click.option(
    "--profile",
    default=None,
    help="Databricks CLI profile (default: dbc-8e058692-373e).",
)
@click.option(
    "--max-wait-seconds",
    type=int,
    default=1800,
    help="Hard cap on notebook-run poll duration (default 1800s).",
)
@click.option(
    "--poll-interval-seconds",
    type=int,
    default=10,
    help="Poll interval for run state (default 10s).",
)
@cli_error_boundary("Validate Tier 2 V1-V5")
def validate_tier2(
    env,
    catalog,
    schema,
    probe_schema,
    probe_table_name,
    cluster_id,
    lhp_workspace_path,
    workspace_target,
    profile,
    max_wait_seconds,
    poll_interval_seconds,
):
    """Run V1-V5 validation against a Tier 2 watermark registry.

    Workspace-imports scripts/validation/validate_tier2_load_group.py,
    submits as a one-off Jobs run with widget params bound, polls until
    terminal, parses the exit JSON. Exits 0 on PASS; non-zero with per-V
    diagnostic on FAIL. Replaces plan v5 §Phase 5.4 manual notebook upload.
    """
    from .commands.validate_tier2_command import (
        DEFAULT_WORKSPACE_TARGET,
        ValidateTier2Command,
    )

    ValidateTier2Command().execute(
        env=env,
        catalog=catalog,
        schema=schema,
        probe_schema=probe_schema,
        probe_table_name=probe_table_name,
        cluster_id=cluster_id,
        lhp_workspace_path=lhp_workspace_path,
        workspace_target=workspace_target or DEFAULT_WORKSPACE_TARGET,
        profile=profile,
        max_wait_seconds=max_wait_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


@cli.group("tier2-rollout")
def tier2_rollout():
    """Tier 2 rollout umbrella: preflight | backfill | optimize | rehearse | seed.

    Per plan v5 §Workstream B (S5/S8). Use these subcommands to drive the
    operator runbook phases via Python (uses databricks-sdk; replaces bash
    polling helpers).
    """


@tier2_rollout.command("preflight")
@click.option("--env", "-e", default=None)
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
@click.option("--warehouse-id", default=None)
@click.option("--profile", default=None)
@click.option("--max-wait-seconds", type=int, default=600)
@cli_error_boundary("tier2-rollout preflight")
def tier2_rollout_preflight(
    env, catalog, schema, warehouse_id, profile, max_wait_seconds
):
    """§Phase 1.A + H23 protocol gate (read-only)."""
    from .commands.tier2_rollout_command import Tier2RolloutCommand

    Tier2RolloutCommand().preflight(
        env=env,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
        profile=profile,
        max_wait_seconds=max_wait_seconds,
    )


@tier2_rollout.command("backfill")
@click.option("--env", "-e", default=None)
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
@click.option("--warehouse-id", default=None)
@click.option("--profile", default=None)
@click.option("--max-wait-seconds", type=int, default=1800)
@click.option(
    "--skip-pre-check",
    is_flag=True,
    default=False,
    help="Skip B6 pre-check (use only for re-runs after pre-check has passed).",
)
@cli_error_boundary("tier2-rollout backfill")
def tier2_rollout_backfill(
    env, catalog, schema, warehouse_id, profile, max_wait_seconds, skip_pre_check
):
    """§Phase 3: backfill load_group='legacy' WHERE NULL."""
    from .commands.tier2_rollout_command import Tier2RolloutCommand

    Tier2RolloutCommand().backfill(
        env=env,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
        profile=profile,
        max_wait_seconds=max_wait_seconds,
        skip_pre_check=skip_pre_check,
    )


@tier2_rollout.command("optimize")
@click.option("--env", "-e", default=None)
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
@click.option("--warehouse-id", default=None)
@click.option("--profile", default=None)
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Use OPTIMIZE FULL (required first run post-clustering-change per B5).",
)
@click.option("--max-wait-seconds", type=int, default=7200)
@cli_error_boundary("tier2-rollout optimize")
def tier2_rollout_optimize(
    env, catalog, schema, warehouse_id, profile, full, max_wait_seconds
):
    """§Phase 4: OPTIMIZE [FULL] watermark registry."""
    from .commands.tier2_rollout_command import Tier2RolloutCommand

    Tier2RolloutCommand().optimize(
        env=env,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
        profile=profile,
        full=full,
        max_wait_seconds=max_wait_seconds,
    )


@tier2_rollout.command("rehearse")
@click.option(
    "--source-table",
    required=True,
    help="Live registry FQN (e.g. metadata.prod_orchestration.watermarks).",
)
@click.option(
    "--target-schema",
    required=True,
    help="Rehearsal-clone schema FQN (e.g. metadata.prod_dryrun_orchestration). "
    "Clone is created at <target_schema>.watermarks (table name MUST be 'watermarks' per L17).",
)
@click.option("--warehouse-id", default=None)
@click.option("--profile", default=None)
@click.option("--max-wait-seconds", type=int, default=7200)
@click.option(
    "--skip-optimize",
    is_flag=True,
    default=False,
    help="Skip OPTIMIZE FULL on the rehearsal clone (faster, less realistic).",
)
@cli_error_boundary("tier2-rollout rehearse")
def tier2_rollout_rehearse(
    source_table,
    target_schema,
    warehouse_id,
    profile,
    max_wait_seconds,
    skip_optimize,
):
    """§Phase 8.1: dress rehearsal — version-pinned deep clone + Tier 2 phases.

    Does NOT bundle-deploy; does NOT exercise concurrent writers (H21).
    """
    from .commands.tier2_rollout_command import Tier2RolloutCommand

    Tier2RolloutCommand().rehearse(
        source_table=source_table,
        target_schema=target_schema,
        warehouse_id=warehouse_id,
        profile=profile,
        max_wait_seconds=max_wait_seconds,
        skip_optimize=skip_optimize,
    )


@tier2_rollout.command("seed")
@click.option("--env", "-e", required=True)
@click.option("--flowgroup", "-f", required=True)
@click.option("--apply", is_flag=True, default=False)
@click.option("--dry-run", is_flag=True, default=True)
@click.option("--catalog", default=None)
@click.option("--schema", default=None)
@click.option("--warehouse-id", default=None)
@cli_error_boundary("tier2-rollout seed")
def tier2_rollout_seed(env, flowgroup, apply, dry_run, catalog, schema, warehouse_id):
    """§Phase 6 wrapper around `lhp seed-load-group` for umbrella parity.

    Note (M9 unresolved): this delegates to the existing seed-load-group
    command which uses ``wait_timeout=30s`` without polling. For long-running
    apply paths, prefer running seed-load-group directly with explicit
    timeout management until M9 is resolved in the seed CLI.
    """
    from .commands.seed_load_group_command import SeedLoadGroupCommand

    SeedLoadGroupCommand().execute(
        env=env,
        flowgroup=flowgroup,
        apply=apply,
        dry_run=dry_run,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
    )


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    cli()
