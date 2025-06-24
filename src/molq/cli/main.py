#!/usr/bin/env python3
"""
Molq CLI - Command Line Interface for job submission.

A Click-based CLI for submitting jobs to various schedulers.
"""

import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict

import click
import yaml

from molq.submitor.local import LocalSubmitor
from molq.submitor.slurm import SlurmSubmitor


class MolqConfig:
    """Configuration manager for Molq CLI."""

    def __init__(self):
        self.config_dir = Path.home() / ".molq"
        self.config_file = self.config_dir / "config.yaml"
        self.config_dir.mkdir(exist_ok=True)
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        if self.config_file.exists():
            try:
                with open(self.config_file, "r") as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                pass
        return {}

    def _save_config(self):
        """Save configuration to file."""
        try:
            with open(self.config_file, "w") as f:
                yaml.dump(self._config, f, default_flow_style=False)
        except Exception as e:
            click.echo(f"Error: Failed to save config: {e}", err=True)

    def get(self, key: str, default=None):
        """Get configuration value."""
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def set(self, key: str, value: Any):
        """Set configuration value."""
        keys = key.split(".")
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
        self._save_config()

    def show(self) -> Dict[str, Any]:
        """Show all configuration."""
        return self._config


# Global configuration instance
molq_config = MolqConfig()


def create_submitter(scheduler: str, **kwargs):
    """Create a submitter instance for the given scheduler."""
    if scheduler == "local":
        return LocalSubmitor("cli_local")
    elif scheduler == "slurm":
        host = kwargs.get("host") or molq_config.get("slurm.host", "local")
        username = kwargs.get("username") or molq_config.get(
            "slurm.username", os.getenv("USER")
        )
        cluster_config = {"host": host, "username": username}
        return SlurmSubmitor("cli_slurm", cluster_config)
    else:
        raise ValueError(f"Unsupported scheduler: {scheduler}")


@click.group()
@click.version_option()
def cli():
    """Molq - Modern Job Queue CLI."""
    pass


@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.argument("scheduler", type=click.Choice(["local", "slurm"]))
@click.argument("command_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--cpu-count", "--n-cpus", type=int, help="Number of CPU cores")
@click.option("--memory", "--mem", help="Memory requirement (e.g., 8G, 512M)")
@click.option("--time", "--time-limit", help="Time limit (e.g., 2h, 30m, 1d)")
@click.option("--queue", "--partition", help="Queue/partition name")
@click.option("--gpu-count", "--n-gpus", type=int, help="Number of GPUs")
@click.option("--gpu-type", help="GPU type (e.g., V100, A100)")
@click.option("--job-name", help="Job name")
@click.option("--workdir", help="Working directory")
@click.option("--email", help="Email for notifications")
@click.option("--priority", help="Job priority (low, normal, high)")
@click.option("--account", help="Account to charge resources to")
@click.option("--block/--no-block", default=False, help="Wait for job completion")
@click.option("--cleanup/--no-cleanup", default=True, help="Clean up temporary files")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be submitted without actually submitting",
)
@click.pass_context
def submit(ctx, scheduler, command_args, **kwargs):
    """Submit a job to the specified scheduler.

    Examples:
        molq submit local echo "Hello World"
        molq submit slurm --cpu-count 8 --memory 16G srun lmp -in input.lmp
        echo "echo hello" | molq submit slurm --cpu-count 4 --memory 8G
    """
    cmd = list(command_args) + list(ctx.args)
    if cmd:
        pass
    elif not sys.stdin.isatty():
        # Read from stdin
        script_content = sys.stdin.read().strip()
        if not script_content:
            click.echo("Error: No command provided", err=True)
            sys.exit(1)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write("#!/bin/bash\n")
            f.write(script_content)
            script_path = f.name
        os.chmod(script_path, 0o755)
        cmd = ["bash", script_path]
    else:
        click.echo(
            "Error: No command provided. Use arguments or pipe script via stdin.",
            err=True,
        )
        sys.exit(1)

    # Create resource spec
    resource_spec = {
        "cmd": cmd,
        "job_name": kwargs.get("job_name") or "molq_cli_job",
        "block": kwargs.get("block", False),
        "cleanup_temp_files": kwargs.get("cleanup", True),
    }

    # Add optional parameters
    for key in ["workdir", "email", "priority", "account"]:
        if kwargs.get(key):
            resource_spec[key] = kwargs[key]

    # Add resource parameters
    for key in ["cpu_count", "memory", "time", "queue", "gpu_count", "gpu_type"]:
        value = kwargs.get(key)
        if value:
            if key == "time":
                resource_spec["time_limit"] = value
            elif key == "queue":
                resource_spec["queue"] = value
            else:
                resource_spec[key] = value

    if kwargs.get("dry_run"):
        click.echo("Dry run - would submit job with configuration:")
        click.echo(yaml.dump(resource_spec, default_flow_style=False))
        return

    try:
        # Create submitter and submit job
        submitter = create_submitter(scheduler, **kwargs)
        job_id = submitter.submit(resource_spec)

        click.echo(f"✅ Job submitted successfully!")
        click.echo(f"   Job ID: {job_id}")
        click.echo(f"   Scheduler: {scheduler}")
        click.echo(f"   Command: {' '.join(cmd)}")

        if kwargs.get("block"):
            click.echo("   Status: Completed")
        else:
            click.echo("   Status: Running")

    except Exception as e:
        click.echo(f"❌ Job submission failed: {e}", err=True)
        sys.exit(1)

    finally:
        # Clean up temp script if created
        if "script_path" in locals():
            try:
                os.unlink(script_path)
            except:
                pass


@cli.command(name="list")
@click.option(
    "--scheduler", type=click.Choice(["local", "slurm"]), help="Filter by scheduler"
)
@click.option(
    "--all-history", is_flag=True, help="Show all jobs including finished/cancelled"
)
@click.option("--verbose", is_flag=True, help="Show detailed job info")
def list_jobs(scheduler, all_history, verbose):
    """List submitted jobs."""
    from molq.submitor.base import BaseSubmitor
    from molq.submitor.local import LocalSubmitor
    from molq.submitor.slurm import SlurmSubmitor

    # Always refresh job statuses before listing
    if scheduler == "local" or scheduler is None:
        # Refresh local jobs
        try:
            LocalSubmitor("cli_local").refresh_all_jobs("local")
        except Exception as e:
            click.echo(f"Warning: Failed to refresh local jobs: {e}", err=True)

    if scheduler == "slurm" or scheduler is None:
        # Refresh SLURM jobs
        try:
            # Use different section names that might exist in the DB
            for section in ["slurm@mock", "test_submitor", "cli_slurm"]:
                try:
                    SlurmSubmitor("cli_slurm").refresh_all_jobs(section)
                except Exception:
                    continue  # Section might not exist, that's ok
        except Exception as e:
            click.echo(f"Warning: Failed to refresh SLURM jobs: {e}", err=True)

    section = None
    if scheduler == "local":
        section = "local"
    elif scheduler == "slurm":
        section = "slurm@mock"  # or use actual cluster name if available

    jobs = BaseSubmitor.list_jobs(section=section, all_history=all_history)
    BaseSubmitor.print_jobs(jobs, verbose=verbose)


@cli.command()
@click.argument("job_id")
@click.option(
    "--scheduler", type=click.Choice(["local", "slurm"]), help="Scheduler to query"
)
def status(job_id, scheduler):
    """Get job status."""
    if not scheduler:
        if "-" in job_id:
            scheduler, job_id = job_id.split("-", 1)
        else:
            click.echo(
                "Error: Please specify --scheduler or use format 'scheduler-jobid'",
                err=True,
            )
            sys.exit(1)

    try:
        submitter = create_submitter(scheduler)
        jobs = submitter.query(int(job_id))

        if int(job_id) in jobs:
            job_status = jobs[int(job_id)]
            click.echo(f"Job {job_id} on {scheduler}:")
            click.echo(f"  Name: {job_status.name}")
            click.echo(f"  Status: {job_status.status.name}")
            for key, value in job_status.others.items():
                click.echo(f"  {key.title()}: {value}")
        else:
            click.echo(f"Job {job_id} not found on {scheduler}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("job_id")
@click.option(
    "--scheduler",
    type=click.Choice(["local", "slurm"]),
    help="Scheduler to cancel from",
)
def cancel(job_id, scheduler):
    """Cancel a job."""
    if not scheduler:
        if "-" in job_id:
            scheduler, job_id = job_id.split("-", 1)
        else:
            click.echo(
                "Error: Please specify --scheduler or use format 'scheduler-jobid'",
                err=True,
            )
            sys.exit(1)

    try:
        submitter = create_submitter(scheduler)
        submitter.cancel(int(job_id))
        click.echo(f"✅ Job {job_id} cancelled successfully on {scheduler}")

    except Exception as e:
        click.echo(f"❌ Cancel failed: {e}", err=True)
        sys.exit(1)


@cli.group()
def config():
    """Manage Molq configuration."""
    pass


@config.command()
@click.argument("key")
@click.argument("value")
def set(key, value):
    """Set a configuration value."""
    molq_config.set(key, value)
    click.echo(f"Set {key} = {value}")


@config.command()
@click.argument("key", required=False)
def show(key):
    """Show configuration."""
    if key:
        value = molq_config.get(key)
        if value is not None:
            click.echo(f"{key} = {value}")
        else:
            click.echo(f"{key} is not set")
    else:
        config_data = molq_config.show()
        if config_data:
            click.echo(yaml.dump(config_data, default_flow_style=False))
        else:
            click.echo("No configuration found")


if __name__ == "__main__":
    cli()
