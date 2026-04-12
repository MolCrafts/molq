#!/usr/bin/env python3
"""Molq CLI - Modern Job Queue.

Typer + Rich CLI for submitting, monitoring, and managing jobs
across local and cluster schedulers.
"""

from contextlib import contextmanager
from enum import Enum
from typing import Annotated, Iterator, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="molq",
    help="Modern Job Queue for local and cluster runners.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

console = Console(stderr=True)


class SchedulerType(str, Enum):
    local = "local"
    slurm = "slurm"
    pbs = "pbs"
    lsf = "lsf"


@contextmanager
def _open_submitor(
    scheduler: SchedulerType,
    cluster: str | None = None,
) -> Iterator["Submitor"]:  # type: ignore[name-defined]
    """Open a Submitor for the CLI and guarantee its connection is closed."""
    from molq import Submitor

    cluster_name = cluster or f"cli_{scheduler.value}"
    submitor = Submitor(cluster_name, scheduler.value)
    try:
        yield submitor
    finally:
        submitor.close()


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


@app.command()
def submit(
    scheduler: Annotated[SchedulerType, typer.Argument(help="Scheduler backend")],
    command: Annotated[
        Optional[list[str]],
        typer.Argument(help="Command to execute"),
    ] = None,
    cpu_count: Annotated[
        Optional[int], typer.Option("--cpus", help="CPU cores")
    ] = None,
    memory: Annotated[
        Optional[str], typer.Option("--mem", help="Memory (e.g. 8G)")
    ] = None,
    time_limit: Annotated[
        Optional[str], typer.Option("--time", help="Time limit")
    ] = None,
    queue: Annotated[Optional[str], typer.Option(help="Queue/partition")] = None,
    gpu_count: Annotated[Optional[int], typer.Option("--gpus", help="GPUs")] = None,
    gpu_type: Annotated[Optional[str], typer.Option(help="GPU type")] = None,
    job_name: Annotated[Optional[str], typer.Option("--name", help="Job name")] = None,
    workdir: Annotated[Optional[str], typer.Option(help="Working directory")] = None,
    account: Annotated[Optional[str], typer.Option(help="Billing account")] = None,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
    block: Annotated[bool, typer.Option(help="Wait for completion")] = False,
) -> None:
    """Submit a job to the specified scheduler."""
    from molq import Duration, JobExecution, JobResources, JobScheduling, Memory

    cmd: list[str] = list(command) if command else []
    if not cmd:
        console.print("[red]Error: No command provided.[/]")
        raise typer.Exit(1)

    # Build resource specs
    resources = JobResources(
        cpu_count=cpu_count,
        memory=Memory.parse(memory) if memory else None,
        gpu_count=gpu_count,
        gpu_type=gpu_type,
        time_limit=Duration.parse(time_limit) if time_limit else None,
    )
    scheduling = JobScheduling(queue=queue, account=account)
    execution = JobExecution(cwd=workdir, job_name=job_name or "molq_cli_job")

    try:
        with _open_submitor(scheduler, cluster) as submitor:
            handle = submitor.submit(
                argv=cmd,
                resources=resources,
                scheduling=scheduling,
                execution=execution,
            )

            rprint(f"[green]Job submitted[/]")
            rprint(f"  ID:        {handle.job_id}")
            rprint(f"  Scheduler: {scheduler.value}")
            rprint(f"  Command:   {' '.join(cmd)}")

            if block:
                record = handle.wait()
                rprint(f"  Status:    {record.state.value}")
            else:
                rprint(f"  Status:    submitted")

    except Exception as e:
        console.print(f"[red]Submission failed: {e}[/]")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@app.command(name="list")
def list_jobs(
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
    all: Annotated[bool, typer.Option("--all", help="Include terminal jobs")] = False,
) -> None:
    """List submitted jobs."""
    with _open_submitor(scheduler, cluster) as submitor:
        records = submitor.list(include_terminal=all)

    if not records:
        rprint("[dim]No jobs found.[/]")
        return

    table = Table(title="Jobs")
    table.add_column("Job ID", style="cyan", max_width=36)
    table.add_column("State", style="bold")
    table.add_column("Type")
    table.add_column("Command", max_width=40)

    for r in records:
        state_style = {
            "running": "green",
            "succeeded": "green",
            "failed": "red",
            "cancelled": "yellow",
            "lost": "red",
        }.get(r.state.value, "")

        table.add_row(
            r.job_id[:12] + "...",
            f"[{state_style}]{r.state.value}[/{state_style}]",
            r.command_type,
            r.command_display[:40],
        )

    rprint(table)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@app.command()
def status(
    job_id: Annotated[str, typer.Argument(help="Job ID")],
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
) -> None:
    """Get job status."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster) as submitor:
        try:
            record = submitor.get(job_id)
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)

    rprint(f"Job {record.job_id}:")
    rprint(f"  State:   [bold]{record.state.value}[/]")
    rprint(f"  Type:    {record.command_type}")
    rprint(f"  Command: {record.command_display}")
    if record.exit_code is not None:
        rprint(f"  Exit:    {record.exit_code}")


# ---------------------------------------------------------------------------
# watch
# ---------------------------------------------------------------------------


@app.command()
def watch(
    job_id: Annotated[str, typer.Argument(help="Job ID to watch")],
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
    timeout: Annotated[Optional[float], typer.Option(help="Max seconds")] = None,
) -> None:
    """Watch a job until completion."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster) as submitor:
        try:
            record = submitor.get(job_id)
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)

        if record.state.is_terminal:
            rprint(f"Job {job_id}: [bold]{record.state.value}[/]")
            return

        try:
            handle_record = submitor._monitor_instance.wait_one(job_id, timeout=timeout)
            rprint(f"Job {job_id}: [bold]{handle_record.state.value}[/]")
            if handle_record.exit_code is not None:
                rprint(f"  Exit code: {handle_record.exit_code}")
        except TimeoutError:
            console.print(f"[red]Timeout waiting for job {job_id}[/]")
            raise typer.Exit(1)
        except KeyboardInterrupt:
            rprint("[dim]Interrupted[/]")


# ---------------------------------------------------------------------------
# monitor
# ---------------------------------------------------------------------------


@app.command()
def monitor(
    all_jobs: Annotated[
        bool,
        typer.Option(
            "--all", "-a", help="Include terminal jobs (done/failed/cancelled)."
        ),
    ] = False,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Max number of job rows to display."),
    ] = 200,
    refresh: Annotated[
        float,
        typer.Option("--refresh", "-r", help="Refresh interval in seconds."),
    ] = 2.0,
    db: Annotated[
        Optional[str],
        typer.Option(
            "--db", help="Path to molq SQLite database (default: ~/.molq/jobs.db)."
        ),
    ] = None,
) -> None:
    """Open full-screen dashboard for all molq jobs across all clusters."""
    from molq.dashboard import MolqMonitor

    rprint(f"[dim]Opening monitor… (press q to close)[/dim]")
    MolqMonitor(
        db_path=db,
        include_terminal=all_jobs,
        limit=limit,
        refresh_interval=refresh,
    ).watch()
    rprint(f"\n[dim]Monitor closed.[/dim]")


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------


@app.command()
def cancel(
    job_id: Annotated[str, typer.Argument(help="Job ID to cancel")],
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
) -> None:
    """Cancel a running job."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster) as submitor:
        try:
            submitor.cancel(job_id)
            rprint(f"[green]Job {job_id} cancelled[/]")
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Cancel failed: {e}[/]")
            raise typer.Exit(1)


if __name__ == "__main__":
    app()
