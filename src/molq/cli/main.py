#!/usr/bin/env python3
"""Molq CLI - Modern Job Queue.

Typer + Rich CLI for submitting, monitoring, and managing jobs
across local and cluster schedulers.
"""

import sys
import time
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from pathlib import Path
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


def _format_timestamp(value: float | None) -> str:
    if value is None:
        return "-"
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def _state_style(state: str) -> str:
    return {
        "running": "green",
        "succeeded": "green",
        "failed": "red",
        "cancelled": "yellow",
        "timed_out": "yellow",
        "lost": "red",
        "queued": "cyan",
        "submitted": "cyan",
    }.get(state, "")


def _log_paths(record: "JobRecord", stream_name: str) -> dict[str, Path]:  # type: ignore[name-defined]
    stream_keys = {
        "stdout": "molq.stdout_path",
        "stderr": "molq.stderr_path",
    }
    wanted = ("stdout", "stderr") if stream_name == "both" else (stream_name,)
    result: dict[str, Path] = {}
    for key in wanted:
        value = record.metadata.get(stream_keys[key])
        if not value:
            raise FileNotFoundError(f"No {key} log is recorded for job {record.job_id}")
        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(f"{key} log does not exist: {path}")
        result[key] = path
    return result


def _emit_log_text(stream_name: str, text: str, *, labeled: bool) -> None:
    if not text:
        return
    if not labeled:
        sys.stdout.write(text)
        sys.stdout.flush()
        return
    for chunk in text.splitlines(keepends=True):
        sys.stdout.write(f"[{stream_name}] {chunk}")
    sys.stdout.flush()


def _read_text(path: Path) -> str:
    return path.read_text(errors="replace")


def _follow_logs(submitor: "Submitor", job_id: str, stream_name: str, tail: int | None) -> None:  # type: ignore[name-defined]
    record = submitor.get(job_id)
    paths = _log_paths(record, stream_name)
    labeled = stream_name == "both"
    handles = {name: path.open("r", encoding="utf-8", errors="replace") for name, path in paths.items()}
    try:
        for name, handle in handles.items():
            initial = handle.read()
            if tail is not None:
                initial = "".join(initial.splitlines(keepends=True)[-tail:])
            _emit_log_text(name, initial, labeled=labeled)
            handle.seek(0, 2)

        while True:
            emitted = False
            for name, handle in handles.items():
                chunk = handle.read()
                if chunk:
                    emitted = True
                    _emit_log_text(name, chunk, labeled=labeled)

            record = submitor.get(job_id)
            if record.state.is_terminal and not emitted:
                break

            submitor.refresh()
            time.sleep(0.2)
    finally:
        for handle in handles.values():
            handle.close()


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
    execution = JobExecution(cwd=workdir, job_name=job_name)

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
        submitor.refresh()
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
        style = _state_style(r.state.value)
        state_value = f"[{style}]{r.state.value}[/{style}]" if style else r.state.value
        table.add_row(
            r.job_id[:12] + "...",
            state_value,
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
        submitor.refresh()
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
    if record.failure_reason:
        rprint(f"  Reason:  {record.failure_reason}")


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------


@app.command()
def logs(
    job_id: Annotated[str, typer.Argument(help="Job ID")],
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
    stream: Annotated[
        str,
        typer.Option("--stream", help="Which stream to read: stdout, stderr, or both"),
    ] = "stdout",
    tail: Annotated[
        Optional[int],
        typer.Option("--tail", help="Show only the last N lines"),
    ] = None,
    follow: Annotated[
        bool,
        typer.Option("--follow", help="Tail the log until the job reaches EOF"),
    ] = False,
) -> None:
    """Print captured job logs."""
    from molq import JobNotFoundError

    stream_name = stream.lower()
    if stream_name not in {"stdout", "stderr", "both"}:
        console.print("[red]--stream must be one of: stdout, stderr, both[/]")
        raise typer.Exit(1)

    with _open_submitor(scheduler, cluster) as submitor:
        submitor.refresh()
        try:
            record = submitor.get(job_id)
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)
        try:
            if follow:
                _follow_logs(submitor, job_id, stream_name, tail)
            else:
                paths = _log_paths(record, stream_name)
                labeled = stream_name == "both"
                emitted = False
                for name, path in paths.items():
                    content = _read_text(path)
                    if tail is not None:
                        content = "".join(content.splitlines(keepends=True)[-tail:])
                    if content:
                        emitted = True
                        _emit_log_text(name, content, labeled=labeled)
                if not emitted:
                    rprint(f"[dim]{stream_name} log is empty[/]")
        except FileNotFoundError as exc:
            console.print(f"[red]{exc}[/]")
            raise typer.Exit(1)


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
# history
# ---------------------------------------------------------------------------


@app.command()
def history(
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
    all: Annotated[bool, typer.Option("--all", help="Include terminal jobs")] = False,
) -> None:
    """Show job history for a scheduler/cluster namespace."""
    with _open_submitor(scheduler, cluster) as submitor:
        submitor.refresh()
        records = submitor.list(include_terminal=all)

    if not records:
        rprint("[dim]No jobs found.[/]")
        return

    table = Table(title="History")
    table.add_column("Job ID", style="cyan", max_width=36)
    table.add_column("State", style="bold")
    table.add_column("Scheduler ID")
    table.add_column("Submitted")
    table.add_column("Finished")
    table.add_column("Exit")
    table.add_column("Command", max_width=36)

    for record in records:
        style = _state_style(record.state.value)
        state_value = (
            f"[{style}]{record.state.value}[/{style}]"
            if style
            else record.state.value
        )
        table.add_row(
            record.job_id[:12] + "...",
            state_value,
            record.scheduler_job_id or "-",
            _format_timestamp(record.submitted_at),
            _format_timestamp(record.finished_at),
            "-" if record.exit_code is None else str(record.exit_code),
            record.command_display[:36],
        )

    rprint(table)


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


@app.command()
def inspect(
    job_id: Annotated[str, typer.Argument(help="Job ID")],
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[Optional[str], typer.Option(help="Cluster name")] = None,
) -> None:
    """Show canonical job metadata and transition timeline."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster) as submitor:
        submitor.refresh()
        try:
            record = submitor.get(job_id)
            transitions = submitor.get_transitions(job_id)
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)

    rprint(f"Job {record.job_id}:")
    rprint(f"  Cluster:        {record.cluster_name}")
    rprint(f"  Scheduler:      {record.scheduler}")
    rprint(f"  Scheduler ID:   {record.scheduler_job_id or '-'}")
    rprint(f"  State:          [bold]{record.state.value}[/]")
    rprint(f"  Command:        {record.command_display}")
    rprint(f"  Command Type:   {record.command_type}")
    rprint(f"  Working Dir:    {record.cwd}")
    rprint(f"  Submitted At:   {_format_timestamp(record.submitted_at)}")
    rprint(f"  Started At:     {_format_timestamp(record.started_at)}")
    rprint(f"  Finished At:    {_format_timestamp(record.finished_at)}")
    rprint(f"  Exit Code:      {record.exit_code if record.exit_code is not None else '-'}")
    rprint(f"  Failure:        {record.failure_reason or '-'}")
    rprint(f"  Job Dir:        {record.metadata.get('molq.job_dir', '-')}")
    rprint(f"  Stdout:         {record.metadata.get('molq.stdout_path', '-')}")
    rprint(f"  Stderr:         {record.metadata.get('molq.stderr_path', '-')}")

    rprint("  Timeline:")
    for transition in transitions:
        old_state = transition.old_state.value if transition.old_state else "-"
        reason = f" ({transition.reason})" if transition.reason else ""
        rprint(
            f"    {_format_timestamp(transition.timestamp)}  "
            f"{old_state} -> {transition.new_state.value}{reason}"
        )


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
