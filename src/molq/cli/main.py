#!/usr/bin/env python3
"""Molq CLI - Modern Job Queue.

Typer + Rich CLI for submitting, monitoring, and managing jobs
across local and cluster schedulers.
"""

import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    from molq import JobRecord, Submitor

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


class SchedulerType(StrEnum):
    local = "local"
    slurm = "slurm"
    pbs = "pbs"
    lsf = "lsf"


@contextmanager
def _open_submitor(
    scheduler: SchedulerType,
    cluster: str | None = None,
    profile: str | None = None,
    config_path: str | None = None,
) -> Iterator["Submitor"]:
    """Open a Submitor for the CLI and guarantee its connection is closed."""
    from molq import Submitor
    from molq.config import load_profile

    if profile:
        loaded = load_profile(profile, config_path)
        if loaded.scheduler != scheduler.value:
            raise typer.BadParameter(
                f"profile {profile!r} uses scheduler {loaded.scheduler!r}, "
                f"not {scheduler.value!r}"
            )
        cluster_name = cluster or loaded.cluster_name
        submitor = Submitor.from_profile(profile, config_path=config_path)
        if cluster is not None and cluster != loaded.cluster_name:
            submitor = Submitor(
                cluster_name,
                scheduler.value,
                defaults=loaded.defaults,
                scheduler_options=loaded.scheduler_options,
                jobs_dir=loaded.jobs_dir,
                default_retry_policy=loaded.retry,
                retention_policy=loaded.retention,
                profile_name=loaded.name,
            )
    else:
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


def _log_paths(record: "JobRecord", stream_name: str) -> dict[str, Path]:
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


def _follow_logs(
    submitor: "Submitor", job_id: str, stream_name: str, tail: int | None
) -> None:
    record = submitor.get(job_id)
    paths = _log_paths(record, stream_name)
    labeled = stream_name == "both"
    handles = {
        name: path.open("r", encoding="utf-8", errors="replace")
        for name, path in paths.items()
    }
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


def _dependency_relation_state(dependency_type: str, record: "JobRecord") -> str:
    from molq.store import dependency_relation_state

    return dependency_relation_state(dependency_type, record.state, record.started_at)


def _dependency_marker(relation_state: str) -> str:
    return {"satisfied": "✓", "pending": "·", "impossible": "!"}.get(
        relation_state, "·"
    )


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


@app.command()
def submit(
    scheduler: Annotated[SchedulerType, typer.Argument(help="Scheduler backend")],
    command: Annotated[
        list[str] | None,
        typer.Argument(help="Command to execute"),
    ] = None,
    cpu_count: Annotated[int | None, typer.Option("--cpus", help="CPU cores")] = None,
    memory: Annotated[
        str | None, typer.Option("--mem", help="Memory (e.g. 8G)")
    ] = None,
    time_limit: Annotated[str | None, typer.Option("--time", help="Time limit")] = None,
    queue: Annotated[str | None, typer.Option(help="Queue/partition")] = None,
    gpu_count: Annotated[int | None, typer.Option("--gpus", help="GPUs")] = None,
    gpu_type: Annotated[str | None, typer.Option(help="GPU type")] = None,
    job_name: Annotated[str | None, typer.Option("--name", help="Job name")] = None,
    workdir: Annotated[str | None, typer.Option(help="Working directory")] = None,
    account: Annotated[str | None, typer.Option(help="Billing account")] = None,
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    retries: Annotated[
        int | None, typer.Option("--retries", help="Maximum attempts")
    ] = None,
    retry_on_exit_code: Annotated[
        list[int] | None,
        typer.Option(
            "--retry-on-exit-code",
            help="Retry only for the specified exit code(s)",
        ),
    ] = None,
    after: Annotated[
        list[str] | None,
        typer.Option("--after", help="Wait until the given molq job(s) finish"),
    ] = None,
    after_started: Annotated[
        list[str] | None,
        typer.Option(
            "--after-started",
            help="Wait until the given molq job(s) start running",
        ),
    ] = None,
    after_failure: Annotated[
        list[str] | None,
        typer.Option(
            "--after-failure",
            help="Wait until the given molq job(s) fail",
        ),
    ] = None,
    after_success: Annotated[
        list[str] | None,
        typer.Option(
            "--after-success",
            help="Wait until the given molq job(s) succeed",
        ),
    ] = None,
    block: Annotated[bool, typer.Option(help="Wait for completion")] = False,
) -> None:
    """Submit a job to the specified scheduler."""
    from molq import (
        Duration,
        JobExecution,
        JobResources,
        JobScheduling,
        Memory,
        RetryPolicy,
    )

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
    retry_policy = None
    if retries is not None:
        retry_policy = RetryPolicy(
            max_attempts=retries,
            retry_on_exit_codes=(
                None
                if not retry_on_exit_code
                else tuple(int(code) for code in retry_on_exit_code)
            ),
        )

    try:
        with _open_submitor(scheduler, cluster, profile, config) as submitor:
            handle = submitor.submit(
                argv=cmd,
                resources=resources,
                scheduling=scheduling,
                execution=execution,
                retry=retry_policy,
                after_started=after_started,
                after=after,
                after_failure=after_failure,
                after_success=after_success,
            )

            rprint("[green]Job submitted[/]")
            rprint(f"  ID:        {handle.job_id}")
            rprint(f"  Scheduler: {scheduler.value}")
            rprint(f"  Command:   {' '.join(cmd)}")

            if block:
                record = handle.wait()
                rprint(f"  Status:    {record.state.value}")
            else:
                rprint("  Status:    submitted")

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
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    all: Annotated[bool, typer.Option("--all", help="Include terminal jobs")] = False,
) -> None:
    """List submitted jobs."""
    with _open_submitor(scheduler, cluster, profile, config) as submitor:
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
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
) -> None:
    """Get job status."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster, profile, config) as submitor:
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
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    stream: Annotated[
        str,
        typer.Option("--stream", help="Which stream to read: stdout, stderr, or both"),
    ] = "stdout",
    tail: Annotated[
        int | None,
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

    with _open_submitor(scheduler, cluster, profile, config) as submitor:
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
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    timeout: Annotated[float | None, typer.Option(help="Max seconds")] = None,
) -> None:
    """Watch a job until completion."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster, profile, config) as submitor:
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
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    all: Annotated[bool, typer.Option("--all", help="Include terminal jobs")] = False,
) -> None:
    """Show job history for a scheduler/cluster namespace."""
    with _open_submitor(scheduler, cluster, profile, config) as submitor:
        submitor.refresh()
        records = submitor.list(include_terminal=all)

    if not records:
        rprint("[dim]No jobs found.[/]")
        return

    table = Table(title="History")
    table.add_column("Job ID", style="cyan", max_width=36)
    table.add_column("Attempt")
    table.add_column("State", style="bold")
    table.add_column("Scheduler ID")
    table.add_column("Submitted")
    table.add_column("Finished")
    table.add_column("Exit")
    table.add_column("Command", max_width=36)

    for record in records:
        style = _state_style(record.state.value)
        state_value = (
            f"[{style}]{record.state.value}[/{style}]" if style else record.state.value
        )
        table.add_row(
            record.job_id[:12] + "...",
            str(record.attempt),
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
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
) -> None:
    """Show canonical job metadata and transition timeline."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster, profile, config) as submitor:
        submitor.refresh()
        try:
            record = submitor.get(job_id)
            transitions = submitor.get_transitions(job_id)
            family = submitor.get_retry_family(job_id)
            dependencies = submitor.get_dependencies(job_id)
            dependents = submitor.get_dependents(job_id)
            upstream_lines: list[str] = []
            downstream_lines: list[str] = []
            for dependency in dependencies:
                dep_record = submitor.get(dependency.dependency_job_id)
                relation_state = _dependency_relation_state(
                    dependency.dependency_type, dep_record
                )
                upstream_lines.append(
                    f"      {_dependency_marker(relation_state)} "
                    f"{dependency.dependency_job_id}  {dependency.dependency_type}  "
                    f"{dep_record.state.value}  scheduler={dependency.scheduler_dependency}"
                )
            for dependent in dependents:
                dependent_record = submitor.get(dependent.job_id)
                relation_state = _dependency_relation_state(
                    dependent.dependency_type, record
                )
                downstream_lines.append(
                    f"      {_dependency_marker(relation_state)} "
                    f"{dependent.job_id}  {dependent.dependency_type}  "
                    f"{dependent_record.state.value}"
                )
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)

    rprint(f"Job {record.job_id}:")
    rprint(f"  Cluster:        {record.cluster_name}")
    rprint(f"  Scheduler:      {record.scheduler}")
    rprint(f"  Root Job ID:    {record.root_job_id}")
    rprint(f"  Attempt:        {record.attempt}")
    rprint(f"  Previous:       {record.previous_attempt_job_id or '-'}")
    rprint(f"  Scheduler ID:   {record.scheduler_job_id or '-'}")
    rprint(f"  State:          [bold]{record.state.value}[/]")
    rprint(f"  Command:        {record.command_display}")
    rprint(f"  Command Type:   {record.command_type}")
    rprint(f"  Working Dir:    {record.cwd}")
    rprint(f"  Submitted At:   {_format_timestamp(record.submitted_at)}")
    rprint(f"  Started At:     {_format_timestamp(record.started_at)}")
    rprint(f"  Finished At:    {_format_timestamp(record.finished_at)}")
    rprint(
        f"  Exit Code:      {record.exit_code if record.exit_code is not None else '-'}"
    )
    rprint(f"  Failure:        {record.failure_reason or '-'}")
    rprint(f"  Job Dir:        {record.metadata.get('molq.job_dir', '-')}")
    rprint(f"  Stdout:         {record.metadata.get('molq.stdout_path', '-')}")
    rprint(f"  Stderr:         {record.metadata.get('molq.stderr_path', '-')}")
    rprint(f"  Profile:        {record.profile_name or '-'}")

    rprint("  Retry Family:")
    for member in family:
        rprint(
            f"    attempt {member.attempt}: {member.job_id} "
            f"[bold]{member.state.value}[/]"
        )

    rprint("  Dependencies:")
    if dependencies:
        rprint("    Upstream:")
        for line in upstream_lines:
            rprint(line)
    else:
        rprint("    Upstream: -")

    if dependents:
        rprint("    Downstream:")
        for line in downstream_lines:
            rprint(line)
    else:
        rprint("    Downstream: -")

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
        str | None,
        typer.Option(
            "--db", help="Path to molq SQLite database (default: ~/.molq/jobs.db)."
        ),
    ] = None,
) -> None:
    """Open full-screen dashboard for all molq jobs across all clusters."""
    from molq.dashboard import MolqMonitor

    rprint("[dim]Opening monitor… (press q to close)[/dim]")
    MolqMonitor(
        db_path=db,
        include_terminal=all_jobs,
        limit=limit,
        refresh_interval=refresh,
    ).watch()
    rprint("\n[dim]Monitor closed.[/dim]")


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------


@app.command()
def cancel(
    job_id: Annotated[str, typer.Argument(help="Job ID to cancel")],
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
) -> None:
    """Cancel a running job."""
    from molq import JobNotFoundError

    with _open_submitor(scheduler, cluster, profile, config) as submitor:
        try:
            submitor.cancel(job_id)
            rprint(f"[green]Job {job_id} cancelled[/]")
        except JobNotFoundError:
            rprint(f"[yellow]Job {job_id} not found[/]")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Cancel failed: {e}[/]")
            raise typer.Exit(1)


@app.command()
def cleanup(
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview only")] = False,
) -> None:
    """Clean up old job artifacts and records."""
    with _open_submitor(scheduler, cluster, profile, config) as submitor:
        result = submitor.cleanup(dry_run=dry_run)
    rprint(f"Job dirs: {len(result['job_dirs'])}")
    rprint(f"Records:  {len(result['records'])}")
    for path in result["job_dirs"]:
        rprint(f"  dir: {path}")
    for job_id in result["records"]:
        rprint(f"  record: {job_id}")


@app.command()
def daemon(
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    once: Annotated[
        bool, typer.Option("--once", help="Run one cycle and exit")
    ] = False,
    interval: Annotated[
        float, typer.Option("--interval", help="Polling interval in seconds")
    ] = 5.0,
    skip_cleanup: Annotated[
        bool, typer.Option("--skip-cleanup", help="Skip retention cleanup")
    ] = False,
) -> None:
    """Run the background reconciliation loop."""
    with _open_submitor(scheduler, cluster, profile, config) as submitor:
        try:
            submitor.daemon(once=once, interval=interval, run_cleanup=not skip_cleanup)
        except KeyboardInterrupt:
            rprint("[dim]Daemon interrupted[/]")


if __name__ == "__main__":
    app()
