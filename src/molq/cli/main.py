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
    from molq import Cluster, Submitor
    from molq.config import load_profile

    if profile:
        loaded = load_profile(profile, config_path)
        if loaded.scheduler != scheduler.value:
            raise typer.BadParameter(
                f"profile {profile!r} uses scheduler {loaded.scheduler!r}, "
                f"not {scheduler.value!r}"
            )
        cluster_name = cluster or loaded.cluster_name
        target = Cluster(
            cluster_name,
            scheduler.value,
            scheduler_options=loaded.scheduler_options,
        )
        submitor = Submitor(
            target,
            defaults=loaded.defaults,
            jobs_dir=loaded.jobs_dir,
            default_retry_policy=loaded.retry,
            retention_policy=loaded.retention,
            profile_name=loaded.name,
        )
    else:
        cluster_name = cluster or f"cli_{scheduler.value}"
        submitor = Submitor(target=Cluster(cluster_name, scheduler.value))
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
    record = submitor.get_job(job_id)
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

            record = submitor.get_job(job_id)
            if record.state.is_terminal and not emitted:
                break

            submitor.refresh_jobs()
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
    partition: Annotated[
        str | None,
        typer.Option(
            "--partition",
            help="Scheduler partition (SLURM partition / PBS / LSF queue)",
        ),
    ] = None,
    queue: Annotated[
        str | None,
        typer.Option("--queue", help="Deprecated alias for --partition", hidden=True),
    ] = None,
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
    if partition is not None and queue is not None:
        console.print("[red]Error: pass --partition or --queue, not both.[/]")
        raise typer.Exit(1)
    if queue is not None and partition is None:
        console.print("[yellow]Warning: --queue is deprecated; use --partition.[/]")
        partition = queue
    scheduling = JobScheduling(partition=partition, account=account)
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
            handle = submitor.submit_job(
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
        submitor.refresh_jobs()
        records = submitor.list_jobs(include_terminal=all)

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
        submitor.refresh_jobs()
        try:
            record = submitor.get_job(job_id)
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
        submitor.refresh_jobs()
        try:
            record = submitor.get_job(job_id)
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
    job_id: Annotated[str | None, typer.Argument(help="Job ID to watch")] = None,
    scheduler: Annotated[
        SchedulerType, typer.Argument(help="Scheduler")
    ] = SchedulerType.local,
    cluster: Annotated[str | None, typer.Option(help="Cluster name")] = None,
    profile: Annotated[str | None, typer.Option(help="Profile name")] = None,
    config: Annotated[str | None, typer.Option(help="Path to config.toml")] = None,
    timeout: Annotated[float | None, typer.Option(help="Max seconds")] = None,
    all_jobs: Annotated[
        bool,
        typer.Option("--all", "-a", help="Watch all active jobs in this namespace"),
    ] = False,
) -> None:
    """Watch a job (or all active jobs with --all) until completion."""
    from molq import JobNotFoundError
    from molq.errors import MolqTimeoutError

    if all_jobs and job_id is not None:
        console.print("[red]Cannot combine --all with a job ID[/]")
        raise typer.Exit(1)
    if not all_jobs and job_id is None:
        console.print("[red]Provide a job ID or use --all[/]")
        raise typer.Exit(1)

    with _open_submitor(scheduler, cluster, profile, config) as submitor:
        if all_jobs:
            submitor.refresh_jobs()
            active = [r for r in submitor.list_jobs(include_terminal=False)]
            if not active:
                rprint("[dim]No active jobs.[/]")
                return
            rprint(f"[dim]Watching {len(active)} active job(s)…[/]")
            try:
                records = submitor.watch_jobs(None, timeout=timeout)
            except MolqTimeoutError:
                console.print("[red]Timeout waiting for jobs[/]")
                raise typer.Exit(1)
            except KeyboardInterrupt:
                rprint("[dim]Interrupted[/]")
                return

            watched_ids = {r.job_id for r in active}
            table = Table(title="Watched Jobs")
            table.add_column("Job ID", style="cyan", max_width=36)
            table.add_column("State", style="bold")
            table.add_column("Exit")
            table.add_column("Command", max_width=40)
            for r in records:
                if r.job_id not in watched_ids:
                    continue
                style = _state_style(r.state.value)
                state_value = (
                    f"[{style}]{r.state.value}[/{style}]" if style else r.state.value
                )
                table.add_row(
                    r.job_id[:12] + "...",
                    state_value,
                    "-" if r.exit_code is None else str(r.exit_code),
                    r.command_display[:40],
                )
            rprint(table)
            return

        # Narrowed by the guards at the top of the function:
        # if not all_jobs and job_id is None we already exited.
        assert job_id is not None
        try:
            record = submitor.get_job(job_id)
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
        except MolqTimeoutError:
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
        submitor.refresh_jobs()
        records = submitor.list_jobs(include_terminal=all)

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
        submitor.refresh_jobs()
        try:
            record = submitor.get_job(job_id)
            transitions = submitor.get_transitions(job_id)
            family = submitor.get_retry_family(job_id)
            dependencies = submitor.get_dependencies(job_id)
            dependents = submitor.get_dependents(job_id)
            upstream_lines: list[str] = []
            downstream_lines: list[str] = []
            for dependency in dependencies:
                dep_record = submitor.get_job(dependency.dependency_job_id)
                relation_state = _dependency_relation_state(
                    dependency.dependency_type, dep_record
                )
                upstream_lines.append(
                    f"      {_dependency_marker(relation_state)} "
                    f"{dependency.dependency_job_id}  {dependency.dependency_type}  "
                    f"{dep_record.state.value}  scheduler={dependency.scheduler_dependency}"
                )
            for dependent in dependents:
                dependent_record = submitor.get_job(dependent.job_id)
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
            "--db",
            help="Path to molq SQLite database. Defaults to the molcrafts-standard "
            "location resolved via molcfg (~/.molcrafts/molq/config/jobs.db, or "
            "$MOLCRAFTS_HOME/molq/config/jobs.db if set).",
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
            submitor.cancel_job(job_id)
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
        result = submitor.cleanup_jobs(dry_run=dry_run)
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
            submitor.run_daemon(
                once=once, interval=interval, run_cleanup=not skip_cleanup
            )
        except KeyboardInterrupt:
            rprint("[dim]Daemon interrupted[/]")


# ---------------------------------------------------------------------------
# clusters — discovery from ~/.ssh/config + ~/.molq/config.toml profiles
# ---------------------------------------------------------------------------


clusters_app = typer.Typer(
    name="clusters",
    help="Inspect cluster destinations: SSH config aliases + molq profiles.",
    no_args_is_help=True,
)
app.add_typer(clusters_app, name="clusters")


def _profile_destinations(config_path: str | None) -> list[dict[str, str]]:
    from molq import load_config

    rows: list[dict[str, str]] = []
    cfg = load_config(config_path)
    for profile in cfg.profiles.values():
        rows.append(
            {
                "name": profile.cluster_name,
                "source": f"profile:{profile.name}",
                "scheduler": profile.scheduler,
                "target": "(profile)",
            }
        )
    return rows


def _ssh_destinations(ssh_config: str | None) -> list[dict[str, str]]:
    from molq import list_ssh_hosts

    rows: list[dict[str, str]] = []
    for host in list_ssh_hosts(ssh_config):
        rows.append(
            {
                "name": host.alias,
                "source": "ssh_config",
                "scheduler": "?",
                "target": host.target,
            }
        )
    return rows


@clusters_app.command("list")
def clusters_list(
    config: Annotated[str | None, typer.Option(help="Path to molq config.toml")] = None,
    ssh_config: Annotated[
        str | None, typer.Option(help="Path to ssh_config (default: ~/.ssh/config)")
    ] = None,
) -> None:
    """List cluster destinations from molq profiles and ~/.ssh/config."""
    profile_rows = _profile_destinations(config)
    ssh_rows = _ssh_destinations(ssh_config)

    if not profile_rows and not ssh_rows:
        rprint("[dim]No clusters discovered.[/]")
        return

    table = Table(title="Clusters")
    table.add_column("Name", style="cyan")
    table.add_column("Source")
    table.add_column("Scheduler")
    table.add_column("Target")
    for row in profile_rows + ssh_rows:
        table.add_row(row["name"], row["source"], row["scheduler"], row["target"])
    rprint(table)


@clusters_app.command("show")
def clusters_show(
    name: Annotated[str, typer.Argument(help="Cluster alias or profile name")],
    config: Annotated[str | None, typer.Option(help="Path to molq config.toml")] = None,
    ssh_config: Annotated[
        str | None, typer.Option(help="Path to ssh_config (default: ~/.ssh/config)")
    ] = None,
) -> None:
    """Show effective settings for a cluster — profile or SSH alias."""
    from molq import load_config, resolve_ssh_host

    cfg = load_config(config)
    profile = next(
        (p for p in cfg.profiles.values() if p.name == name or p.cluster_name == name),
        None,
    )
    if profile is not None:
        rprint(f"[bold]Profile:[/] {profile.name}")
        rprint(f"  Cluster:   {profile.cluster_name}")
        rprint(f"  Scheduler: {profile.scheduler}")
        if profile.scheduler_options is not None:
            rprint(f"  Options:   {profile.scheduler_options}")
        if profile.jobs_dir:
            rprint(f"  Jobs dir:  {profile.jobs_dir}")
        return

    try:
        host = resolve_ssh_host(name)
    except OSError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1)

    rprint(f"[bold]SSH alias:[/] {host.alias}")
    rprint(f"  Hostname:      {host.hostname or '-'}")
    rprint(f"  User:          {host.user or '-'}")
    rprint(f"  Port:          {host.port or 22}")
    rprint(f"  IdentityFile:  {host.identity_file or '-'}")
    if host.proxy_jump:
        rprint(f"  ProxyJump:     {host.proxy_jump}")
    if host.forward_agent:
        rprint("  ForwardAgent:  yes")


if __name__ == "__main__":
    app()
