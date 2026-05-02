"""Full-screen terminal dashboard for monitoring runs/jobs.

Presentation layer only.  Does not own run lifecycle or scheduler logic.
The caller supplies a ``data_fn`` that returns a fresh :class:`DashboardState`
on every refresh tick.

Keyboard bindings::

    ↑ / ↓       navigate job list
    ↵           open detail view for selected job
    ↵ / Esc     close detail view, return to list
    q / Ctrl-C  close dashboard (jobs keep running)

Usage::

    from molq.dashboard import RunDashboard, DashboardState, JobRow

    RunDashboard().watch(lambda: build_state(), refresh_interval=2.0)
"""

from __future__ import annotations

import os
import select
import sys
import termios
import threading
import time
import tty
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskProgressColumn
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from molq.store import JobStore

# ── Data models ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class JobRow:
    """A single job/run entry in the dashboard.

    Attributes:
        state: Status string, e.g. ``"running"``, ``"pending"``, ``"succeeded"``.
        run_id: Job or run identifier shown in the list and detail title.
        cluster: Cluster name; ``None`` for local runs.
        scheduler_id: Scheduler-assigned job ID; ``None`` if unknown.
        elapsed: Human-readable elapsed time, e.g. ``"1m 23s"``.
        message: Short note or error summary; ``None`` if none.
        extras: Additional key-value pairs shown only in the detail view
            (e.g. command, cwd, exit code).  Use an empty tuple if not needed.
    """

    state: str
    run_id: str
    cluster: str | None = None
    scheduler_id: str | None = None
    elapsed: str | None = None
    message: str | None = None
    dependency_summary: str | None = None
    upstream: tuple[DependencyLine, ...] = ()
    downstream: tuple[DependencyLine, ...] = ()
    extras: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class DependencyLine:
    """Single dependency relation rendered in the detail view."""

    marker: str
    job_id: str
    dependency_type: str
    job_state: str
    scheduler_dependency: str | None = None


@dataclass(frozen=True)
class DashboardState:
    """Immutable snapshot passed to :class:`RunDashboard` on every tick."""

    title: str
    overall_status: str  # "running" | "pending" | "done" | "failed" | "mixed"
    total: int
    running: int
    pending: int
    done: int
    failed: int
    updated_at: str  # e.g. "14:32:07"
    jobs: tuple[JobRow, ...] = ()


# ── Style maps ────────────────────────────────────────────────────────────────

_STATE_STYLE: dict[str, str] = {
    "running": "bold cyan",
    "pending": "yellow",
    "done": "bold green",
    "succeeded": "bold green",
    "dry_run": "dim",
    "failed": "bold red",
    "cancelled": "dim",
    "timed_out": "bold red",
    "lost": "bold red",
    "mixed": "bold yellow",
    "blocked": "yellow",
    "skipped": "dim",
}

_STATUS_ICON: dict[str, str] = {
    "running": "⟳",
    "pending": "·",
    "done": "✓",
    "succeeded": "✓",
    "failed": "✗",
    "mixed": "~",
}


# ── UI state (shared between key-reader thread and render loop) ───────────────


class _UIState:
    """Thread-safe navigation state for the dashboard."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._selected = 0
        self._detail = False
        self._total = 0

    @property
    def selected(self) -> int:
        with self._lock:
            return self._selected

    @property
    def detail(self) -> bool:
        with self._lock:
            return self._detail

    def move_up(self) -> None:
        with self._lock:
            if self._selected > 0:
                self._selected -= 1

    def move_down(self) -> None:
        with self._lock:
            if self._selected < self._total - 1:
                self._selected += 1

    def toggle_detail(self) -> None:
        with self._lock:
            if self._detail:
                self._detail = False
            elif self._total > 0:
                self._detail = True

    def exit_detail(self) -> None:
        with self._lock:
            self._detail = False

    def update_total(self, total: int) -> None:
        with self._lock:
            self._total = total
            if total == 0:
                self._selected = 0
                self._detail = False
            elif self._selected >= total:
                self._selected = total - 1


# ── Dashboard ─────────────────────────────────────────────────────────────────


class RunDashboard:
    """Full-screen terminal dashboard.

    Layout::

        ┌─────────────────────────────────────────┐  header   (3 lines)
        │  ⟳ title  [RUNNING]   updated 14:32:07  │
        ├─────────────────────────────────────────┤  overview (4 lines)
        │  Overview                               │
        │  12 total  2 running  …   ████████░░░   │
        ├─────────────────────────────────────────┤  jobs / detail (remaining)
        │  Jobs                                   │
        │  ▶ RUNNING  abc123  hpc  88001  1m 23s  │  ← selected
        │    PENDING  def456  hpc  —      —       │
        ├─────────────────────────────────────────┤  footer (1 line)
        │  [q] quit  [↑↓] navigate  [↵] detail   │
        └─────────────────────────────────────────┘
    """

    def __init__(self, console: Console | None = None) -> None:
        self._console = console or Console()

    # ── Public API ────────────────────────────────────────────────────────

    def watch(
        self,
        data_fn: Callable[[], DashboardState],
        *,
        refresh_interval: float = 2.0,
    ) -> None:
        """Open the full-screen dashboard and block until ``q`` is pressed.

        Closing the monitor does **not** cancel any running jobs.

        Args:
            data_fn: Returns a fresh :class:`DashboardState` on each tick.
            refresh_interval: Seconds between data refreshes.
        """
        stop = threading.Event()
        ui = _UIState()

        def _key_reader() -> None:
            if not sys.stdin.isatty():
                return
            fd = sys.stdin.fileno()
            saved = None
            try:
                saved = termios.tcgetattr(fd)
                tty.setcbreak(fd)
                while not stop.is_set():
                    ready, _, _ = select.select([fd], [], [], 0.1)
                    if not ready:
                        continue
                    ch = os.read(fd, 1).decode("utf-8", errors="replace")

                    if ch in ("q", "Q", "\x03"):  # quit
                        stop.set()
                        break

                    elif ch == "\x1b":  # escape sequence or bare Esc
                        r2, _, _ = select.select([fd], [], [], 0.05)
                        if not r2:
                            ui.exit_detail()  # bare Esc → back to list
                            continue
                        ch2 = os.read(fd, 1).decode("utf-8", errors="replace")
                        if ch2 != "[":
                            continue
                        r3, _, _ = select.select([fd], [], [], 0.05)
                        if not r3:
                            continue
                        ch3 = os.read(fd, 1).decode("utf-8", errors="replace")
                        if ch3 == "A":  # ↑
                            ui.move_up()
                        elif ch3 == "B":  # ↓
                            ui.move_down()

                    elif ch in ("\r", "\n"):  # Enter → toggle detail
                        ui.toggle_detail()

            finally:
                if saved is not None:
                    termios.tcsetattr(fd, termios.TCSADRAIN, saved)

        key_thread = threading.Thread(target=_key_reader, daemon=True)
        key_thread.start()

        try:
            state = data_fn()
            ui.update_total(len(state.jobs))
            with Live(
                self._render(state, ui),
                console=self._console,
                refresh_per_second=20,
                screen=True,
            ) as live:
                last_refresh = time.monotonic()
                while not stop.is_set():
                    if time.monotonic() - last_refresh >= refresh_interval:
                        state = data_fn()
                        ui.update_total(len(state.jobs))
                        last_refresh = time.monotonic()
                    # Render every 50 ms so arrow-key selection feels instant
                    live.update(self._render(state, ui))
                    time.sleep(0.05)
        finally:
            stop.set()
            # Give the key reader a chance to restore termios state.  The
            # reader polls stdin every 100 ms so a few hundred ms is plenty,
            # but allow a little extra slack on slow hosts.
            key_thread.join(timeout=2.0)
            if key_thread.is_alive():
                # Last-resort: forcibly restore the terminal so the user does
                # not end up in raw/cbreak mode after Ctrl-C.
                try:
                    fd = sys.stdin.fileno()
                    saved_attrs = termios.tcgetattr(fd)
                    termios.tcsetattr(fd, termios.TCSADRAIN, saved_attrs)
                except Exception:
                    pass

    # ── Rendering ─────────────────────────────────────────────────────────

    def _render(self, state: DashboardState, ui: _UIState) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="overview", size=4),
            Layout(name="main"),
            Layout(name="footer", size=1),
        )
        layout["header"].update(self._render_header(state))
        layout["overview"].update(self._render_overview(state))

        sel = ui.selected
        if ui.detail and state.jobs:
            layout["main"].update(self._render_detail(state.jobs[sel]))
        else:
            layout["main"].update(self._render_jobs(state, sel))

        layout["footer"].update(self._render_footer(ui.detail))
        return layout

    def _render_header(self, state: DashboardState) -> Panel:
        icon = _STATUS_ICON.get(state.overall_status, "·")
        style = _STATE_STYLE.get(state.overall_status, "white")
        badge = style.replace("bold ", "")

        line = Text()
        line.append(f" {icon} ", style=style)
        line.append(state.title, style="bold white")
        line.append("  ")
        line.append(f"[{state.overall_status.upper()}]", style=f"bold {badge}")
        line.append(f"   updated {state.updated_at}", style="dim")
        return Panel(line, style="dim")

    def _render_overview(self, state: DashboardState) -> Panel:
        stats = Text()

        def _seg(count: int, label: str, style: str) -> None:
            stats.append(str(count), style=f"bold {style}")
            stats.append(f" {label}", style="dim")

        _seg(state.total, "total", "white")
        stats.append("   ")
        _seg(state.running, "running", "cyan")
        stats.append("   ")
        _seg(state.pending, "pending", "yellow")
        stats.append("   ")
        _seg(state.done, "done", "green")
        stats.append("   ")
        _seg(state.failed, "failed", "red")

        prog = Progress(
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            expand=True,
        )
        prog.add_task(
            "", total=max(state.total, 1), completed=state.done + state.failed
        )

        return Panel(Group(stats, prog), title="[bold]Overview[/bold]", padding=(0, 2))

    def _render_jobs(self, state: DashboardState, selected: int) -> Panel:
        table = Table(
            show_header=True,
            header_style="dim",
            show_edge=False,
            box=None,
            expand=True,
            padding=(0, 2),
        )
        table.add_column("STATE", width=13, no_wrap=True, justify="left")
        table.add_column("JOB ID", ratio=3, no_wrap=True, justify="left")
        table.add_column("CLUSTER", ratio=2, no_wrap=True, justify="left")
        table.add_column("SCHED ID", ratio=2, no_wrap=True, justify="left")
        table.add_column("ELAPSED", width=10, no_wrap=True, justify="left")
        table.add_column("DEPS", width=10, no_wrap=True, justify="left")
        table.add_column("NOTE", ratio=3, justify="left")

        for i, job in enumerate(state.jobs):
            style = _STATE_STYLE.get(job.state.lower(), "white")
            is_sel = i == selected
            row_style = "on grey19" if is_sel else ""
            marker = "▶ " if is_sel else "  "

            table.add_row(
                Text(marker + job.state.upper(), style=style),
                Text(job.run_id, style="white"),
                Text(job.cluster or "—", style="dim"),
                Text(job.scheduler_id or "—", style="dim"),
                Text(job.elapsed or "—", style="dim"),
                Text(job.dependency_summary or "—", style="dim"),
                Text(job.message or "", style="dim"),
                style=row_style,
            )

        if not state.jobs:
            table.add_row(
                Text("—", style="dim"),
                Text("no jobs", style="dim"),
                *[Text("", style="dim")] * 5,
            )

        return Panel(table, title="[bold]Jobs[/bold]", padding=(0, 1))

    def _render_detail(self, job: JobRow) -> Panel:
        grid = Table.grid(padding=(0, 3))
        grid.add_column(justify="right", style="dim", min_width=14)
        grid.add_column(style="white")

        state_style = _STATE_STYLE.get(job.state.lower(), "white")

        def _kv(key: str, val: str, val_style: str = "white") -> None:
            grid.add_row(key, Text(val, style=val_style))

        _kv("state", job.state.upper(), state_style)
        _kv("job id", job.run_id)
        if job.cluster:
            _kv("cluster", job.cluster)
        if job.scheduler_id:
            _kv("sched id", job.scheduler_id)
        if job.elapsed:
            _kv("elapsed", job.elapsed)
        if job.message:
            _kv("note", job.message, "bold red")
        if job.dependency_summary:
            _kv("deps", job.dependency_summary, "dim")
        for key, val in job.extras:
            _kv(key, val)

        if job.upstream:
            grid.add_row("", Text(""))
            grid.add_row("upstream", Text(""))
            for dep in job.upstream:
                line = (
                    f"{dep.marker} {dep.job_id}  {dep.dependency_type}  {dep.job_state}"
                )
                if dep.scheduler_dependency:
                    line += f"  {dep.scheduler_dependency}"
                grid.add_row("", Text(line, style=_dependency_line_style(dep.marker)))

        if job.downstream:
            grid.add_row("", Text(""))
            grid.add_row("downstream", Text(""))
            for dep in job.downstream:
                line = (
                    f"{dep.marker} {dep.job_id}  {dep.dependency_type}  {dep.job_state}"
                )
                grid.add_row("", Text(line, style=_dependency_line_style(dep.marker)))

        return Panel(
            grid,
            title=f"[bold]Detail — {job.run_id}[/bold]",
            padding=(1, 3),
        )

    def _render_footer(self, detail: bool) -> Text:
        if detail:
            hint = "  [q] quit   [↑↓] prev/next job   [↵ / Esc] back to list"
        else:
            hint = "  [q] quit   [↑↓] navigate   [↵] open detail"
        return Text(hint, style="dim", justify="left")


# ── Helpers for MolqMonitor ───────────────────────────────────────────────────


def _elapsed_ts(
    submitted_at: float | None, finished_at: float | None = None
) -> str | None:
    """Compute elapsed time from UNIX epoch timestamps."""
    if submitted_at is None:
        return None
    from datetime import datetime

    start = datetime.fromtimestamp(submitted_at)
    end = datetime.fromtimestamp(finished_at) if finished_at else datetime.now()
    secs = max(0, int((end - start).total_seconds()))
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _molq_overall_status(running: int, pending: int, failed: int, done: int) -> str:
    if running > 0:
        return "running"
    if pending > 0:
        return "pending"
    if failed > 0 and done == 0:
        return "failed"
    if failed > 0:
        return "mixed"
    return "done"


def _dependency_summary(preview: object | None) -> str | None:
    if preview is None:
        return None
    upstream_total = getattr(preview, "upstream_total", 0)
    upstream_satisfied = getattr(preview, "upstream_satisfied", 0)
    downstream_total = getattr(preview, "downstream_total", 0)

    if upstream_total and downstream_total:
        return f"{upstream_satisfied}/{upstream_total} +{downstream_total}"
    if upstream_total:
        return f"{upstream_satisfied}/{upstream_total} ok"
    if downstream_total:
        return f"-> {downstream_total}"
    return None


def _dependency_marker(relation_state: str) -> str:
    return {
        "satisfied": "✓",
        "pending": "·",
        "impossible": "!",
    }.get(relation_state, "·")


def _dependency_line_style(marker: str) -> str:
    return {
        "✓": "green",
        "·": "yellow",
        "!": "red",
    }.get(marker, "white")


# ── MolqMonitor ───────────────────────────────────────────────────────────────


class MolqMonitor:
    """Full-screen dashboard for all molq jobs across all clusters.

    Reads from :class:`~molq.store.JobStore` on every refresh tick.

    Args:
        db_path: SQLite database path.  ``None`` → ``~/.molq/jobs.db``.
        include_terminal: Show completed/failed jobs too.  Default ``False``.
        limit: Maximum job rows displayed.  Default 200.
        refresh_interval: Seconds between data refreshes.
    """

    def __init__(
        self,
        db_path: str | None = None,
        *,
        include_terminal: bool = False,
        limit: int = 200,
        refresh_interval: float = 2.0,
    ) -> None:
        self._db_path = db_path
        self._include_terminal = include_terminal
        self._limit = limit
        self._refresh_interval = refresh_interval

    def watch(self) -> None:
        """Open the full-screen dashboard and block until ``q`` is pressed."""
        from molq.store import JobStore

        store = JobStore(self._db_path)
        try:
            self._run_dashboard(store)
        finally:
            store.close()

    def _run_dashboard(self, store: JobStore) -> None:
        from datetime import datetime

        from molq.status import JobState

        _TERMINAL = frozenset(
            {
                JobState.SUCCEEDED,
                JobState.FAILED,
                JobState.CANCELLED,
                JobState.TIMED_OUT,
                JobState.LOST,
            }
        )
        _ACTIVE = frozenset({JobState.RUNNING})
        _PENDING = frozenset({JobState.CREATED, JobState.SUBMITTED, JobState.QUEUED})

        def _build_state() -> DashboardState:
            records = store.list_all_records(
                include_terminal=self._include_terminal,
                limit=self._limit,
            )
            previews = store.get_dependency_previews([rec.job_id for rec in records])
            rows: list[JobRow] = []
            running = pending = done = failed = 0

            for rec in records:
                elapsed = _elapsed_ts(rec.submitted_at, rec.finished_at)
                preview = previews.get(rec.job_id)

                # Build extras for the detail view
                extras: list[tuple[str, str]] = [
                    ("scheduler", rec.scheduler),
                    ("command", rec.command_display),
                    ("cwd", rec.cwd),
                    ("full id", rec.job_id),
                ]
                if rec.exit_code is not None:
                    extras.append(("exit code", str(rec.exit_code)))
                if rec.metadata:
                    for k, v in rec.metadata.items():
                        extras.append((k, str(v)))

                rows.append(
                    JobRow(
                        state=rec.state.value,
                        run_id=rec.job_id[:16],
                        cluster=rec.cluster_name,
                        scheduler_id=rec.scheduler_job_id,
                        elapsed=elapsed,
                        message=rec.failure_reason,
                        dependency_summary=_dependency_summary(preview),
                        upstream=tuple(
                            DependencyLine(
                                marker=_dependency_marker(item.relation_state),
                                job_id=item.job_id,
                                dependency_type=item.dependency_type,
                                job_state=item.job_state.value,
                                scheduler_dependency=item.scheduler_dependency,
                            )
                            for item in (
                                preview.upstream if preview is not None else ()
                            )
                        ),
                        downstream=tuple(
                            DependencyLine(
                                marker=_dependency_marker(item.relation_state),
                                job_id=item.job_id,
                                dependency_type=item.dependency_type,
                                job_state=item.job_state.value,
                            )
                            for item in (
                                preview.downstream if preview is not None else ()
                            )
                        ),
                        extras=tuple(extras),
                    )
                )

                if rec.state in _ACTIVE:
                    running += 1
                elif rec.state in _PENDING:
                    pending += 1
                elif rec.state == JobState.SUCCEEDED:
                    done += 1
                elif rec.state in _TERMINAL - {JobState.SUCCEEDED}:
                    failed += 1
                else:
                    pending += 1

            title = "molq  [all jobs]" if self._include_terminal else "molq"

            return DashboardState(
                title=title,
                overall_status=_molq_overall_status(running, pending, failed, done),
                total=len(rows),
                running=running,
                pending=pending,
                done=done,
                failed=failed,
                updated_at=datetime.now().strftime("%H:%M:%S"),
                jobs=tuple(rows),
            )

        RunDashboard().watch(_build_state, refresh_interval=self._refresh_interval)
