# Spec: Monitoring System Redesign

## Summary

Complete redesign of molq's job monitoring subsystem. Replace the current ad-hoc blocking/polling with a layered architecture: an enriched status model, a reconciliation engine that syncs scheduler state with persistent DB, pluggable polling strategies, a callback/event system, a background daemon, and a Rich-based live dashboard.

## Motivation

The current monitoring system has 8 critical defects (see code review above). The root cause is that monitoring was never designed as a first-class subsystem — it's a `while True: sleep(2)` loop bolted onto the submitter. This redesign extracts monitoring into its own module hierarchy with clear separation of concerns.

## Design

### New Module Structure

```
src/molq/
├── status.py          # NEW: JobStatus enum + JobRecord model + StatusChange
├── reconciler.py      # NEW: Sync scheduler ↔ DB state
├── monitor.py         # NEW: Core monitoring engine
├── strategies.py      # NEW: Pluggable polling strategies
├── dashboard.py       # NEW: Rich-based live display
├── callbacks.py       # NEW: Event/callback system
├── lifecycle.py       # MODIFIED: Enhanced schema, WAL mode, indexes
├── jobstatus.py       # DEPRECATED: Replaced by status.py
├── adapters.py        # MODIFIED: Add batch_query() + terminal_status()
├── submitor/base.py   # MODIFIED: Delegate monitoring to JobMonitor
├── cli/main.py        # MODIFIED: Add watch/daemon/history commands
└── tempfiles.py       # MODIFIED: Persist tracking to DB
```

### Layer Diagram

```
┌──────────────────────────────────────────────────┐
│                   CLI / User Code                │
│  molq watch · molq daemon · submitor.submit()    │
└──────────────┬────────────────────┬──────────────┘
               │                    │
       ┌───────▼────────┐  ┌───────▼────────┐
       │  JobMonitor     │  │  JobDashboard   │
       │  watch/daemon   │  │  Rich Live UI   │
       └───────┬────────┘  └───────┬────────┘
               │                    │
       ┌───────▼────────────────────▼──────────┐
       │          StatusReconciler              │
       │  scheduler.query() ⇄ DB diff ⇄ update │
       └───────┬───────────────────┬───────────┘
               │                   │
    ┌──────────▼──────┐  ┌────────▼──────────┐
    │ SchedulerAdapter │  │ JobLifecycleManager│
    │ (squeue/ps/...)  │  │ (SQLite WAL)       │
    └─────────────────┘  └───────────────────┘
```

---

## 1. `status.py` — Enhanced Status Model

### JobStatus Enum

```python
class JobStatus(str, Enum):
    """Terminal-aware job status."""
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    CANCELLED = "cancelled"
    TIMEOUT   = "timeout"
    UNKNOWN   = "unknown"

    @property
    def is_terminal(self) -> bool:
        return self in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMEOUT,
        )
```

Replace the current `JobStatus` class (which confusingly mixes enum with instance state) with a clean enum. Drop `FINISHED` — use `COMPLETED` for success, `FAILED` for error.

### JobRecord

```python
class JobRecord(BaseModel):
    """Immutable snapshot of a job's full lifecycle state."""
    model_config = ConfigDict(frozen=True)

    job_id: int
    section: str
    name: str
    status: JobStatus
    command: str
    workdir: str
    submit_time: float
    start_time: float | None = None
    end_time: float | None = None
    last_polled: float | None = None
    exit_code: int | None = None
    error_message: str | None = None
    scheduler_data: dict[str, str] = Field(default_factory=dict)

    @property
    def elapsed(self) -> float | None:
        if self.start_time is None:
            return None
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def is_stale(self) -> bool:
        """True if last_polled is more than 5 minutes ago."""
        if self.last_polled is None:
            return True
        return (time.time() - self.last_polled) > 300
```

### StatusChange

```python
class StatusChange(BaseModel):
    """Immutable record of a single status transition."""
    model_config = ConfigDict(frozen=True)

    job_id: int
    section: str
    old_status: JobStatus
    new_status: JobStatus
    timestamp: float
    reason: str | None = None
```

---

## 2. `reconciler.py` — State Reconciliation

The core problem: scheduler state and DB state drift. The reconciler is the single place where they're compared and aligned.

```python
class StatusReconciler:
    """Single source of truth for job status transitions.

    Compares scheduler-reported state against persisted DB state,
    determines transitions, updates DB, and returns changes.
    """

    def __init__(
        self,
        adapter: SchedulerAdapter,
        lifecycle: JobLifecycleManager,
        section: str,
    ):
        self.adapter = adapter
        self.lifecycle = lifecycle
        self.section = section

    def reconcile(self) -> list[StatusChange]:
        """Run one reconciliation cycle.

        1. Load all non-terminal jobs from DB
        2. Batch-query scheduler for their status
        3. For jobs missing from scheduler, infer terminal status
        4. Diff old vs new, update DB, return changes
        """
        active_jobs = self.lifecycle.list_active_jobs(self.section)
        if not active_jobs:
            return []

        # Batch query — one subprocess call, not N
        scheduler_states = self.adapter.query()
        now = time.time()
        changes: list[StatusChange] = []

        for job in active_jobs:
            job_id = job.job_id
            old_status = job.status

            if job_id in scheduler_states:
                new_status = self._map_scheduler_status(scheduler_states[job_id])
            else:
                # Job disappeared from scheduler → terminal
                new_status = self._infer_terminal_status(job)

            self.lifecycle.touch(self.section, job_id, last_polled=now)

            if new_status != old_status:
                end_time = now if new_status.is_terminal else None
                self.lifecycle.update_status(
                    self.section, job_id,
                    status=new_status,
                    end_time=end_time,
                )
                changes.append(StatusChange(
                    job_id=job_id,
                    section=self.section,
                    old_status=old_status,
                    new_status=new_status,
                    timestamp=now,
                ))

        return changes

    def _infer_terminal_status(self, job: JobRecord) -> JobStatus:
        """Determine why a job disappeared from the scheduler.

        For SLURM: call sacct to get exit code.
        For local: check /proc/<pid> or exit code from waitpid.
        Fallback: if job was PENDING, assume CANCELLED;
                  if RUNNING, assume COMPLETED.
        """
        terminal = self.adapter.terminal_status(job.job_id)
        if terminal is not None:
            return terminal

        # Heuristic fallback
        if job.status == JobStatus.PENDING:
            return JobStatus.CANCELLED
        return JobStatus.COMPLETED
```

### Required Adapter Extension

Add `terminal_status()` and `batch_query()` to the protocol:

```python
class SchedulerAdapter(Protocol):
    def submit(self, spec: JobSpec) -> int: ...
    def query(self, job_id: int | None = None) -> dict[int, SchedulerJobInfo]: ...
    def cancel(self, job_id: int) -> None: ...

    def terminal_status(self, job_id: int) -> JobStatus | None:
        """Query completed job's terminal status (e.g., via sacct).

        Returns None if unknown.
        """
        ...
```

**SlurmAdapter.terminal_status()** — calls `sacct -j <id> -o State,ExitCode -n -P`:
- `COMPLETED|0:0` → COMPLETED
- `FAILED|1:0` → FAILED
- `CANCELLED|0:0` → CANCELLED
- `TIMEOUT|0:0` → TIMEOUT

**LocalAdapter.terminal_status()** — checks `/proc/<pid>`:
- PID not found → COMPLETED (or try `wait4` for exit code)
- Zombie → read exit code from `waitpid`

---

## 3. `strategies.py` — Polling Strategies

```python
class PollingStrategy(Protocol):
    def next_interval(self, elapsed: float, poll_count: int) -> float:
        """Return seconds to sleep before next poll."""
        ...

class FixedStrategy:
    """Fixed interval. Simple but wasteful for long jobs."""
    def __init__(self, interval: float = 5.0): ...

class ExponentialBackoffStrategy:
    """Start fast, slow down over time. Good general-purpose default."""
    def __init__(
        self,
        initial: float = 1.0,
        maximum: float = 60.0,
        factor: float = 1.5,
    ):
        ...

    def next_interval(self, elapsed: float, poll_count: int) -> float:
        return min(self.initial * (self.factor ** poll_count), self.maximum)

class AdaptiveStrategy:
    """Adapts interval based on expected job duration from time_limit.

    For a 1-minute job: poll every 1-2s
    For a 1-hour job: poll every 30s
    For a 24-hour job: poll every 5min
    """
    def __init__(self, expected_duration: float | None = None): ...

    def next_interval(self, elapsed: float, poll_count: int) -> float:
        if self.expected_duration is None:
            # Fall back to exponential
            return min(1.0 * (1.5 ** poll_count), 60.0)
        # ~1% of expected duration, clamped to [1s, 300s]
        return max(1.0, min(self.expected_duration * 0.01, 300.0))
```

---

## 4. `callbacks.py` — Event System

```python
class EventType(str, Enum):
    STATUS_CHANGE = "status_change"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    ALL_COMPLETED = "all_completed"

class EventBus:
    """Pub/sub for job lifecycle events."""

    def __init__(self):
        self._handlers: dict[EventType, list[Callable]] = defaultdict(list)

    def on(self, event: EventType, handler: Callable) -> None:
        self._handlers[event].append(handler)

    def emit(self, event: EventType, data: StatusChange | JobRecord) -> None:
        for handler in self._handlers.get(event, []):
            try:
                handler(data)
            except Exception:
                pass  # log but don't break monitoring
```

Example usage:
```python
monitor = JobMonitor(...)
monitor.events.on(EventType.JOB_FAILED, lambda change: send_email(change))
monitor.events.on(EventType.ALL_COMPLETED, lambda _: print("All done!"))
```

---

## 5. `monitor.py` — Core Monitoring Engine

```python
class JobMonitor:
    """Core monitoring engine. Coordinates reconciliation, strategies, events."""

    def __init__(
        self,
        reconciler: StatusReconciler,
        lifecycle: JobLifecycleManager,
        strategy: PollingStrategy | None = None,
        events: EventBus | None = None,
    ):
        self.reconciler = reconciler
        self.lifecycle = lifecycle
        self.strategy = strategy or ExponentialBackoffStrategy()
        self.events = events or EventBus()
        self._stop = threading.Event()

    def poll_once(self) -> list[StatusChange]:
        """Single reconciliation cycle. Returns status changes."""
        changes = self.reconciler.reconcile()
        for change in changes:
            self.events.emit(EventType.STATUS_CHANGE, change)
            if change.new_status == JobStatus.COMPLETED:
                self.events.emit(EventType.JOB_COMPLETED, change)
            elif change.new_status == JobStatus.FAILED:
                self.events.emit(EventType.JOB_FAILED, change)
            elif change.new_status == JobStatus.RUNNING:
                self.events.emit(EventType.JOB_STARTED, change)
        return changes

    def watch_one(
        self,
        job_id: int,
        *,
        timeout: float | None = None,
        progress: bool = True,
    ) -> JobRecord:
        """Block until a single job reaches terminal state.

        Args:
            job_id: Job to watch.
            timeout: Max seconds to wait. None = no limit.
            progress: Print status updates.

        Returns:
            Final JobRecord.

        Raises:
            TimeoutError: If timeout exceeded.
            KeyboardInterrupt: Propagated cleanly with DB in consistent state.
        """
        start = time.time()
        poll_count = 0

        try:
            while not self._stop.is_set():
                changes = self.poll_once()
                record = self.lifecycle.get_job_record(
                    self.reconciler.section, job_id
                )

                if record is None or record.status.is_terminal:
                    return record

                if timeout and (time.time() - start) > timeout:
                    raise TimeoutError(
                        f"Job {job_id} did not complete within {timeout}s"
                    )

                interval = self.strategy.next_interval(
                    time.time() - start, poll_count
                )
                self._stop.wait(interval)  # interruptible sleep
                poll_count += 1
        except KeyboardInterrupt:
            # DB is already consistent (reconcile committed)
            raise

        return self.lifecycle.get_job_record(
            self.reconciler.section, job_id
        )

    def watch_all(
        self,
        section: str | None = None,
        *,
        timeout: float | None = None,
    ) -> list[JobRecord]:
        """Block until all active jobs reach terminal state."""
        start = time.time()
        poll_count = 0

        try:
            while not self._stop.is_set():
                self.poll_once()
                active = self.lifecycle.list_active_jobs(
                    section or self.reconciler.section
                )

                if not active:
                    self.events.emit(EventType.ALL_COMPLETED, None)
                    break

                if timeout and (time.time() - start) > timeout:
                    raise TimeoutError(
                        f"{len(active)} jobs still active after {timeout}s"
                    )

                interval = self.strategy.next_interval(
                    time.time() - start, poll_count
                )
                self._stop.wait(interval)
                poll_count += 1
        except KeyboardInterrupt:
            raise

        return self.lifecycle.list_jobs_as_records(
            section or self.reconciler.section
        )

    def stop(self):
        """Signal the monitor to stop after current cycle."""
        self._stop.set()

    def run_daemon(self, *, pid_file: Path | None = None):
        """Run as a long-lived background process.

        Writes PID to pid_file for management.
        Polls indefinitely until stopped.
        """
        pid_file = pid_file or Path.home() / ".molq" / "monitor.pid"

        pid_file.write_text(str(os.getpid()))
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT, lambda *_: self.stop())

        try:
            while not self._stop.is_set():
                self.poll_once()
                active = self.lifecycle.list_active_jobs(
                    self.reconciler.section
                )
                if not active:
                    # No active jobs — increase interval significantly
                    self._stop.wait(60)
                else:
                    interval = self.strategy.next_interval(0, 0)
                    self._stop.wait(interval)
        finally:
            pid_file.unlink(missing_ok=True)
```

---

## 6. `dashboard.py` — Rich Live Dashboard

```python
class JobDashboard:
    """Terminal-based live job status display."""

    def __init__(self, monitor: JobMonitor, lifecycle: JobLifecycleManager):
        self.monitor = monitor
        self.lifecycle = lifecycle

    def run(self, section: str | None = None):
        """Show live-updating dashboard until all jobs complete or Ctrl+C."""
        from rich.live import Live

        try:
            with Live(
                self._build_layout(section),
                refresh_per_second=1,
                screen=False,
            ) as live:
                while True:
                    self.monitor.poll_once()
                    live.update(self._build_layout(section))

                    active = self.lifecycle.list_active_jobs(section)
                    if not active:
                        break

                    time.sleep(
                        self.monitor.strategy.next_interval(0, 0)
                    )
        except KeyboardInterrupt:
            pass

    def _build_layout(self, section: str | None) -> Table:
        """Build Rich Table from current job state."""
        from rich.table import Table

        table = Table(title="Molq Jobs", expand=True)
        table.add_column("ID", style="magenta", width=10)
        table.add_column("Name", style="cyan", width=20)
        table.add_column("Status", width=12)
        table.add_column("Elapsed", width=12)
        table.add_column("Section", style="dim", width=15)

        jobs = self.lifecycle.list_jobs_as_records(section)
        for job in jobs:
            status_style = {
                JobStatus.PENDING: "yellow",
                JobStatus.RUNNING: "green bold",
                JobStatus.COMPLETED: "blue",
                JobStatus.FAILED: "red bold",
                JobStatus.CANCELLED: "dim",
                JobStatus.TIMEOUT: "red",
                JobStatus.UNKNOWN: "dim red",
            }.get(job.status, "white")

            elapsed = ""
            if job.elapsed is not None:
                m, s = divmod(int(job.elapsed), 60)
                h, m = divmod(m, 60)
                elapsed = f"{h:02d}:{m:02d}:{s:02d}"

            table.add_row(
                str(job.job_id),
                job.name,
                f"[{status_style}]{job.status.value}[/]",
                elapsed,
                job.section,
            )

        return table
```

---

## 7. Enhanced Lifecycle Manager Changes

```sql
-- New schema
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    section TEXT NOT NULL,
    job_id INTEGER NOT NULL,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    command TEXT,
    workdir TEXT,
    submit_time REAL,
    start_time REAL,
    end_time REAL,
    last_polled REAL,
    exit_code INTEGER,
    error_message TEXT,
    scheduler_data TEXT DEFAULT '{}',
    UNIQUE(section, job_id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_active
    ON jobs(section, status)
    WHERE status IN ('pending', 'running');

-- Status transition log (append-only)
CREATE TABLE IF NOT EXISTS status_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    section TEXT NOT NULL,
    job_id INTEGER NOT NULL,
    old_status TEXT,
    new_status TEXT NOT NULL,
    timestamp REAL NOT NULL,
    reason TEXT
);
```

Key changes:
- `UNIQUE(section, job_id)` — prevents duplicate registrations
- `start_time` — tracked separately from submit_time
- `last_polled` — staleness detection
- `exit_code`, `error_message` — terminal state details
- `status_transitions` table — full audit trail
- WAL mode: `PRAGMA journal_mode=WAL` in `_init_db()`
- Partial index on active jobs for fast reconciliation queries

New methods:
- `list_active_jobs(section) -> list[JobRecord]` — non-terminal only
- `list_jobs_as_records(section) -> list[JobRecord]` — returns Pydantic models
- `touch(section, job_id, last_polled)` — update poll timestamp
- `record_transition(section, job_id, old, new, timestamp, reason)` — audit log

---

## 8. CLI Extensions

```python
@cli.command()
@click.argument("job_id", required=False)
@click.option("--section", help="Filter by section")
@click.option("--timeout", type=float, help="Max seconds to wait")
@click.option("--strategy", type=click.Choice(["fixed", "backoff", "adaptive"]),
              default="backoff")
def watch(job_id, section, timeout, strategy):
    """Live-watch job status. Shows dashboard for all jobs, or blocks on one."""
    ...

@cli.group()
def daemon():
    """Manage background monitoring daemon."""

@daemon.command()
@click.option("--section", help="Section to monitor")
def start(section):
    """Start background monitor daemon."""
    ...

@daemon.command()
def stop():
    """Stop background monitor daemon."""
    ...

@daemon.command()
def status():
    """Check daemon status."""
    ...

@cli.command()
@click.option("--section", help="Filter by section")
@click.option("--limit", type=int, default=50)
def history(section, limit):
    """Show completed job history."""
    ...
```

---

## 9. BaseSubmitor Integration

Replace inline blocking with monitor delegation:

```python
class BaseSubmitor:
    def __init__(self, ...):
        ...
        self._monitor = None  # lazy

    @property
    def monitor(self) -> JobMonitor:
        if self._monitor is None:
            reconciler = StatusReconciler(
                self.adapter, self.lifecycle_manager, self.section
            )
            self._monitor = JobMonitor(
                reconciler, self.lifecycle_manager,
                strategy=ExponentialBackoffStrategy(),
            )
        return self._monitor

    def _handle_post_submit(self, job_id, spec):
        if spec.execution.block:
            self.monitor.watch_one(job_id)
            if spec.execution.cleanup_temp_files:
                self.temp_manager.cleanup(job_id)
        return job_id
```

The old `block_one_until_complete()` and `block_all_until_complete()` become thin wrappers around `monitor.watch_one()` / `monitor.watch_all()` for backward compatibility.

---

## 10. TempFileManager Persistence

Move tracking from in-memory dict to SQLite:

```sql
CREATE TABLE IF NOT EXISTS temp_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    section TEXT NOT NULL,
    filepath TEXT NOT NULL,
    created_at REAL
);
```

This survives process restart — cleanup can happen when the monitor detects a terminal state, even across sessions.

---

## Migration Plan

### Phase 1: Foundation (non-breaking)
1. Create `status.py` with new `JobStatus` enum and `JobRecord`
2. Create `strategies.py` with polling strategies
3. Create `callbacks.py` with EventBus
4. Migrate lifecycle.py schema (add columns, keep old working)
5. Tests for all new modules

### Phase 2: Core engine (internal refactor)
6. Create `reconciler.py`
7. Create `monitor.py`
8. Add `terminal_status()` to adapters
9. Wire `BaseSubmitor._handle_post_submit` to use `JobMonitor`
10. Deprecate old `block_*` methods (keep as wrappers)

### Phase 3: User-facing (new features)
11. Create `dashboard.py`
12. Add CLI commands: `watch`, `daemon`, `history`
13. Persist temp file tracking to DB
14. Remove `local_jobs.json` tracking
15. Fix CLI `list` command

### Phase 4: Cleanup
16. Remove old `jobstatus.py` (replace all imports)
17. Remove `_record_local_job_id` / `local_jobs.json`
18. Remove inline `block_*` implementations

## Testing Strategy

### Unit Tests
- StatusReconciler: mock adapter + lifecycle, verify correct transitions
- JobMonitor.watch_one: mock reconciler, verify timeout/interrupt behavior
- PollingStrategy: verify interval calculations
- EventBus: verify callback dispatch and error isolation
- Dashboard: mock lifecycle, verify table rendering

### Integration Tests
- Full cycle: submit → reconcile → terminal state → cleanup
- Process restart: submit, kill, restart, reconcile → correct terminal status
- Concurrent submissions: multiple submitters sharing same DB (WAL mode)

### Edge Cases
- Job never appears in scheduler (submit failed silently)
- Scheduler unreachable during reconcile (network error)
- DB locked by another process
- PID reuse for local jobs
- SLURM array jobs (multiple sub-IDs)
- Ctrl+C during watch_one (DB consistency)
- Daemon start when already running

## Open Questions

1. **Should the daemon be a separate process or a thread?**
   Recommendation: separate process via `subprocess.Popen` (or `fork`). Threads share GIL and can't survive parent exit.

2. **Should we support webhooks for status changes?**
   Defer to Phase 5. The EventBus is extensible — a webhook handler is just another callback.

3. **Should `sacct` be required for SLURM terminal status?**
   Make it best-effort: try sacct, fall back to heuristic. Some clusters restrict sacct access.
