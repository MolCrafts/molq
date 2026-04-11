"""Public Submitor API for molq.

Provides the Submitor class (single entry point for job submission
and management) and JobHandle (lightweight handle for a submitted job).
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from molq.errors import (
    CommandError,
    ConfigError,
    JobNotFoundError,
    ScriptError,
)
from molq.merge import merge_defaults
from molq.models import Command, JobRecord, JobSpec, SubmitorDefaults
from molq.monitor import JobMonitor
from molq.options import OPTIONS_TYPE_MAP, SchedulerOptions
from molq.reconciler import JobReconciler
from molq.scheduler import create_scheduler
from molq.status import JobState
from molq.store import JobStore
from molq.types import JobExecution, JobResources, JobScheduling, Script


class Submitor:
    """Unified interface for job submission and management.

    Each instance represents a connection to a specific cluster.

    Args:
        cluster_name: Identifies this cluster namespace.
        scheduler: One of 'local', 'slurm', 'pbs', 'lsf'.
        defaults: Default resource/scheduling/execution parameters.
        scheduler_options: Scheduler-specific configuration.
        store: Custom JobStore (default: ~/.molq/jobs.db).
    """

    def __init__(
        self,
        cluster_name: str,
        scheduler: str,
        *,
        defaults: SubmitorDefaults | None = None,
        scheduler_options: SchedulerOptions | None = None,
        store: JobStore | None = None,
    ) -> None:
        if scheduler not in OPTIONS_TYPE_MAP:
            raise ConfigError(
                f"Unknown scheduler {scheduler!r}. "
                f"Must be one of: {', '.join(OPTIONS_TYPE_MAP)}",
                scheduler=scheduler,
            )

        if scheduler_options is not None:
            expected_type = OPTIONS_TYPE_MAP[scheduler]
            if not isinstance(scheduler_options, expected_type):
                raise TypeError(
                    f"scheduler_options must be {expected_type.__name__} "
                    f"for scheduler {scheduler!r}, "
                    f"got {type(scheduler_options).__name__}"
                )

        self._cluster_name = cluster_name
        self._scheduler_name = scheduler
        self._defaults = defaults
        self._store = store or JobStore()
        self._scheduler_impl = create_scheduler(scheduler, scheduler_options)

        # Jobs directory for script materialization
        self._jobs_dir = Path.home() / ".molq" / "jobs"
        self._jobs_dir.mkdir(parents=True, exist_ok=True)

        self._reconciler = JobReconciler(
            self._scheduler_impl,
            self._store,
            cluster_name,
            jobs_dir=self._jobs_dir,
        )
        self._monitor: JobMonitor | None = None

    @property
    def cluster_name(self) -> str:
        return self._cluster_name

    @property
    def _monitor_instance(self) -> JobMonitor:
        if self._monitor is None:
            self._monitor = JobMonitor(self._reconciler, self._store)
        return self._monitor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(
        self,
        *,
        argv: list[str] | None = None,
        command: str | None = None,
        script: Script | None = None,
        resources: JobResources | None = None,
        scheduling: JobScheduling | None = None,
        execution: JobExecution | None = None,
        metadata: dict[str, str] | None = None,
    ) -> JobHandle:
        """Submit a job.

        Exactly one of argv, command, or script must be provided.

        Returns:
            JobHandle for the submitted job.
        """
        # 1. Validate command
        cmd = Command.from_submit_args(argv=argv, command=command, script=script)

        # 2. Merge defaults
        m_resources, m_scheduling, m_execution = merge_defaults(
            self._defaults,
            resources=resources,
            scheduling=scheduling,
            execution=execution,
        )

        # 3. Generate job ID and create spec
        job_id = JobSpec.new_job_id()
        cwd = str(m_execution.cwd) if m_execution.cwd else str(Path.cwd())

        spec = JobSpec(
            job_id=job_id,
            cluster_name=self._cluster_name,
            scheduler=self._scheduler_name,
            command=cmd,
            resources=m_resources,
            scheduling=m_scheduling,
            execution=m_execution,
            metadata=metadata or {},
            cwd=cwd,
        )

        # 4. Insert into store (state=CREATED)
        self._store.insert_job(spec)

        # 5. Prepare job directory and materialize scripts
        job_dir = self._jobs_dir / job_id
        job_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

        if cmd.script is not None:
            self._materialize_script(cmd.script, job_dir)

        # 6. Submit to scheduler
        import time

        try:
            scheduler_job_id = self._scheduler_impl.submit(spec, job_dir)
        except Exception:
            self._store.update_job(job_id, state=JobState.FAILED)
            self._store.record_transition(
                job_id,
                JobState.CREATED,
                JobState.FAILED,
                time.time(),
                "submission failed",
            )
            raise

        # 7. Update store with scheduler ID and state
        now = time.time()
        self._store.update_job(
            job_id,
            state=JobState.SUBMITTED,
            scheduler_job_id=scheduler_job_id,
            submitted_at=now,
        )
        self._store.record_transition(
            job_id,
            JobState.CREATED,
            JobState.SUBMITTED,
            now,
            "submitted",
        )

        return JobHandle(
            job_id=job_id,
            cluster_name=self._cluster_name,
            scheduler=self._scheduler_name,
            scheduler_job_id=scheduler_job_id,
            _state=JobState.SUBMITTED,
            _submitor=self,
        )

    def get(self, job_id: str) -> JobRecord:
        """Get a job record by ID.

        Raises:
            JobNotFoundError: If job doesn't exist.
        """
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._cluster_name)
        return record

    def list(self, include_terminal: bool = False) -> list[JobRecord]:
        """List jobs for this cluster."""
        return self._store.list_records(
            self._cluster_name, include_terminal=include_terminal
        )

    def watch(
        self,
        job_ids: list[str] | None = None,
        *,
        timeout: float | None = None,
    ) -> list[JobRecord]:
        """Block until specified jobs (or all active) reach terminal state."""
        return self._monitor_instance.wait_many(
            job_ids,
            self._cluster_name,
            timeout=timeout,
        )

    def cancel(self, job_id: str) -> None:
        """Cancel a job."""
        import time

        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._cluster_name)

        if record.scheduler_job_id:
            self._scheduler_impl.cancel(record.scheduler_job_id)

        now = time.time()
        self._store.update_job(job_id, state=JobState.CANCELLED, finished_at=now)
        self._store.record_transition(
            job_id,
            record.state,
            JobState.CANCELLED,
            now,
            "cancelled by user",
        )

    def refresh(self) -> None:
        """Reconcile all active jobs with the scheduler."""
        self._reconciler.reconcile()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _materialize_script(self, script: Script, job_dir: Path) -> None:
        """Prepare script files in the job directory."""
        if script.variant == "path" and script.file_path:
            if not script.file_path.exists():
                raise ScriptError(
                    f"Script file not found: {script.file_path}",
                    path=str(script.file_path),
                )
            shutil.copy2(script.file_path, job_dir / "user_script.sh")


# ---------------------------------------------------------------------------
# JobHandle
# ---------------------------------------------------------------------------


@dataclass
class JobHandle:
    """Lightweight handle for a submitted job.

    Returned by Submitor.submit(). Provides single-job operations.
    """

    job_id: str
    cluster_name: str
    scheduler: str
    scheduler_job_id: str | None
    _state: JobState
    _submitor: Submitor

    def status(self) -> JobState:
        """Return cached job state (no I/O)."""
        return self._state

    def refresh(self) -> JobHandle:
        """Reconcile with scheduler and return updated handle."""
        new_state = self._submitor._reconciler.reconcile_one(self.job_id)
        if new_state is not None:
            self._state = new_state
        return self

    def wait(self, timeout: float | None = None) -> JobRecord:
        """Block until this job reaches a terminal state."""
        return self._submitor._monitor_instance.wait_one(
            self.job_id,
            timeout=timeout,
        )

    def cancel(self) -> None:
        """Cancel this job."""
        self._submitor.cancel(self.job_id)
        self._state = JobState.CANCELLED
