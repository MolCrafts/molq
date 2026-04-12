"""Public Submitor API for molq.

Provides the Submitor class (single entry point for job submission
and management) and JobHandle (lightweight handle for a submitted job).
"""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from molq.errors import (
    ConfigError,
    JobNotFoundError,
    ScriptError,
)
from molq.merge import merge_defaults
from molq.models import Command, JobRecord, JobSpec, StatusTransition, SubmitorDefaults
from molq.monitor import JobMonitor
from molq.options import OPTIONS_TYPE_MAP, SchedulerOptions
from molq.reconciler import JobReconciler
from molq.scheduler import SchedulerCapabilities, create_scheduler
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
        jobs_dir: Runtime directory for materialized scripts and default logs.
    """

    def __init__(
        self,
        cluster_name: str,
        scheduler: str = "local",
        *,
        defaults: SubmitorDefaults | None = None,
        scheduler_options: SchedulerOptions | None = None,
        store: JobStore | None = None,
        jobs_dir: str | Path | None = None,
        _scheduler: Any | None = None,
    ) -> None:
        if _scheduler is None:
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

            self._scheduler_impl = create_scheduler(scheduler, scheduler_options)
        else:
            self._scheduler_impl = _scheduler

        self._cluster_name = cluster_name
        self._scheduler_name = scheduler
        self._defaults = defaults
        self._store = store or JobStore()
        self._jobs_dir = self._resolve_jobs_dir(jobs_dir)

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
        if cmd.script is not None and cmd.script.variant == "path":
            if cmd.script.file_path is None or not cmd.script.file_path.exists():
                raise ScriptError(
                    f"Script file not found: {cmd.script.file_path}",
                    path=str(cmd.script.file_path) if cmd.script.file_path else None,
                )

        # 2. Merge defaults
        m_resources, m_scheduling, m_execution = merge_defaults(
            self._defaults,
            resources=resources,
            scheduling=scheduling,
            execution=execution,
        )

        # 3. Build the canonical request and validate it before persisting.
        job_id = JobSpec.new_job_id()
        job_dir = self._job_dir_path(job_id)
        requested_execution = m_execution
        cwd = self._resolve_cwd(m_execution.cwd)
        stdout_path = self._resolve_output_path(
            m_execution.output_file, cwd, job_dir, "stdout.log"
        )
        stderr_path = self._resolve_output_path(
            m_execution.error_file, cwd, job_dir, "stderr.log"
        )
        m_execution = replace(
            m_execution,
            cwd=cwd,
            output_file=str(stdout_path),
            error_file=str(stderr_path),
        )
        merged_metadata = dict(metadata or {})
        merged_metadata["molq.job_dir"] = str(job_dir)
        merged_metadata["molq.stdout_path"] = str(stdout_path)
        merged_metadata["molq.stderr_path"] = str(stderr_path)

        # 4. Create the canonical job spec and validate it before persisting.
        spec = JobSpec(
            job_id=job_id,
            cluster_name=self._cluster_name,
            scheduler=self._scheduler_name,
            command=cmd,
            resources=m_resources,
            scheduling=m_scheduling,
            execution=m_execution,
            metadata=merged_metadata,
            cwd=cwd,
        )
        self._validate_spec(spec, requested_execution=requested_execution)

        # 4. Materialize runtime files and persist the initial record.
        job_dir = self._prepare_job_dir(job_id)
        if cmd.script is not None and cmd.script.variant == "path":
            self._materialize_script(cmd.script, job_dir)
        self._store.insert_job(spec)

        # 5. Submit to scheduler
        import time

        try:
            scheduler_job_id = self._scheduler_impl.submit(spec, job_dir)
        except Exception as exc:
            self._store.update_job(
                job_id,
                state=JobState.FAILED,
                finished_at=time.time(),
                failure_reason=str(exc),
            )
            self._store.record_transition(
                job_id,
                JobState.CREATED,
                JobState.FAILED,
                time.time(),
                f"submission failed: {exc}",
            )
            raise

        # 6. Update store with scheduler ID and state
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

    def get_transitions(self, job_id: str) -> list[StatusTransition]:
        """Return the persisted transition timeline for a job."""
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._cluster_name)
        return self._store.get_transitions(job_id)

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

    def close(self) -> None:
        """Release the underlying :class:`JobStore` connection.

        Safe to call multiple times.  After ``close()`` no further methods
        should be invoked on this Submitor.
        """
        store = getattr(self, "_store", None)
        if store is not None:
            store.close()
            self._store = None  # type: ignore[assignment]

    def __enter__(self) -> Submitor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        # Last-resort cleanup: keeps sqlite from emitting ResourceWarning
        # if the user neglected to close()/use a context manager.
        try:
            self.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _materialize_script(self, script: Script, job_dir: Path) -> None:
        """Prepare script files in the job directory."""
        if script.variant == "path" and script.file_path:
            target = job_dir / "user_script.sh"
            shutil.copy2(script.file_path, target)
            target.chmod(0o700)

    def _resolve_jobs_dir(self, jobs_dir: str | Path | None) -> Path:
        if jobs_dir is not None:
            return Path(jobs_dir).expanduser().resolve()

        db_path = getattr(self._store, "db_path", None)
        if isinstance(db_path, Path):
            return db_path.parent / "jobs"

        return Path(tempfile.gettempdir()) / "molq" / "jobs"

    def _prepare_job_dir(self, job_id: str) -> Path:
        self._jobs_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        job_dir = self._job_dir_path(job_id)
        job_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        return job_dir

    def _job_dir_path(self, job_id: str) -> Path:
        return self._jobs_dir / job_id

    def _resolve_cwd(self, cwd: str | Path | None) -> str:
        base = Path(cwd).expanduser() if cwd is not None else Path.cwd()
        return str(base.resolve())

    def _resolve_output_path(
        self,
        path: str | None,
        cwd: str,
        job_dir: Path,
        default_name: str,
    ) -> Path:
        if path is None:
            return job_dir / default_name

        candidate = Path(path).expanduser()
        if candidate.is_absolute():
            return candidate
        return Path(cwd) / candidate

    def _validate_spec(
        self,
        spec: JobSpec,
        *,
        requested_execution: JobExecution,
    ) -> None:
        capabilities = self._scheduler_capabilities()
        unsupported: list[str] = []

        def require(field: str, supported: bool, requested: bool) -> None:
            if requested and not supported:
                unsupported.append(field)

        r, s, e = spec.resources, spec.scheduling, spec.execution
        req_e = requested_execution
        require("execution.cwd", capabilities.supports_cwd, req_e.cwd is not None)
        require("execution.env", capabilities.supports_env, bool(req_e.env))
        require("execution.job_name", capabilities.supports_job_name, e.job_name is not None)
        require("execution.output_file", capabilities.supports_output_file, e.output_file is not None)
        require("execution.error_file", capabilities.supports_error_file, e.error_file is not None)
        require("resources.cpu_count", capabilities.supports_cpu_count, r.cpu_count is not None)
        require("resources.memory", capabilities.supports_memory, r.memory is not None)
        require("resources.gpu_count", capabilities.supports_gpu_count, r.gpu_count is not None)
        require("resources.gpu_type", capabilities.supports_gpu_type, r.gpu_type is not None)
        require("resources.time_limit", capabilities.supports_time_limit, r.time_limit is not None)
        require("scheduling.queue", capabilities.supports_queue, s.queue is not None)
        require("scheduling.account", capabilities.supports_account, s.account is not None)
        require("scheduling.priority", capabilities.supports_priority, s.priority is not None)
        require("scheduling.dependency", capabilities.supports_dependency, s.dependency is not None)
        require("scheduling.node_count", capabilities.supports_node_count, s.node_count is not None)
        require(
            "scheduling.exclusive_node",
            capabilities.supports_exclusive_node,
            s.exclusive_node,
        )
        require("scheduling.array_spec", capabilities.supports_array_jobs, s.array_spec is not None)
        require("scheduling.email", capabilities.supports_email, s.email is not None)
        require("scheduling.qos", capabilities.supports_qos, s.qos is not None)
        require(
            "scheduling.reservation",
            capabilities.supports_reservation,
            s.reservation is not None,
        )

        if unsupported:
            fields = ", ".join(unsupported)
            raise ConfigError(
                f"Scheduler {self._scheduler_name!r} does not support requested fields: {fields}",
                scheduler=self._scheduler_name,
                unsupported_fields=tuple(unsupported),
            )

    def _scheduler_capabilities(self) -> SchedulerCapabilities:
        if hasattr(self._scheduler_impl, "capabilities"):
            return self._scheduler_impl.capabilities()
        return SchedulerCapabilities(
            supports_cwd=True,
            supports_env=True,
            supports_output_file=True,
            supports_error_file=True,
            supports_job_name=True,
            supports_cpu_count=True,
            supports_memory=True,
            supports_gpu_count=True,
            supports_gpu_type=True,
            supports_time_limit=True,
            supports_queue=True,
            supports_account=True,
            supports_priority=True,
            supports_dependency=True,
            supports_node_count=True,
            supports_exclusive_node=True,
            supports_array_jobs=True,
            supports_email=True,
            supports_qos=True,
            supports_reservation=True,
        )



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
