"""Public Submitor API for molq.

Provides the Submitor class (single entry point for job submission
and management) and JobHandle (lightweight handle for a submitted job).
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from molq.callbacks import EventBus, EventPayload, EventType

if TYPE_CHECKING:
    from molq.cluster import Cluster
from molq.config import load_profile
from molq.errors import (
    ConfigError,
    JobNotFoundError,
    ScriptError,
)
from molq.merge import merge_defaults
from molq.models import (
    Command,
    JobDependency,
    JobRecord,
    JobSpec,
    RetentionPolicy,
    RetryPolicy,
    StatusTransition,
    SubmitorDefaults,
)
from molq.monitor import JobMonitor
from molq.reconciler import JobReconciler
from molq.scheduler import SchedulerCapabilities
from molq.serde import (
    deserialize_execution,
    deserialize_resources,
    deserialize_retry_policy,
    deserialize_scheduling,
    deserialize_script,
    dump_submit_request,
    load_submit_request,
    serialize_execution,
    serialize_resources,
    serialize_retry_policy,
    serialize_scheduling,
    serialize_script,
)
from molq.status import JobState
from molq.store import JobStore
from molq.transport import Transport
from molq.types import DependencyRef, JobExecution, JobResources, JobScheduling, Script

# ---------------------------------------------------------------------------
# Dependency condition → scheduler keyword/expression maps
# ---------------------------------------------------------------------------

# SLURM:  --dependency=<keyword>:<jobid>[,<keyword>:<jobid2>...]
_SLURM_DEP_KEYWORDS: dict[str, str] = {
    "after_started": "after",
    "after_success": "afterok",
    "after_failure": "afternotok",
    "after": "afterany",
}

# PBS:  -W depend=<type>:<jobid>[:<jobid2>...][,<type2>:<jobid3>...]
# IDs with the same type are colon-joined; different types are comma-joined.
_PBS_DEP_KEYWORDS: dict[str, str] = {
    "after_started": "after",
    "after_success": "afterok",
    "after_failure": "afternotok",
    "after": "afterany",
}

# LSF:  -w "<expr> [&& <expr2> ...]"
_LSF_DEP_KEYWORDS: dict[str, str] = {
    "after_started": "started",
    "after_success": "done",
    "after_failure": "exit",
    "after": "ended",
}


class Submitor:
    """Lifecycle engine for submitted jobs.

    A Submitor holds the persistence + monitoring half of molq's two-axis
    model (the destination half is :class:`~molq.cluster.Cluster`).  Each
    Submitor is bound to a single :class:`~molq.cluster.Cluster` as its
    ``target`` at construction; submission, listing, cancellation, and
    watching are all implicitly scoped to that target's name.

    Multi-cluster on one process: instantiate one Submitor per Cluster.
    They share a :class:`~molq.store.JobStore` by default and filter their
    queries by ``target.name`` so they do not see each other's records.

    Args:
        target: The destination Cluster.
        defaults: Default resource/scheduling/execution parameters.
        store: Custom JobStore (default: ``~/.molq/jobs.db``).
        jobs_dir: Optional override for per-job artifacts.  When omitted,
            materialized scripts and default logs are written under the
            submission working directory at ``.molq/jobs/<job-id>/``.
    """

    # Always set after __init__; close() flips to None as an escape hatch
    # so __del__ can run cleanly.  Annotation captures the normal-operation
    # invariant — calls after close() raise via _store.get_record(...) etc.
    _store: JobStore

    def __init__(
        self,
        target: Cluster,
        *,
        defaults: SubmitorDefaults | None = None,
        store: JobStore | None = None,
        jobs_dir: str | Path | None = None,
        default_retry_policy: RetryPolicy | None = None,
        retention_policy: RetentionPolicy | None = None,
        profile_name: str | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        from molq.cluster import Cluster

        if not isinstance(target, Cluster):
            raise TypeError(
                f"Submitor.target must be a Cluster, got {type(target).__name__}. "
                f"Construct a Cluster first: "
                f"Submitor(target=Cluster(name, scheduler))"
            )
        self._target = target
        self._defaults = defaults
        self._store = store or JobStore()
        self._jobs_dir = self._resolve_jobs_dir(jobs_dir)
        self._default_retry_policy = default_retry_policy
        self._retention_policy = retention_policy or RetentionPolicy()
        self._profile_name = profile_name
        self._event_bus = event_bus or EventBus()

        self._reconciler = JobReconciler(
            target.scheduler_impl,
            self._store,
            target.name,
            jobs_dir=self._jobs_dir,
            event_bus=self._event_bus,
            on_terminal=self._handle_terminal_record,
        )
        self._monitor: JobMonitor | None = None

    @classmethod
    def from_profile(
        cls,
        profile_name: str,
        *,
        target: Cluster | None = None,
        config_path: str | Path | None = None,
        store: JobStore | None = None,
    ) -> Submitor:
        """Load lifecycle parameters from a profile, bind to *target*.

        If ``target`` is omitted, builds one via :meth:`Cluster.from_profile`.
        """
        from molq.cluster import Cluster

        profile = load_profile(profile_name, config_path)
        if target is None:
            target = Cluster.from_profile(profile_name, config_path=config_path)
        return cls(
            target,
            defaults=profile.defaults,
            store=store,
            jobs_dir=profile.jobs_dir,
            default_retry_policy=profile.retry,
            retention_policy=profile.retention,
            profile_name=profile.name,
        )

    @property
    def target(self) -> Cluster:
        return self._target

    @property
    def cluster_name(self) -> str:
        return self._target.name

    # `_scheduler_impl` and `_transport` are read-only views onto the bound
    # Cluster.  Submitor is the only legitimate caller of the underlying
    # protocol, so we keep them private.
    @property
    def _scheduler_impl(self) -> Any:
        return self._target.scheduler_impl

    @property
    def _transport(self) -> Transport:
        return self._target.transport

    @property
    def _monitor_instance(self) -> JobMonitor:
        if self._monitor is None:
            self._monitor = JobMonitor(self._reconciler, self._store)
        return self._monitor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit_job(
        self,
        *,
        argv: list[str] | None = None,
        command: str | None = None,
        script: Script | None = None,
        resources: JobResources | None = None,
        scheduling: JobScheduling | None = None,
        execution: JobExecution | None = None,
        metadata: dict[str, str] | None = None,
        retry: RetryPolicy | None = None,
        after_started: list[str] | None = None,
        after: list[str] | None = None,
        after_failure: list[str] | None = None,
        after_success: list[str] | None = None,
        job_dir_name: str | None = None,
    ) -> JobHandle:
        """Submit a job.

        Exactly one of argv, command, or script must be provided.

        Args:
            job_dir_name: Optional name for the job directory under ``jobs_dir``.
                When provided, the directory is named *job_dir_name* instead of
                the auto-generated UUID.  Useful when callers want log files to
                live alongside other per-execution artifacts under a meaningful
                name (e.g. ``exec-<run_id>``). When ``jobs_dir`` is not set,
                the base directory is the resolved submission ``cwd``.

        Returns:
            JobHandle for the submitted job.
        """
        effective_retry = retry if retry is not None else self._default_retry_policy
        lineage_job_id = JobSpec.new_job_id()
        handle, _ = self._submit_prepared(
            argv=argv,
            command=command,
            script=script,
            resources=resources,
            scheduling=scheduling,
            execution=execution,
            metadata=metadata,
            retry=effective_retry,
            after_started=after_started,
            after=after,
            after_failure=after_failure,
            after_success=after_success,
            root_job_id=lineage_job_id,
            attempt=1,
            previous_attempt_job_id=None,
            retry_group_id=lineage_job_id,
            profile_name=self._profile_name,
            dir_name=job_dir_name,
        )
        return handle

    def get_job(self, job_id: str) -> JobRecord:
        """Get a job record by ID.

        Raises:
            JobNotFoundError: If job doesn't exist.
        """
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)
        return record

    def list_jobs(self, include_terminal: bool = False) -> list[JobRecord]:
        """List jobs for this cluster."""
        return self._store.list_records(
            self._target.name, include_terminal=include_terminal
        )

    def get_transitions(self, job_id: str) -> list[StatusTransition]:
        """Return the persisted transition timeline for a job."""
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)
        return self._store.get_transitions(job_id)

    def get_retry_family(self, job_id: str) -> list[JobRecord]:
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)
        return self._store.get_retry_family(job_id)

    def get_dependencies(self, job_id: str) -> list[JobDependency]:
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)
        return self._store.get_dependencies(job_id)

    def get_dependents(self, job_id: str) -> list[JobDependency]:
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)
        return self._store.get_dependents(job_id)

    def get_dependency_preview(self, job_id: str) -> object:
        record = self._store.get_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)
        return self._store.get_dependency_previews([job_id]).get(job_id)

    def on_event(self, event: EventType, handler: Any) -> None:
        self._event_bus.on(event, handler)

    def off_event(self, event: EventType, handler: Any) -> None:
        self._event_bus.off(event, handler)

    def watch_jobs(
        self,
        job_ids: list[str] | None = None,
        *,
        timeout: float | None = None,
    ) -> list[JobRecord]:
        """Block until specified jobs (or all active) reach terminal state."""
        return self._monitor_instance.wait_many(
            job_ids,
            self._target.name,
            timeout=timeout,
        )

    def cancel_job(self, job_id: str) -> None:
        """Cancel a job."""
        record = self._store.get_latest_attempt_record(job_id)
        if record is None:
            raise JobNotFoundError(job_id, self._target.name)

        if record.scheduler_job_id:
            self._scheduler_impl.cancel(record.scheduler_job_id)

        now = time.time()
        self._store.update_job(record.job_id, state=JobState.CANCELLED, finished_at=now)
        self._store.record_transition(
            record.job_id,
            record.state,
            JobState.CANCELLED,
            now,
            "cancelled by user",
        )
        self._emit_status_change(
            job_id=record.job_id,
            old_state=record.state,
            new_state=JobState.CANCELLED,
            timestamp=now,
            reason="cancelled by user",
        )

    def refresh_jobs(self) -> None:
        """Reconcile all active jobs with the scheduler."""
        self._reconciler.reconcile()

    def cleanup_jobs(
        self,
        *,
        dry_run: bool = False,
        retention_policy: RetentionPolicy | None = None,
    ) -> dict[str, list[str]]:
        policy = retention_policy or self._retention_policy
        now = time.time()
        job_dir_cutoff = now - policy.keep_job_dirs_for_days * 86400
        record_cutoff = now - policy.keep_terminal_records_for_days * 86400
        artifact_candidates, record_candidates = self._store.list_cleanup_candidates(
            self._target.name,
            job_dir_cutoff=job_dir_cutoff,
            record_cutoff=record_cutoff,
            include_failed_job_dirs=not policy.keep_failed_job_dirs,
        )
        deleted_dirs: list[str] = []
        deleted_records: list[str] = []

        for record in artifact_candidates:
            job_dir = record.metadata.get("molq.job_dir")
            if not job_dir:
                continue
            deleted_dirs.append(job_dir)
            if not dry_run:
                shutil.rmtree(job_dir, ignore_errors=True)
                self._store.update_job(record.job_id, cleaned_at=now)

        if record_candidates:
            deleted_records = [record.job_id for record in record_candidates]
            if not dry_run:
                self._store.delete_terminal_records(deleted_records)

        return {"job_dirs": deleted_dirs, "records": deleted_records}

    def fetch_logs(
        self,
        job_id: str,
        *,
        dest_dir: str | Path | None = None,
        streams: tuple[str, ...] = ("stdout", "stderr"),
    ) -> dict[str, Path]:
        """Pull captured log files from the cluster's filesystem to local.

        The job's recorded log paths (``metadata["molq.stdout_path"]`` /
        ``"molq.stderr_path"``) live on the cluster's filesystem.  For a
        remote :class:`~molq.cluster.Cluster` (SSH transport), this method
        rsyncs them down to *dest_dir*.  For a local cluster, it's a copy.

        Args:
            job_id: Job to fetch logs for.
            dest_dir: Local directory.  Defaults to a per-job folder under
                the local jobs_dir.
            streams: Subset of ``("stdout", "stderr")``.

        Returns:
            Mapping ``stream_name -> local_path`` for streams that existed
            on the remote side.  Missing-on-remote streams are silently
            skipped.

        Raises:
            JobNotFoundError: When *job_id* is unknown.
        """
        record = self.get_job(job_id)
        keys = {"stdout": "molq.stdout_path", "stderr": "molq.stderr_path"}

        if dest_dir is None:
            # _jobs_dir is None by default — fall back to a local scratch
            # directory rather than the (possibly remote) per-job cwd.
            base = self._jobs_dir or Path.cwd() / ".molq" / "fetched"
            dest = base / job_id / "logs"
        else:
            dest = Path(dest_dir).expanduser()
        dest.mkdir(parents=True, exist_ok=True)

        out: dict[str, Path] = {}
        transport = self._target.transport
        for stream in streams:
            remote_path = record.metadata.get(keys[stream])
            if not remote_path:
                continue
            if not transport.exists(remote_path):
                continue
            local_path = dest / f"{stream}.log"
            transport.download(remote_path, str(local_path))
            out[stream] = local_path
        return out

    def fetch_artifacts(
        self,
        job_id: str,
        *,
        dest_dir: str | Path | None = None,
        exclude: tuple[str, ...] = (),
    ) -> Path:
        """Mirror the job's working directory back to a local folder.

        Behaves like ``rsync -a <job_dir>/ <dest_dir>/`` over the cluster's
        transport — useful when the job emitted output files alongside its
        scripts and you want the whole bundle locally.

        Returns the local destination directory.
        """
        record = self.get_job(job_id)
        job_dir = record.metadata.get("molq.job_dir")
        if not job_dir:
            raise FileNotFoundError(
                f"Job {job_id} has no recorded molq.job_dir to mirror"
            )
        if dest_dir is None:
            base = self._jobs_dir or Path.cwd() / ".molq" / "fetched"
            dest = base / job_id / "mirror"
        else:
            dest = Path(dest_dir).expanduser()
        dest.mkdir(parents=True, exist_ok=True)
        self._target.transport.download(
            job_dir, str(dest), recursive=True, exclude=exclude
        )
        return dest

    def run_daemon(
        self,
        *,
        once: bool = False,
        interval: float = 5.0,
        run_cleanup: bool = True,
    ) -> None:
        while True:
            self.refresh_jobs()
            if run_cleanup:
                self.cleanup_jobs(dry_run=False)
            if once:
                return
            time.sleep(interval)

    def close(self) -> None:
        """Release the underlying :class:`JobStore` connection.

        Safe to call multiple times.  After ``close()`` no further methods
        should be invoked on this Submitor.
        """
        store = getattr(self, "_store", None)
        if store is not None:
            store.close()
            self._store = None  # ty: ignore[invalid-assignment]

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
        """Prepare script files in the job directory.

        Reads the user's script from the local filesystem and writes it into
        ``job_dir`` via the transport — so for an :class:`SshTransport` the
        materialised copy lands on the remote host.
        """
        if script.variant == "path" and script.file_path:
            target = job_dir / "user_script.sh"
            content = Path(script.file_path).read_bytes()
            self._transport.write_bytes(str(target), content, mode=0o700)

    def _resolve_jobs_dir(self, jobs_dir: str | Path | None) -> Path | None:
        if jobs_dir is not None:
            return Path(jobs_dir).expanduser().resolve()

        return None

    def _default_jobs_dir(self, cwd: str) -> Path:
        return Path(cwd) / ".molq" / "jobs"

    def _prepare_job_dir(
        self,
        job_id: str,
        cwd: str,
        dir_name: str | None = None,
    ) -> Path:
        jobs_dir = self._job_dir_root(cwd)
        self._transport.mkdir(str(jobs_dir), parents=True, exist_ok=True)
        job_dir = self._job_dir_path(job_id, cwd, dir_name)
        self._transport.mkdir(str(job_dir), parents=True, exist_ok=True)
        # mode=0o700 is honoured by LocalTransport-backed pathlib only on
        # creation; for SshTransport mkdir uses the remote umask.  Set it
        # explicitly so both paths converge.
        try:
            self._transport.chmod(str(job_dir), 0o700)
        except Exception:
            # chmod failures are non-fatal — directory exists and is usable.
            pass
        return job_dir

    def _job_dir_root(self, cwd: str) -> Path:
        return self._jobs_dir or self._default_jobs_dir(cwd)

    def _job_dir_path(
        self,
        job_id: str,
        cwd: str,
        dir_name: str | None = None,
    ) -> Path:
        return self._job_dir_root(cwd) / (dir_name or job_id)

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

    def _submit_prepared(
        self,
        *,
        argv: list[str] | None,
        command: str | None,
        script: Script | None,
        resources: JobResources | None,
        scheduling: JobScheduling | None,
        execution: JobExecution | None,
        metadata: dict[str, str] | None,
        retry: RetryPolicy | None,
        after_started: list[str] | None,
        after: list[str] | None,
        after_failure: list[str] | None,
        after_success: list[str] | None,
        root_job_id: str,
        attempt: int,
        previous_attempt_job_id: str | None,
        retry_group_id: str | None,
        profile_name: str | None,
        dir_name: str | None = None,
    ) -> tuple[JobHandle, list[JobDependency]]:
        cmd = Command.from_submit_args(argv=argv, command=command, script=script)
        if cmd.script is not None and cmd.script.variant == "path":
            if cmd.script.file_path is None or not cmd.script.file_path.exists():
                raise ScriptError(
                    f"Script file not found: {cmd.script.file_path}",
                    path=str(cmd.script.file_path) if cmd.script.file_path else None,
                )

        merged_resources, merged_scheduling, merged_execution = merge_defaults(
            self._defaults,
            resources=resources,
            scheduling=scheduling,
            execution=execution,
        )
        requested_execution = merged_execution
        request_scheduling = replace(
            merged_scheduling,
            dependencies=self._merge_dependency_refs(
                merged_scheduling.dependencies,
                after_started=after_started or [],
                after=after or [],
                after_failure=after_failure or [],
                after_success=after_success or [],
            ),
        )

        cwd = self._resolve_cwd(merged_execution.cwd)
        job_id = JobSpec.new_job_id() if attempt > 1 else root_job_id
        job_dir = self._job_dir_path(job_id, cwd, dir_name)
        stdout_path = self._resolve_output_path(
            merged_execution.output_file, cwd, job_dir, "stdout.log"
        )
        stderr_path = self._resolve_output_path(
            merged_execution.error_file, cwd, job_dir, "stderr.log"
        )
        canonical_execution = replace(
            merged_execution,
            cwd=cwd,
            output_file=str(stdout_path),
            error_file=str(stderr_path),
        )

        dependency_string, dependencies = self._resolve_dependencies(
            job_id=job_id,
            root_job_id=root_job_id,
            explicit_dependency=merged_scheduling.dependency,
            dependency_refs=request_scheduling.dependencies,
        )
        # The logical refs have been compiled into dependency_string.
        # Clear dependencies on the spec-bound copy so __post_init__ doesn't
        # see both fields set simultaneously.
        merged_scheduling = replace(
            merged_scheduling,
            dependency=dependency_string,
            dependencies=(),
        )

        user_metadata = dict(metadata or {})
        merged_metadata = dict(user_metadata)
        merged_metadata["molq.job_dir"] = str(job_dir)
        merged_metadata["molq.stdout_path"] = str(stdout_path)
        merged_metadata["molq.stderr_path"] = str(stderr_path)

        request_json = dump_submit_request(
            {
                "argv": list(cmd.argv) if cmd.argv is not None else None,
                "command": cmd.command,
                "script": serialize_script(cmd.script),
                "resources": serialize_resources(merged_resources),
                "scheduling": serialize_scheduling(request_scheduling),
                "execution": serialize_execution(requested_execution),
                "metadata": user_metadata,
                "retry": serialize_retry_policy(retry),
                "after_started": list(after_started or []),
                "after": list(after or []),
                "after_failure": list(after_failure or []),
                "after_success": list(after_success or []),
                "profile_name": profile_name,
            }
        )

        spec = JobSpec(
            job_id=job_id,
            cluster_name=self._target.name,
            scheduler=self._target.scheduler,
            command=cmd,
            resources=merged_resources,
            scheduling=merged_scheduling,
            execution=canonical_execution,
            metadata=merged_metadata,
            cwd=cwd,
            root_job_id=root_job_id,
            attempt=attempt,
            previous_attempt_job_id=previous_attempt_job_id,
            retry_group_id=retry_group_id or root_job_id,
            request_json=request_json,
            profile_name=profile_name,
            dir_name=dir_name,
        )
        self._validate_spec(spec, requested_execution=requested_execution)

        job_dir = self._prepare_job_dir(job_id, cwd, dir_name)
        if cmd.script is not None and cmd.script.variant == "path":
            self._materialize_script(cmd.script, job_dir)
        self._write_manifest(spec)
        self._store.insert_job(spec)
        if dependencies:
            self._store.add_dependencies(
                job_id,
                [
                    JobDependency(
                        job_id=job_id,
                        dependency_job_id=dep.dependency_job_id,
                        dependency_type=dep.dependency_type,
                        scheduler_dependency=dep.scheduler_dependency,
                    )
                    for dep in dependencies
                ],
            )

        self._emit_status_change(
            job_id=job_id,
            old_state=None,
            new_state=JobState.CREATED,
            timestamp=time.time(),
            reason="job created",
        )

        try:
            scheduler_job_id = self._scheduler_impl.submit(spec, job_dir)
        except Exception as exc:
            failed_at = time.time()
            self._store.update_job(
                job_id,
                state=JobState.FAILED,
                finished_at=failed_at,
                failure_reason=str(exc),
            )
            self._store.record_transition(
                job_id,
                JobState.CREATED,
                JobState.FAILED,
                failed_at,
                f"submission failed: {exc}",
            )
            self._emit_status_change(
                job_id=job_id,
                old_state=JobState.CREATED,
                new_state=JobState.FAILED,
                timestamp=failed_at,
                reason=f"submission failed: {exc}",
            )
            raise

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
        self._emit_status_change(
            job_id=job_id,
            old_state=JobState.CREATED,
            new_state=JobState.SUBMITTED,
            timestamp=now,
            reason="submitted",
        )
        return (
            JobHandle(
                job_id=root_job_id,
                cluster_name=self._target.name,
                scheduler=self._target.scheduler,
                scheduler_job_id=scheduler_job_id,
                _state=JobState.SUBMITTED,
                _submitor=self,
            ),
            dependencies,
        )

    def _merge_dependency_refs(
        self,
        dependencies: tuple[DependencyRef, ...],
        *,
        after_started: list[str],
        after: list[str],
        after_failure: list[str],
        after_success: list[str],
    ) -> tuple[DependencyRef, ...]:
        merged = list(dependencies)
        merged.extend(
            DependencyRef(job_id=job_id, condition="after_started")
            for job_id in after_started
        )
        merged.extend(
            DependencyRef(job_id=job_id, condition="after") for job_id in after
        )
        merged.extend(
            DependencyRef(job_id=job_id, condition="after_failure")
            for job_id in after_failure
        )
        merged.extend(
            DependencyRef(job_id=job_id, condition="after_success")
            for job_id in after_success
        )
        return tuple(merged)

    def _resolve_dependencies(
        self,
        *,
        job_id: str,
        root_job_id: str,
        explicit_dependency: str | None,
        dependency_refs: tuple[DependencyRef, ...],
    ) -> tuple[str | None, list[JobDependency]]:
        if explicit_dependency:
            return explicit_dependency, []
        if not dependency_refs:
            return None, []

        caps = self._scheduler_capabilities()
        if not caps.supports_dependency:
            raise ConfigError(
                f"Scheduler {self._target.scheduler!r} does not support job dependencies",
                scheduler=self._target.scheduler,
            )

        seen: set[tuple[str, str]] = set()
        # (condition, scheduler_job_id) pairs in submission order
        pairs: list[tuple[str, str]] = []
        dependencies: list[JobDependency] = []

        for ref in dependency_refs:
            dep_job_id = ref.job_id
            if dep_job_id in {job_id, root_job_id}:
                raise ConfigError(
                    "A job cannot depend on itself",
                    dependency_job_id=dep_job_id,
                )

            key = (dep_job_id, ref.condition)
            if key in seen:
                continue
            seen.add(key)

            dep_record = self._store.get_latest_attempt_record(dep_job_id)
            if dep_record is None:
                raise JobNotFoundError(dep_job_id, self._target.name)
            if dep_record.scheduler != self._target.scheduler:
                raise ConfigError(
                    f"Dependency job {dep_job_id!r} belongs to scheduler"
                    f" {dep_record.scheduler!r}, not {self._target.scheduler!r}",
                    dependency_job_id=dep_job_id,
                )
            if dep_record.cluster_name != self._target.name:
                raise ConfigError(
                    f"Dependency job {dep_job_id!r} belongs to cluster"
                    f" {dep_record.cluster_name!r}, not {self._target.name!r}",
                    dependency_job_id=dep_job_id,
                )
            if dep_record.scheduler_job_id is None:
                raise ConfigError(
                    f"Dependency job {dep_job_id!r} does not have a scheduler job id yet",
                    dependency_job_id=dep_job_id,
                )

            sid = dep_record.scheduler_job_id
            pairs.append((ref.condition, sid))
            dependencies.append(
                JobDependency(
                    job_id="",
                    dependency_job_id=dep_job_id,
                    dependency_type=ref.condition,
                    scheduler_dependency=self._format_single_dep(ref.condition, sid),
                )
            )

        return self._format_dep_string(pairs), dependencies

    def _format_single_dep(self, condition: str, scheduler_job_id: str) -> str:
        """Format one (condition, jobid) edge for storage in job_dependencies."""
        if self._target.scheduler == "slurm":
            kw = _SLURM_DEP_KEYWORDS.get(condition)
            if kw is None:
                raise ConfigError(
                    f"Unsupported dependency condition {condition!r}",
                    scheduler=self._target.scheduler,
                )
            return f"{kw}:{scheduler_job_id}"
        if self._target.scheduler == "pbs":
            kw = _PBS_DEP_KEYWORDS.get(condition)
            if kw is None:
                raise ConfigError(
                    f"Unsupported dependency condition {condition!r}",
                    scheduler=self._target.scheduler,
                )
            return f"{kw}:{scheduler_job_id}"
        if self._target.scheduler == "lsf":
            kw = _LSF_DEP_KEYWORDS.get(condition)
            if kw is None:
                raise ConfigError(
                    f"Unsupported dependency condition {condition!r}",
                    scheduler=self._target.scheduler,
                )
            return f"{kw}({scheduler_job_id})"
        return f"{condition}:{scheduler_job_id}"

    def _format_dep_string(self, pairs: list[tuple[str, str]]) -> str:
        """Format the complete dependency string for scheduling.dependency."""
        if self._target.scheduler == "slurm":
            parts: list[str] = []
            for condition, sid in pairs:
                kw = _SLURM_DEP_KEYWORDS.get(condition)
                if kw is None:
                    raise ConfigError(
                        f"Unsupported dependency condition {condition!r}",
                        scheduler=self._target.scheduler,
                    )
                parts.append(f"{kw}:{sid}")
            return ",".join(parts)

        if self._target.scheduler == "pbs":
            # PBS groups IDs by type: afterok:123:456,afternotok:789
            groups: dict[str, list[str]] = {}
            for condition, sid in pairs:
                kw = _PBS_DEP_KEYWORDS.get(condition)
                if kw is None:
                    raise ConfigError(
                        f"Unsupported dependency condition {condition!r}",
                        scheduler=self._target.scheduler,
                    )
                groups.setdefault(kw, []).append(sid)
            return ",".join(f"{kw}:{':'.join(sids)}" for kw, sids in groups.items())

        if self._target.scheduler == "lsf":
            # LSF: done(123) && done(456) && exit(789)
            exprs: list[str] = []
            for condition, sid in pairs:
                kw = _LSF_DEP_KEYWORDS.get(condition)
                if kw is None:
                    raise ConfigError(
                        f"Unsupported dependency condition {condition!r}",
                        scheduler=self._target.scheduler,
                    )
                exprs.append(f"{kw}({sid})")
            return " && ".join(exprs)

        return ""

    def _write_manifest(self, spec: JobSpec) -> None:
        job_dir = self._job_dir_path(spec.job_id, spec.cwd, spec.dir_name)
        manifest_path = job_dir / "manifest.json"
        self._transport.write_text(
            str(manifest_path),
            json.dumps(
                {
                    "job_id": spec.job_id,
                    "root_job_id": spec.root_job_id,
                    "attempt": spec.attempt,
                    "script_path": str(job_dir / "user_script.sh")
                    if spec.command.script is not None
                    and spec.command.script.variant == "path"
                    else None,
                    "stdout_path": spec.metadata.get("molq.stdout_path"),
                    "stderr_path": spec.metadata.get("molq.stderr_path"),
                    "created_at": time.time(),
                },
                sort_keys=True,
            ),
            mode=0o600,
        )

    def _emit_status_change(
        self,
        *,
        job_id: str,
        old_state: JobState | None,
        new_state: JobState,
        timestamp: float,
        reason: str | None,
    ) -> None:
        record = self._store.get_record(job_id)
        if record is None:
            return
        transition = StatusTransition(
            job_id=job_id,
            old_state=old_state,
            new_state=new_state,
            timestamp=timestamp,
            reason=reason,
        )
        self._event_bus.emit(
            EventType.STATUS_CHANGE,
            EventPayload(
                event=EventType.STATUS_CHANGE,
                job_id=job_id,
                transition=transition,
                record=record,
            ),
        )
        event = {
            JobState.RUNNING: EventType.JOB_STARTED,
            JobState.SUCCEEDED: EventType.JOB_COMPLETED,
            JobState.FAILED: EventType.JOB_FAILED,
            JobState.CANCELLED: EventType.JOB_CANCELLED,
            JobState.LOST: EventType.JOB_LOST,
        }.get(new_state)
        if event is not None:
            self._event_bus.emit(
                event,
                EventPayload(
                    event=event,
                    job_id=job_id,
                    transition=transition,
                    record=record,
                ),
            )
        if new_state == JobState.TIMED_OUT:
            payload = EventPayload(
                event=EventType.JOB_TIMED_OUT,
                job_id=job_id,
                transition=transition,
                record=record,
            )
            self._event_bus.emit(EventType.JOB_TIMED_OUT, payload)
            self._event_bus.emit(EventType.JOB_TIMEOUT, payload)

    def _handle_terminal_record(self, record: JobRecord) -> None:
        request = load_submit_request(self._store.get_request_json(record.job_id))
        retry_policy = deserialize_retry_policy(request.get("retry"))
        if retry_policy is None:
            return
        if record.attempt >= retry_policy.max_attempts:
            return
        if record.state not in retry_policy.retry_on_states:
            return
        if (
            retry_policy.retry_on_exit_codes is not None
            and record.exit_code not in retry_policy.retry_on_exit_codes
        ):
            return

        delay = self._retry_delay_seconds(retry_policy, record.attempt)
        if delay > 0:
            time.sleep(delay)

        scheduling = deserialize_scheduling(request.get("scheduling", {}))
        execution = deserialize_execution(request.get("execution", {}))
        resources = deserialize_resources(request.get("resources", {}))
        script = deserialize_script(request.get("script"))

        self._submit_prepared(
            argv=request.get("argv"),
            command=request.get("command"),
            script=script,
            resources=resources,
            scheduling=scheduling,
            execution=execution,
            metadata=request.get("metadata"),
            retry=retry_policy,
            after_started=[],
            after=[],
            after_failure=[],
            after_success=[],
            root_job_id=record.root_job_id or record.job_id,
            attempt=record.attempt + 1,
            previous_attempt_job_id=record.job_id,
            retry_group_id=record.retry_group_id or record.root_job_id or record.job_id,
            profile_name=request.get("profile_name"),
        )

    def _retry_delay_seconds(self, policy: RetryPolicy, attempt: int) -> float:
        if policy.backoff.mode == "fixed":
            return min(policy.backoff.initial_seconds, policy.backoff.maximum_seconds)
        delay = policy.backoff.initial_seconds * (
            policy.backoff.factor ** max(attempt - 1, 0)
        )
        return min(delay, policy.backoff.maximum_seconds)

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
        require(
            "execution.job_name", capabilities.supports_job_name, e.job_name is not None
        )
        require(
            "execution.output_file",
            capabilities.supports_output_file,
            e.output_file is not None,
        )
        require(
            "execution.error_file",
            capabilities.supports_error_file,
            e.error_file is not None,
        )
        require(
            "resources.cpu_count",
            capabilities.supports_cpu_count,
            r.cpu_count is not None,
        )
        require("resources.memory", capabilities.supports_memory, r.memory is not None)
        require(
            "resources.gpu_count",
            capabilities.supports_gpu_count,
            r.gpu_count is not None,
        )
        require(
            "resources.gpu_type", capabilities.supports_gpu_type, r.gpu_type is not None
        )
        require(
            "resources.time_limit",
            capabilities.supports_time_limit,
            r.time_limit is not None,
        )
        require(
            "scheduling.partition",
            capabilities.supports_partition,
            s.partition is not None,
        )
        require(
            "scheduling.account", capabilities.supports_account, s.account is not None
        )
        require(
            "scheduling.priority",
            capabilities.supports_priority,
            s.priority is not None,
        )
        require(
            "scheduling.dependency",
            capabilities.supports_dependency,
            s.dependency is not None,
        )
        require(
            "scheduling.node_count",
            capabilities.supports_node_count,
            s.node_count is not None,
        )
        require(
            "scheduling.exclusive_node",
            capabilities.supports_exclusive_node,
            s.exclusive_node,
        )
        require(
            "scheduling.array_spec",
            capabilities.supports_array_jobs,
            s.array_spec is not None,
        )
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
                f"Scheduler {self._target.scheduler!r} does not support requested fields: {fields}",
                scheduler=self._target.scheduler,
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
            supports_partition=True,
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
        latest = self._submitor._store.get_latest_attempt_record(self.job_id)
        watched_job_id = latest.job_id if latest is not None else self.job_id
        new_state = self._submitor._reconciler.reconcile_one(watched_job_id)
        latest = self._submitor._store.get_latest_attempt_record(self.job_id)
        if new_state is not None:
            self._state = new_state
        if latest is not None:
            self.scheduler_job_id = latest.scheduler_job_id
        return self

    def wait(self, timeout: float | None = None) -> JobRecord:
        """Block until this job reaches a terminal state."""
        return self._submitor._monitor_instance.wait_one(
            self.job_id,
            timeout=timeout,
        )

    def cancel(self) -> None:
        """Cancel this job."""
        self._submitor.cancel_job(self.job_id)
        self._state = JobState.CANCELLED
