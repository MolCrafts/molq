"""Public Submitor API for molq.

Provides the Submitor class (single entry point for job submission
and management) and JobHandle (lightweight handle for a submitted job).
"""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from molq import artifacts, jobpaths
from molq.callbacks import EventBus, EventType, emit_transition

if TYPE_CHECKING:
    from molq.cluster import Cluster
from molq.config import load_profile
from molq.dependencies import (
    merge_dependency_refs,
    resolve_dependencies,
)
from molq.errors import (
    JobNotFoundError,
    ScriptError,
)
from molq.handle import JobHandle
from molq.merge import merge_defaults
from molq.models import (
    Command,
    JobDependency,
    JobRecord,
    JobSpec,
    RememberedAllocation,
    RetentionPolicy,
    RetryPolicy,
    StatusTransition,
    SubmitorDefaults,
)
from molq.monitor import JobMonitor
from molq.plugin import PluginManager, store_context_factory
from molq.reconciler import JobReconciler
from molq.retention import apply_retention
from molq.retry import retry_delay_seconds, should_retry
from molq.scheduler import SchedulerCapabilities
from molq.serde import (
    build_submit_request,
    deserialize_execution,
    deserialize_resources,
    deserialize_retry_policy,
    deserialize_scheduling,
    deserialize_script,
    load_submit_request,
)
from molq.status import JobState
from molq.store import JobStore, default_jobs_db_path
from molq.transport import Transport
from molq.types import JobExecution, JobResources, JobScheduling, Script
from molq.validation import default_capabilities, validate_spec


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
        store: Custom JobStore.  When ``None``, auto-bootstraps a
            ``JobStore`` at the molcrafts-standard location via
            :func:`molq.store.default_jobs_db_path` (which delegates
            to :func:`molcfg.paths.project_config_dir`).
        jobs_dir: Optional override for per-job artifacts.  When omitted,
            materialized scripts and default logs are written under the
            submission working directory at ``.molq/jobs/<job-id>/``.
        plugins: Official or third-party plugin names to attach (e.g.
            ``["nerve"]``).  Empty/omitted means no plugins.
        plugin_configs: Per-plugin config dicts (from ``[plugins.<name>]``).
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
        plugins: list[str] | None = None,
        plugin_configs: dict[str, dict[str, Any]] | None = None,
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
        # Explicit auto-bootstrap via molcfg — no silent ``Path.home()``
        # fallback in ``JobStore`` itself.  Callers that want isolation
        # (tests, ops) pass a fully-constructed ``JobStore`` or set
        # ``MOLCRAFTS_HOME`` to redirect the bootstrap location.
        # Only close what we opened.  Several Submitors legitimately share one
        # caller-supplied JobStore (the multi-cluster pattern above), and
        # closing it when the first of them exits would break the rest.
        self._owns_store = store is None
        self._store = store if store is not None else JobStore(default_jobs_db_path())
        self._jobs_dir = self._resolve_jobs_dir(jobs_dir)
        self._default_retry_policy = default_retry_policy
        self._retention_policy = retention_policy or RetentionPolicy()
        self._profile_name = profile_name
        self._event_bus = event_bus or EventBus()
        self._plugin_manager = PluginManager()

        self._reconciler = JobReconciler(
            target.scheduler_impl,
            self._store,
            target.name,
            jobs_dir=self._jobs_dir,
            event_bus=self._event_bus,
            on_terminal=self._handle_terminal_record,
        )
        self._monitor: JobMonitor | None = None

        if plugins:
            self._attach_plugins(plugins, plugin_configs or {})

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

    def remembered_allocations(
        self, *, limit: int | None = None
    ) -> list[RememberedAllocation]:
        """Return scheduling configs previously used to submit to this cluster.

        Ordered most-recently-used first. Pure local recall — no cluster query.
        """
        return self._store.list_allocations(self._target.name, limit=limit)

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

    def _attach_plugins(
        self,
        names: list[str],
        configs: dict[str, dict[str, Any]],
    ) -> None:
        self._plugin_manager.load(
            names,
            ctx_factory=store_context_factory(
                self._event_bus, self._target.name, self._store
            ),
            configs=configs,
        )

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

    def refresh_job(self, job_id: str) -> JobRecord:
        """Reconcile one job with the scheduler and return its current record.

        Follows the retry family: the newest attempt of *job_id*'s family is
        reconciled, and if that reconciliation submits a retry, the record of
        the resulting attempt is returned. A job already in a terminal state is
        not re-polled and emits no events.

        Args:
            job_id: Any attempt's ID in the retry family (typically the root).

        Returns:
            The record of the latest attempt after reconciliation.

        Raises:
            JobNotFoundError: If job doesn't exist.
        """
        latest = self._store.get_latest_attempt_record(job_id)
        if latest is None:
            raise JobNotFoundError(job_id, self._target.name)
        self._reconciler.reconcile_one(latest.job_id)
        current = self._store.get_latest_attempt_record(job_id)
        if current is None:
            raise JobNotFoundError(job_id, self._target.name)
        return current

    def cleanup_jobs(
        self,
        *,
        dry_run: bool = False,
        retention_policy: RetentionPolicy | None = None,
    ) -> dict[str, list[str]]:
        """Delete expired job directories and terminal records.

        Returns ``{"job_dirs": [...], "records": [...]}`` — what was removed,
        or what would be under *dry_run*.
        """
        return apply_retention(
            self._store,
            self._target.name,
            retention_policy or self._retention_policy,
            dry_run=dry_run,
        )

    def fetch_logs(
        self,
        job_id: str,
        *,
        dest_dir: str | Path | None = None,
        streams: tuple[str, ...] = ("stdout", "stderr"),
    ) -> dict[str, Path]:
        """Pull captured log files from the cluster's filesystem to local.

        Args:
            job_id: Job to fetch logs for.
            dest_dir: Local directory.  Defaults to a per-job folder under
                the local jobs_dir.
            streams: Subset of ``("stdout", "stderr")``.

        Returns:
            Mapping ``stream_name -> local_path`` for streams that existed on
            the cluster.  Missing streams are silently skipped.

        Raises:
            JobNotFoundError: When *job_id* is unknown.
        """
        record = self.get_job(job_id)
        dest = (
            Path(dest_dir).expanduser()
            if dest_dir is not None
            else artifacts.local_scratch_dir(self._jobs_dir, job_id, "logs")
        )
        return artifacts.fetch_logs(self._transport, record, dest, streams)

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
        dest = (
            Path(dest_dir).expanduser()
            if dest_dir is not None
            else artifacts.local_scratch_dir(self._jobs_dir, job_id, "mirror")
        )
        return artifacts.fetch_job_dir(self._transport, record, dest, exclude)

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
        """Release plugins and this Submitor's :class:`JobStore` connection.

        The store connection is closed only when this Submitor opened it.  A
        store passed in via ``store=`` belongs to the caller and stays open
        for whoever else is using it.

        Safe to call multiple times.  After ``close()`` no further methods
        should be invoked on this Submitor.
        """
        mgr = getattr(self, "_plugin_manager", None)
        if mgr is not None:
            mgr.detach_all()
        store = getattr(self, "_store", None)
        if store is not None:
            if getattr(self, "_owns_store", False):
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

    def _resolve_jobs_dir(self, jobs_dir: str | Path | None) -> Path | None:
        if jobs_dir is not None:
            return Path(jobs_dir).expanduser().resolve()

        return None

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
            dependencies=merge_dependency_refs(
                merged_scheduling.dependencies,
                after_started=after_started or [],
                after=after or [],
                after_failure=after_failure or [],
                after_success=after_success or [],
            ),
        )

        cwd = jobpaths.resolve_cwd(merged_execution.cwd)
        job_id = JobSpec.new_job_id() if attempt > 1 else root_job_id
        job_dir = jobpaths.job_dir_path(self._jobs_dir, job_id, cwd, dir_name)
        stdout_path = jobpaths.resolve_output_path(
            merged_execution.output_file, cwd, job_dir, "stdout.log"
        )
        stderr_path = jobpaths.resolve_output_path(
            merged_execution.error_file, cwd, job_dir, "stderr.log"
        )
        canonical_execution = replace(
            merged_execution,
            cwd=cwd,
            output_file=str(stdout_path),
            error_file=str(stderr_path),
        )

        dependency_string, dependencies = resolve_dependencies(
            self._store,
            self._scheduler_impl,
            self._scheduler_capabilities(),
            self._target.name,
            self._target.scheduler,
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

        request_json = build_submit_request(
            command=cmd,
            resources=merged_resources,
            scheduling=request_scheduling,
            execution=requested_execution,
            metadata=user_metadata,
            retry=retry,
            after_started=after_started or [],
            after=after or [],
            after_failure=after_failure or [],
            after_success=after_success or [],
            profile_name=profile_name,
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
        validate_spec(
            spec,
            self._scheduler_capabilities(),
            requested_execution=requested_execution,
            scheduler_name=self._target.scheduler,
        )

        job_dir = jobpaths.prepare_job_dir(
            self._transport, self._jobs_dir, job_id, cwd, dir_name
        )
        if cmd.script is not None and cmd.script.variant == "path":
            jobpaths.materialize_script(self._transport, cmd.script, job_dir)
        jobpaths.write_manifest(self._transport, self._jobs_dir, spec, time.time())
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
        self._store.record_allocation(self._target.name, merged_scheduling, now=now)
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

    def _scheduler_capabilities(self) -> SchedulerCapabilities:
        if hasattr(self._scheduler_impl, "capabilities"):
            return self._scheduler_impl.capabilities()
        return default_capabilities()

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
        emit_transition(
            self._event_bus,
            StatusTransition(
                job_id=job_id,
                old_state=old_state,
                new_state=new_state,
                timestamp=timestamp,
                reason=reason,
            ),
            record,
        )

    def _handle_terminal_record(self, record: JobRecord) -> None:
        request = load_submit_request(self._store.get_request_json(record.job_id))
        retry_policy = deserialize_retry_policy(request.get("retry"))
        if not should_retry(record, retry_policy):
            return
        assert retry_policy is not None  # narrowed by should_retry

        delay = retry_delay_seconds(retry_policy, record.attempt)
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


__all__ = ["JobHandle", "Submitor"]
