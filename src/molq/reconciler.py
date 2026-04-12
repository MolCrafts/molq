"""Job state reconciliation for molq.

Syncs scheduler state with the JobStore. Internal module.
"""

from __future__ import annotations

import time

# Use a broad type hint since we accept any Scheduler-like object
from typing import Any, Protocol

from molq.scheduler import LocalScheduler
from molq.status import JobState
from molq.store import JobStore


class _SchedulerLike(Protocol):
    def poll_many(self, scheduler_job_ids: list[str]) -> dict[str, JobState]: ...
    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None: ...


class StatusChange:
    """Record of a single job state transition."""

    __slots__ = ("job_id", "old_state", "new_state", "timestamp")

    def __init__(
        self,
        job_id: str,
        old_state: JobState,
        new_state: JobState,
        timestamp: float,
    ) -> None:
        self.job_id = job_id
        self.old_state = old_state
        self.new_state = new_state
        self.timestamp = timestamp


class JobReconciler:
    """Sync scheduler state with persisted JobStore state.

    Each call to reconcile() performs one poll cycle: load active jobs,
    batch-query the scheduler, compute diffs, update the store.
    """

    def __init__(
        self,
        scheduler: Any,
        store: JobStore,
        cluster_name: str,
        *,
        jobs_dir: Any | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._store = store
        self._cluster_name = cluster_name
        self._jobs_dir = jobs_dir

    def reconcile(self) -> list[StatusChange]:
        """Run one reconciliation cycle for all active jobs."""
        active = self._store.get_active_records(self._cluster_name)
        if not active:
            return []

        # Build scheduler_job_id -> job_id mapping
        id_map: dict[str, str] = {}
        for record in active:
            if record.scheduler_job_id:
                id_map[record.scheduler_job_id] = record.job_id

        if not id_map:
            return []

        # Batch query scheduler
        scheduler_states = self._scheduler.poll_many(list(id_map.keys()))
        now = time.time()
        changes: list[StatusChange] = []

        for record in active:
            if not record.scheduler_job_id:
                continue

            sid = record.scheduler_job_id

            # Re-read the latest state to avoid trampling concurrent
            # writes (e.g., a parallel cancel()).
            current = self._store.get_record(record.job_id)
            if current is None or current.state.is_terminal:
                continue
            old_state = current.state

            if sid in scheduler_states:
                new_state = scheduler_states[sid]
            else:
                new_state = self._infer_terminal(sid, record.job_id)

            if new_state != old_state:
                if self._apply_transition(record.job_id, old_state, new_state, now):
                    changes.append(
                        StatusChange(record.job_id, old_state, new_state, now)
                    )

            # Update last_polled
            self._store.update_job(record.job_id, last_polled=now)

        return changes

    def reconcile_one(self, job_id: str) -> JobState | None:
        """Reconcile a single job. Returns new state or None if not found."""
        record = self._store.get_record(job_id)
        if record is None or record.state.is_terminal:
            return record.state if record else None

        if not record.scheduler_job_id:
            return record.state

        now = time.time()
        result = self._scheduler.poll_many([record.scheduler_job_id])
        sid = record.scheduler_job_id

        if sid in result:
            new_state = result[sid]
        else:
            new_state = self._infer_terminal(sid, job_id)

        if new_state != record.state:
            if not self._apply_transition(job_id, record.state, new_state, now):
                # State changed under us — return the latest authoritative value.
                latest = self._store.get_record(job_id)
                if latest is not None:
                    new_state = latest.state

        self._store.update_job(job_id, last_polled=now)
        return new_state

    def _infer_terminal(self, scheduler_job_id: str, job_id: str) -> JobState:
        """Determine terminal state for a disappeared job."""
        # For LocalScheduler, use resolve_terminal_with_dir if available
        if hasattr(self._scheduler, "resolve_terminal_with_dir") and self._jobs_dir:
            from pathlib import Path

            job_dir = Path(self._jobs_dir) / job_id
            result = self._scheduler.resolve_terminal_with_dir(
                scheduler_job_id, job_dir
            )
            if result is not None:
                return result

        result = self._scheduler.resolve_terminal(scheduler_job_id)
        if result is not None:
            return result

        return JobState.LOST

    def _apply_transition(
        self,
        job_id: str,
        old_state: JobState,
        new_state: JobState,
        timestamp: float,
    ) -> bool:
        """Update store with a state transition, atomically.

        Returns True if the transition was applied, False if the row's state
        changed under us (e.g. a concurrent cancel()) and the transition was
        skipped to preserve the authoritative value.
        """
        kwargs: dict[str, object] = {}
        if new_state == JobState.RUNNING and old_state != JobState.RUNNING:
            kwargs["started_at"] = timestamp
        if new_state.is_terminal:
            kwargs["finished_at"] = timestamp

        applied = self._store.compare_and_update_state(
            job_id,
            expected_state=old_state,
            new_state=new_state,
            **kwargs,
        )
        if not applied:
            return False

        self._store.record_transition(
            job_id,
            old_state=old_state,
            new_state=new_state,
            timestamp=timestamp,
            reason=_describe_transition(old_state, new_state),
        )
        return True


def _describe_transition(old: JobState, new: JobState) -> str:
    descriptions = {
        JobState.RUNNING: "scheduler started the job",
        JobState.SUCCEEDED: "job completed successfully",
        JobState.FAILED: "job failed",
        JobState.CANCELLED: "job was cancelled",
        JobState.TIMED_OUT: "job exceeded time limit",
        JobState.LOST: "job disappeared from scheduler",
    }
    return descriptions.get(new, f"{old.value} -> {new.value}")
