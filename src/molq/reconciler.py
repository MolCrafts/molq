"""Job state reconciliation for molq.

Syncs scheduler state with the JobStore. Internal module.
"""

from __future__ import annotations

import time

# Use a broad type hint since we accept any Scheduler-like object
from collections.abc import Callable
from typing import Any, Protocol

from molq.callbacks import EventBus, EventPayload, EventType
from molq.models import JobRecord, StatusTransition
from molq.scheduler import TerminalStatus
from molq.status import JobState
from molq.store import JobStore


class _SchedulerLike(Protocol):
    def poll_many(self, scheduler_job_ids: list[str]) -> dict[str, JobState]: ...
    def resolve_terminal(
        self, scheduler_job_id: str
    ) -> JobState | TerminalStatus | None: ...


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
        event_bus: EventBus | None = None,
        on_terminal: Callable[[JobRecord], None] | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._store = store
        self._cluster_name = cluster_name
        self._jobs_dir = jobs_dir
        self._event_bus = event_bus
        self._on_terminal = on_terminal

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
                terminal = (
                    self._infer_terminal(sid, record.job_id, fallback_state=new_state)
                    if new_state.is_terminal
                    else None
                )
            else:
                terminal = self._infer_terminal(sid, record.job_id)
                new_state = terminal.state

            if new_state != old_state:
                if self._apply_transition(
                    record.job_id,
                    old_state,
                    new_state,
                    now,
                    terminal=terminal,
                ):
                    updated = self._store.get_record(record.job_id)
                    transition = StatusTransition(
                        job_id=record.job_id,
                        old_state=old_state,
                        new_state=new_state,
                        timestamp=now,
                        reason=_describe_transition(old_state, new_state),
                    )
                    if updated is not None:
                        self._emit_transition_events(
                            transition=transition, record=updated
                        )
                        if new_state.is_terminal and self._on_terminal is not None:
                            self._on_terminal(updated)
                    changes.append(
                        StatusChange(record.job_id, old_state, new_state, now)
                    )

            # Update last_polled
            self._store.update_job(record.job_id, last_polled=now)

        if changes and not self._store.get_active_records(self._cluster_name):
            self._emit_all_completed()

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
            terminal = (
                self._infer_terminal(sid, job_id, fallback_state=new_state)
                if new_state.is_terminal
                else None
            )
        else:
            terminal = self._infer_terminal(sid, job_id)
            new_state = terminal.state

        if new_state != record.state:
            if not self._apply_transition(
                job_id,
                record.state,
                new_state,
                now,
                terminal=terminal,
            ):
                # State changed under us — return the latest authoritative value.
                latest = self._store.get_record(job_id)
                if latest is not None:
                    new_state = latest.state
            else:
                updated = self._store.get_record(job_id)
                transition = StatusTransition(
                    job_id=job_id,
                    old_state=record.state,
                    new_state=new_state,
                    timestamp=now,
                    reason=_describe_transition(record.state, new_state),
                )
                if updated is not None:
                    self._emit_transition_events(transition=transition, record=updated)
                    if new_state.is_terminal and self._on_terminal is not None:
                        self._on_terminal(updated)

        self._store.update_job(job_id, last_polled=now)
        return new_state

    def _infer_terminal(
        self,
        scheduler_job_id: str,
        job_id: str,
        fallback_state: JobState | None = None,
    ) -> TerminalStatus:
        """Determine terminal state for a disappeared job."""
        # For LocalScheduler, use resolve_terminal_with_dir if available
        if hasattr(self._scheduler, "resolve_terminal_with_dir") and self._jobs_dir:
            from pathlib import Path

            job_dir = Path(self._jobs_dir) / job_id
            result = _normalize_terminal_status(
                self._scheduler.resolve_terminal_with_dir(scheduler_job_id, job_dir)
            )
            if result is not None:
                return result

        result = _normalize_terminal_status(
            self._scheduler.resolve_terminal(scheduler_job_id)
        )
        if result is not None:
            return result

        return TerminalStatus(
            state=fallback_state or JobState.LOST,
            failure_reason=(
                "job disappeared from scheduler"
                if fallback_state in (None, JobState.LOST)
                else None
            ),
        )

    def _apply_transition(
        self,
        job_id: str,
        old_state: JobState,
        new_state: JobState,
        timestamp: float,
        *,
        terminal: TerminalStatus | None = None,
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
            if terminal is not None:
                kwargs["exit_code"] = terminal.exit_code
                kwargs["failure_reason"] = terminal.failure_reason

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

    def _emit_transition_events(
        self,
        *,
        transition: StatusTransition,
        record: JobRecord,
    ) -> None:
        if self._event_bus is None:
            return

        self._event_bus.emit(
            EventType.STATUS_CHANGE,
            EventPayload(
                event=EventType.STATUS_CHANGE,
                job_id=record.job_id,
                transition=transition,
                record=record,
            ),
        )
        if transition.new_state == JobState.RUNNING:
            event = EventType.JOB_STARTED
        elif transition.new_state == JobState.SUCCEEDED:
            event = EventType.JOB_COMPLETED
        elif transition.new_state == JobState.FAILED:
            event = EventType.JOB_FAILED
        elif transition.new_state == JobState.CANCELLED:
            event = EventType.JOB_CANCELLED
        elif transition.new_state == JobState.TIMED_OUT:
            payload = EventPayload(
                event=EventType.JOB_TIMED_OUT,
                job_id=record.job_id,
                transition=transition,
                record=record,
            )
            self._event_bus.emit(EventType.JOB_TIMED_OUT, payload)
            self._event_bus.emit(EventType.JOB_TIMEOUT, payload)
            return
        elif transition.new_state == JobState.LOST:
            event = EventType.JOB_LOST
        else:
            return

        self._event_bus.emit(
            event,
            EventPayload(
                event=event,
                job_id=record.job_id,
                transition=transition,
                record=record,
            ),
        )

    def _emit_all_completed(self) -> None:
        if self._event_bus is None:
            return
        self._event_bus.emit(
            EventType.ALL_COMPLETED,
            EventPayload(
                event=EventType.ALL_COMPLETED,
                data={"cluster_name": self._cluster_name},
            ),
        )


def _normalize_terminal_status(
    status: JobState | TerminalStatus | None,
) -> TerminalStatus | None:
    if status is None:
        return None
    if isinstance(status, TerminalStatus):
        return status
    return TerminalStatus(
        state=status,
        failure_reason=None
        if status == JobState.SUCCEEDED
        else _describe_transition(status, status),
    )


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
