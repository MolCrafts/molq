"""Job state reconciliation for molq.

Syncs scheduler state with the JobStore. Internal module.
"""

from __future__ import annotations

import time

# Use a broad type hint since we accept any Scheduler-like object
from collections.abc import Callable
from dataclasses import replace
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
        polled: list[str] = []

        for record in active:
            if not record.scheduler_job_id:
                continue

            sid = record.scheduler_job_id
            old_state = record.state

            if sid in scheduler_states:
                new_state = scheduler_states[sid]
                terminal = (
                    self._infer_terminal(sid, record, fallback_state=new_state)
                    if new_state.is_terminal
                    else None
                )
            else:
                terminal = self._infer_terminal(sid, record)
                if terminal is None:
                    # poll miss with no accounting evidence: keep waiting.
                    # Brand-new SLURM jobs are often absent from squeue for a
                    # beat (and sacct lags longer); that is not LOST.
                    polled.append(record.job_id)
                    continue
                new_state = terminal.state

            polled.append(record.job_id)
            if new_state == old_state:
                continue

            # No pre-read of the row here: compare_and_update_state is itself a
            # compare-and-swap on the expected state, so a concurrent cancel()
            # makes it return None and we simply skip the transition.
            updated = self._apply_transition(
                record, old_state, new_state, now, terminal=terminal
            )
            if updated is None:
                continue

            self._emit_transition_events(
                transition=StatusTransition(
                    job_id=record.job_id,
                    old_state=old_state,
                    new_state=new_state,
                    timestamp=now,
                    reason=_describe_transition(old_state, new_state),
                ),
                record=updated,
            )
            if new_state.is_terminal and self._on_terminal is not None:
                self._on_terminal(updated)
            changes.append(StatusChange(record.job_id, old_state, new_state, now))

        # One write for the whole cycle instead of one per job.
        self._store.mark_polled(polled, now)

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
                self._infer_terminal(sid, record, fallback_state=new_state)
                if new_state.is_terminal
                else None
            )
        else:
            terminal = self._infer_terminal(sid, record)
            if terminal is None:
                self._store.update_job(job_id, last_polled=now)
                return record.state
            new_state = terminal.state

        if new_state != record.state:
            updated = self._apply_transition(
                record, record.state, new_state, now, terminal=terminal
            )
            if updated is None:
                # State changed under us — return the latest authoritative value.
                latest = self._store.get_record(job_id)
                if latest is not None:
                    new_state = latest.state
            else:
                self._emit_transition_events(
                    transition=StatusTransition(
                        job_id=job_id,
                        old_state=record.state,
                        new_state=new_state,
                        timestamp=now,
                        reason=_describe_transition(record.state, new_state),
                    ),
                    record=updated,
                )
                if new_state.is_terminal and self._on_terminal is not None:
                    self._on_terminal(updated)

        self._store.update_job(job_id, last_polled=now)
        return new_state

    def _infer_terminal(
        self,
        scheduler_job_id: str,
        record: JobRecord,
        fallback_state: JobState | None = None,
    ) -> TerminalStatus | None:
        """Confirm terminal state for a job missing from ``poll_many``.

        Returns ``None`` when accounting also has no verdict so the caller
        keeps the current (non-terminal) state instead of inventing ``LOST``.

        Takes the caller's *record* rather than re-reading it: this runs once
        per disappeared job per cycle.
        """
        job_id = record.job_id
        # ShellScheduler resolves terminal status from job_dir/.exit_code; the
        # batch backends don't need job_dir.  Duck-type the optional method.
        resolve_with_dir = getattr(
            type(self._scheduler), "resolve_terminal_with_dir", None
        )
        if callable(resolve_with_dir):
            from pathlib import Path

            job_dir_value = record.metadata.get("molq.job_dir")
            if job_dir_value:
                result = _normalize_terminal_status(
                    resolve_with_dir(
                        self._scheduler, scheduler_job_id, Path(job_dir_value)
                    )
                )
                if result is not None:
                    return result
            elif self._jobs_dir:
                job_dir = Path(self._jobs_dir) / job_id
                result = _normalize_terminal_status(
                    resolve_with_dir(self._scheduler, scheduler_job_id, job_dir)
                )
                if result is not None:
                    return result

        result = _normalize_terminal_status(
            self._scheduler.resolve_terminal(scheduler_job_id)
        )
        if result is not None:
            return result
        if fallback_state is not None:
            return TerminalStatus(
                state=fallback_state,
                failure_reason=(
                    "job disappeared from scheduler"
                    if fallback_state == JobState.LOST
                    else None
                ),
            )
        return None

    def _apply_transition(
        self,
        record: JobRecord,
        old_state: JobState,
        new_state: JobState,
        timestamp: float,
        *,
        terminal: TerminalStatus | None = None,
    ) -> JobRecord | None:
        """Update store with a state transition, atomically.

        Returns the post-transition record, or ``None`` if the row's state
        changed under us (e.g. a concurrent cancel()) and the transition was
        skipped to preserve the authoritative value.

        The returned record is derived from *record* rather than re-read: we
        know exactly which columns the update touched, and this runs for every
        job that changes state on every cycle.
        """
        is_terminal = new_state.is_terminal
        started_at = (
            timestamp
            if new_state == JobState.RUNNING and old_state != JobState.RUNNING
            else None
        )
        finished_at = timestamp if is_terminal else None
        exit_code = terminal.exit_code if is_terminal and terminal is not None else None
        failure_reason = (
            terminal.failure_reason if is_terminal and terminal is not None else None
        )

        applied = self._store.compare_and_update_state(
            record.job_id,
            expected_state=old_state,
            new_state=new_state,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code,
            failure_reason=failure_reason,
        )
        if not applied:
            return None

        self._store.record_transition(
            record.job_id,
            old_state=old_state,
            new_state=new_state,
            timestamp=timestamp,
            reason=_describe_transition(old_state, new_state),
        )
        # Mirror compare_and_update_state, which only writes non-None fields.
        return replace(
            record,
            state=new_state,
            started_at=started_at if started_at is not None else record.started_at,
            finished_at=finished_at if finished_at is not None else record.finished_at,
            exit_code=exit_code if exit_code is not None else record.exit_code,
            failure_reason=(
                failure_reason if failure_reason is not None else record.failure_reason
            ),
        )

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
