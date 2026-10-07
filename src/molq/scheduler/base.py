"""Scheduler protocol and the value types every backend speaks.

Concrete backends live in sibling modules (:mod:`molq.scheduler.slurm` and
friends); this module holds only what they share, so adding a backend never
means editing another backend's file.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from molq.errors import ConfigError
from molq.models import JobSpec
from molq.status import JobState


class Scheduler(Protocol):
    """Internal protocol for scheduler backends."""

    def capabilities(self) -> SchedulerCapabilities:
        """Return the backend capability contract."""
        ...

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        """Submit a job. Returns scheduler_job_id."""
        ...

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        """Batch query. Returns scheduler_job_id -> JobState."""
        ...

    def cancel(self, scheduler_job_id: str) -> None:
        """Cancel a job."""
        ...

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        """Confirm a *terminal* status when the job is absent from ``poll_many``.

        Return ``None`` when the outcome is unknown (not yet in accounting,
        poll blip, nested-allocation ``squeue`` lag).  ``None`` means keep
        waiting — it is not ``LOST``.
        """
        ...

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        """Return the scheduler's current queue snapshot.

        Equivalent to ``squeue --me`` (SLURM), ``qstat -u $USER`` (PBS), or
        ``bjobs`` (LSF).  Local-style schedulers return an empty list.
        """
        ...

    def format_dependency(self, edge: DependencyEdge) -> str:
        """Render one dependency edge in this backend's syntax."""
        ...

    def format_dependencies(self, edges: Sequence[DependencyEdge]) -> str:
        """Render a whole dependency set for the submit directive."""
        ...


@dataclass(frozen=True)
class DependencyEdge:
    """One "wait for that job, under this condition" edge.

    *condition* is a canonical molq ``DependencyCondition`` — ``after``,
    ``after_success``, ``after_failure``, ``after_started`` — and
    *scheduler_job_id* is the upstream job's id as the backend knows it.
    Translating the pair into submit syntax belongs to the backend, since only
    it knows whether that reads ``afterok:123``, ``done(123)``, or something
    else entirely.
    """

    condition: str
    scheduler_job_id: str


def _dependency_keyword(
    keywords: dict[str, str], condition: str, scheduler_name: str
) -> str:
    keyword = keywords.get(condition)
    if keyword is None:
        raise ConfigError(
            f"Unsupported dependency condition {condition!r}",
            scheduler=scheduler_name,
        )
    return keyword


@dataclass(frozen=True)
class QueueEntry:
    """A row from the scheduler's current queue.

    Independent of molq's own ``JobRecord``: this represents jobs as the
    scheduler sees them, including ones submitted outside molq.
    """

    scheduler_job_id: str
    name: str | None = None
    user: str | None = None
    state: JobState = JobState.QUEUED
    raw_state: str = ""
    partition: str | None = None
    submit_time: float | None = None
    start_time: float | None = None


@dataclass(frozen=True)
class TerminalStatus:
    """Terminal scheduler resolution with optional failure metadata."""

    state: JobState
    exit_code: int | None = None
    failure_reason: str | None = None
    raw_state: str | None = None


@dataclass(frozen=True)
class SchedulerCapabilities:
    """Declared scheduler support matrix used for submit-time validation."""

    supports_cwd: bool = False
    supports_env: bool = False
    supports_output_file: bool = False
    supports_error_file: bool = False
    supports_job_name: bool = False
    supports_cpu_count: bool = False
    supports_memory: bool = False
    supports_gpu_count: bool = False
    supports_gpu_type: bool = False
    supports_time_limit: bool = False
    supports_partition: bool = False
    supports_account: bool = False
    supports_priority: bool = False
    supports_dependency: bool = False
    supports_node_count: bool = False
    supports_exclusive_node: bool = False
    supports_array_jobs: bool = False
    supports_email: bool = False
    supports_qos: bool = False
    supports_reservation: bool = False
