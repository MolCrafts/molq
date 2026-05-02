"""Testing utilities for molq.

Provides :class:`FakeScheduler` and the :func:`make_submitor` convenience
factory for writing tests and runnable examples without a real HPC scheduler.

Usage::

    from molq.testing import make_submitor

    # Standalone example or test:
    with make_submitor("demo") as s:
        handle = s.submit_job(argv=["echo", "hello"])
        record = handle.wait()
        print(record.state)   # JobState.SUCCEEDED

    # Inject custom outcomes / timing:
    with make_submitor("demo", outcomes=["succeeded", "failed"], job_duration=0) as s:
        h1 = s.submit_job(argv=["true"])   # will succeed
        h2 = s.submit_job(argv=["false"])  # will fail
        ...

    # Use FakeScheduler directly for fine-grained control:
    from molq.testing import FakeScheduler
    from molq.store import JobStore
    from molq.submitor import Submitor

    fake = FakeScheduler(outcomes="failed", job_duration=0)
    with Submitor("test", store=JobStore(":memory:"), _scheduler=fake) as s:
        ...
"""

from __future__ import annotations

import itertools
import threading
import time
from collections.abc import Sequence
from pathlib import Path

from molq.models import JobSpec
from molq.scheduler import SchedulerCapabilities
from molq.status import JobState
from molq.store import JobStore
from molq.submitor import Submitor

_OUTCOME_TO_STATE: dict[str, JobState] = {
    "succeeded": JobState.SUCCEEDED,
    "failed": JobState.FAILED,
    "timed_out": JobState.TIMED_OUT,
}


class FakeScheduler:
    """Fake scheduler for tests and examples.

    Simulates job execution without spawning any real process or contacting
    a cluster.  Jobs progress through states based on wall time:

    * ``elapsed < job_duration * 0.3`` → :attr:`~molq.JobState.QUEUED`
    * ``elapsed < job_duration``        → :attr:`~molq.JobState.RUNNING`
    * ``elapsed >= job_duration``       → job disappears from :meth:`poll_many`;
      :meth:`resolve_terminal` then returns the configured outcome.

    Args:
        outcomes: Outcome(s) for submitted jobs, cycled in submission order.
            A plain string applies the same outcome to every job.  A list is
            consumed in a round-robin fashion.
            Valid values: ``"succeeded"``, ``"failed"``, ``"timed_out"``.
        job_duration: Simulated wall-clock seconds until a job "completes".
            Use ``0.0`` for instant completion (useful in unit tests).
    """

    def __init__(
        self,
        *,
        outcomes: str | Sequence[str] = "succeeded",
        job_duration: float = 0.05,
    ) -> None:
        if isinstance(outcomes, str):
            self._outcomes: itertools.cycle = itertools.cycle([outcomes])
        else:
            self._outcomes = itertools.cycle(outcomes)
        self._job_duration = job_duration
        self._lock = threading.Lock()
        self._counter = itertools.count(1)
        # scheduler_job_id → {submitted_at, outcome, cancelled}
        self._jobs: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Scheduler protocol
    # ------------------------------------------------------------------

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        """Accept a job and return a fake scheduler ID (e.g. ``"fake-0001"``)."""
        sched_id = f"fake-{next(self._counter):04d}"
        outcome = next(self._outcomes)
        with self._lock:
            self._jobs[sched_id] = {
                "submitted_at": time.monotonic(),
                "outcome": outcome,
                "cancelled": False,
            }
        return sched_id

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        """Return active states for jobs still running; omit completed/cancelled jobs."""
        now = time.monotonic()
        result: dict[str, JobState] = {}
        with self._lock:
            for sid in scheduler_job_ids:
                info = self._jobs.get(sid)
                if info is None or info["cancelled"]:
                    continue  # absent → reconciler calls resolve_terminal
                elapsed = now - info["submitted_at"]
                if elapsed >= self._job_duration:
                    continue  # done → reconciler calls resolve_terminal
                result[sid] = (
                    JobState.QUEUED
                    if elapsed < self._job_duration * 0.3
                    else JobState.RUNNING
                )
        return result

    def cancel(self, scheduler_job_id: str) -> None:
        """Mark a job as cancelled."""
        with self._lock:
            if scheduler_job_id in self._jobs:
                self._jobs[scheduler_job_id]["cancelled"] = True

    def list_queue(self, *, user: str | None = None):
        """Return an empty list — FakeScheduler simulates a private execution surface."""
        return []

    def capabilities(self) -> SchedulerCapabilities:
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

    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None:
        """Return the configured terminal state for a completed or cancelled job."""
        with self._lock:
            info = self._jobs.get(scheduler_job_id)
        if info is None:
            return None
        if info["cancelled"]:
            return JobState.CANCELLED
        return _OUTCOME_TO_STATE.get(info["outcome"], JobState.SUCCEEDED)


def make_submitor(
    cluster_name: str = "demo",
    *,
    outcomes: str | Sequence[str] = "succeeded",
    job_duration: float = 0.05,
    store: JobStore | None = None,
) -> Submitor:
    """Create a :class:`~molq.Submitor` backed by :class:`FakeScheduler`.

    Intended for examples and tests.  No real process is spawned, no cluster
    connection is made.  Supports use as a context manager.

    Args:
        cluster_name: Cluster label for the Submitor (e.g. ``"demo"``).
        outcomes: Job outcome(s).  See :class:`FakeScheduler` for details.
        job_duration: Simulated wall time in seconds (``0.0`` for instant).
        store: Custom :class:`~molq.store.JobStore`.  Defaults to a fresh
            in-memory store (``":memory:"``).

    Returns:
        A ready-to-use :class:`~molq.Submitor` instance.

    Example::

        with make_submitor("demo", outcomes="failed", job_duration=0) as s:
            handle = s.submit_job(argv=["python", "train.py"])
            record = handle.wait()
            assert record.state == JobState.FAILED
    """
    from molq.cluster import Cluster

    _store = store if store is not None else JobStore(":memory:")
    fake = FakeScheduler(outcomes=outcomes, job_duration=job_duration)
    cluster = Cluster(cluster_name, "local", _scheduler_impl=fake)
    return Submitor(target=cluster, store=_store)
