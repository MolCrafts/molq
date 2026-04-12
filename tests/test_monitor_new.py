"""Tests for molq.monitor — JobMonitor wait/timeout."""

from unittest.mock import MagicMock

import pytest

from molq.errors import MolqTimeoutError
from molq.models import Command, JobSpec
from molq.monitor import JobMonitor
from molq.reconciler import JobReconciler
from molq.status import JobState
from molq.store import JobStore
from molq.strategies import FixedStrategy


def _insert_job(store: JobStore, job_id: str = "j1"):
    spec = JobSpec(
        job_id=job_id,
        cluster_name="dev",
        scheduler="local",
        command=Command.from_submit_args(argv=["echo"]),
    )
    store.insert_job(spec)
    store.update_job(job_id, state=JobState.RUNNING, scheduler_job_id="s1")


@pytest.fixture
def store():
    s = JobStore(":memory:")
    yield s
    s.close()


class TestWaitOne:
    def test_returns_on_terminal(self, store):
        _insert_job(store)

        mock_scheduler = MagicMock()
        # First call: still running, second call: succeeded
        mock_scheduler.poll_many.side_effect = [
            {"s1": JobState.RUNNING},
            {"s1": JobState.SUCCEEDED},
        ]
        mock_scheduler.resolve_terminal.return_value = None

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        monitor = JobMonitor(reconciler, store, strategy=FixedStrategy(0.01))

        record = monitor.wait_one("j1")
        assert record.state == JobState.SUCCEEDED

    def test_timeout_raises(self, store):
        _insert_job(store)

        mock_scheduler = MagicMock()
        mock_scheduler.poll_many.return_value = {"s1": JobState.RUNNING}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        monitor = JobMonitor(reconciler, store, strategy=FixedStrategy(0.01))

        with pytest.raises(MolqTimeoutError, match="did not complete"):
            monitor.wait_one("j1", timeout=0.05)

    def test_already_terminal(self, store):
        _insert_job(store)
        store.update_job("j1", state=JobState.SUCCEEDED)

        mock_scheduler = MagicMock()
        mock_scheduler.poll_many.return_value = {}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        monitor = JobMonitor(reconciler, store, strategy=FixedStrategy(0.01))

        record = monitor.wait_one("j1")
        assert record.state == JobState.SUCCEEDED


class TestWaitMany:
    def test_waits_for_all(self, store):
        _insert_job(store, "j1")
        # Use a different scheduler_job_id to avoid UNIQUE constraint
        spec2 = JobSpec(
            job_id="j2",
            cluster_name="dev",
            scheduler="local",
            command=Command.from_submit_args(argv=["echo"]),
        )
        store.insert_job(spec2)
        store.update_job("j2", state=JobState.RUNNING, scheduler_job_id="s2")

        mock_scheduler = MagicMock()
        mock_scheduler.poll_many.return_value = {
            "s1": JobState.SUCCEEDED,
            "s2": JobState.SUCCEEDED,
        }
        mock_scheduler.resolve_terminal.return_value = None

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        monitor = JobMonitor(reconciler, store, strategy=FixedStrategy(0.01))

        records = monitor.wait_many(["j1", "j2"], "dev", timeout=2.0)
        assert all(r.state.is_terminal for r in records)

    def test_timeout_raises(self, store):
        _insert_job(store, "j1")

        mock_scheduler = MagicMock()
        mock_scheduler.poll_many.return_value = {"s1": JobState.RUNNING}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        monitor = JobMonitor(reconciler, store, strategy=FixedStrategy(0.01))

        with pytest.raises(MolqTimeoutError):
            monitor.wait_many(["j1"], "dev", timeout=0.05)
