"""Tests for molq.reconciler — JobReconciler state sync."""

from pathlib import Path

import pytest

from molq.models import Command, JobSpec
from molq.reconciler import JobReconciler
from molq.status import JobState
from molq.store import JobStore


def _insert_job(store: JobStore, job_id: str = "j1", scheduler_job_id: str = "s1"):
    spec = JobSpec(
        job_id=job_id,
        cluster_name="dev",
        scheduler="local",
        command=Command.from_submit_args(argv=["echo", "hi"]),
        metadata={"molq.job_dir": "/tmp/work/.molq/jobs/j1"},
    )
    store.insert_job(spec)
    store.update_job(
        job_id, state=JobState.SUBMITTED, scheduler_job_id=scheduler_job_id
    )


@pytest.fixture
def store():
    s = JobStore(":memory:")
    yield s
    s.close()


class TestReconcile:
    def test_no_active_jobs(self, store, mock_scheduler):
        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()
        assert changes == []
        mock_scheduler.poll_many.assert_not_called()

    def test_running_transition(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {"s1": JobState.RUNNING}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        assert len(changes) == 1
        assert changes[0].old_state == JobState.SUBMITTED
        assert changes[0].new_state == JobState.RUNNING

        record = store.get_record("j1")
        assert record.state == JobState.RUNNING
        assert record.started_at is not None

    def test_terminal_transition(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {"s1": JobState.SUCCEEDED}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        assert len(changes) == 1
        assert changes[0].new_state == JobState.SUCCEEDED

        record = store.get_record("j1")
        assert record.state == JobState.SUCCEEDED
        assert record.finished_at is not None

    def test_disappeared_uses_resolve_terminal(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {}  # disappeared
        mock_scheduler.resolve_terminal.return_value = JobState.FAILED

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        assert len(changes) == 1
        assert changes[0].new_state == JobState.FAILED

    def test_disappeared_prefers_recorded_job_dir(self, store):
        _insert_job(store)

        class SchedulerWithDir:
            def __init__(self) -> None:
                self.calls: list[tuple[str, Path]] = []

            def poll_many(self, scheduler_job_ids):
                return {}

            def resolve_terminal_with_dir(self, scheduler_job_id, job_dir):
                self.calls.append((scheduler_job_id, job_dir))
                return JobState.SUCCEEDED

            def resolve_terminal(self, scheduler_job_id):
                return None

        mock_scheduler = SchedulerWithDir()

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        assert len(changes) == 1
        assert changes[0].new_state == JobState.SUCCEEDED
        assert mock_scheduler.calls == [("s1", Path("/tmp/work/.molq/jobs/j1"))]

    def test_disappeared_no_evidence_becomes_lost(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {}
        mock_scheduler.resolve_terminal.return_value = None

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        assert len(changes) == 1
        assert changes[0].new_state == JobState.LOST

    def test_no_change(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {"s1": JobState.SUBMITTED}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        # State didn't change
        assert len(changes) == 0

    def test_multiple_jobs(self, store, mock_scheduler):
        _insert_job(store, "j1", "s1")
        _insert_job(store, "j2", "s2")
        mock_scheduler.poll_many.return_value = {
            "s1": JobState.RUNNING,
            "s2": JobState.SUCCEEDED,
        }

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()
        assert len(changes) == 2


class TestReconcileOne:
    def test_reconcile_one(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {"s1": JobState.RUNNING}

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        state = reconciler.reconcile_one("j1")

        assert state == JobState.RUNNING

    def test_reconcile_one_terminal_skips_poll(self, store, mock_scheduler):
        _insert_job(store)
        store.update_job("j1", state=JobState.SUCCEEDED)

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        state = reconciler.reconcile_one("j1")

        assert state == JobState.SUCCEEDED
        mock_scheduler.poll_many.assert_not_called()

    def test_reconcile_one_not_found(self, store, mock_scheduler):
        reconciler = JobReconciler(mock_scheduler, store, "dev")
        state = reconciler.reconcile_one("nonexistent")
        assert state is None
