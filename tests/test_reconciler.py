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

    def test_disappeared_no_evidence_keeps_waiting(self, store, mock_scheduler):
        """squeue/qstat miss + no accounting row is lag, not LOST."""
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {}
        mock_scheduler.resolve_terminal.return_value = None

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        changes = reconciler.reconcile()

        assert changes == []
        assert store.get_record("j1").state == JobState.SUBMITTED

    def test_disappeared_resolve_lost_is_lost(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {}
        mock_scheduler.resolve_terminal.return_value = JobState.LOST

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

    def test_reconcile_one_poll_miss_keeps_submitted(self, store, mock_scheduler):
        _insert_job(store)
        mock_scheduler.poll_many.return_value = {}
        mock_scheduler.resolve_terminal.return_value = None

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        state = reconciler.reconcile_one("j1")

        assert state == JobState.SUBMITTED
        assert store.get_record("j1").state == JobState.SUBMITTED

    def test_reconcile_one_poll_miss_keeps_queued(self, store, mock_scheduler):
        """The peo-tg incident: queued \u2192 lost 2s after sbatch because
        squeue/sacct had not caught up. Stay queued."""
        _insert_job(store)
        store.update_job("j1", state=JobState.QUEUED)
        mock_scheduler.poll_many.return_value = {}
        mock_scheduler.resolve_terminal.return_value = None

        reconciler = JobReconciler(mock_scheduler, store, "dev")
        state = reconciler.reconcile_one("j1")

        assert state == JobState.QUEUED
        assert store.get_record("j1").state == JobState.QUEUED


class TestReconcileQueryCost:
    """A poll cycle must not issue a per-job round trip storm."""

    def _count_queries(self, store):
        """Record every statement SQLite actually executes on this connection."""
        seen: list[str] = []
        store._conn.set_trace_callback(lambda sql: seen.append(" ".join(sql.split())))
        return seen

    def test_query_count_is_flat_in_job_count(self, store, mock_scheduler):
        for i in range(12):
            _insert_job(store, job_id=f"j{i}", scheduler_job_id=f"s{i}")
        # Every job still running: no state changes, so the cycle should be
        # one read plus one batched last_polled write.
        mock_scheduler.poll_many.return_value = {
            f"s{i}": JobState.RUNNING for i in range(12)
        }
        reconciler = JobReconciler(mock_scheduler, store, "dev")
        # Prime, then measure a steady-state cycle.
        reconciler.reconcile()

        seen = self._count_queries(store)
        reconciler.reconcile()

        selects = [q for q in seen if q.upper().startswith("SELECT")]
        # One listing query; emphatically not one per job.
        assert len(selects) <= 3, selects
        updates = [q for q in seen if q.upper().startswith("UPDATE")]
        assert len(updates) == 1, updates

    def test_single_batched_last_polled_write(self, store, mock_scheduler):
        for i in range(5):
            _insert_job(store, job_id=f"j{i}", scheduler_job_id=f"s{i}")
        mock_scheduler.poll_many.return_value = {
            f"s{i}": JobState.RUNNING for i in range(5)
        }
        reconciler = JobReconciler(mock_scheduler, store, "dev")
        reconciler.reconcile()

        for i in range(5):
            record = store.get_record(f"j{i}")
            assert record is not None
            assert record.state == JobState.RUNNING

    def test_concurrent_cancel_is_not_trampled(self, store, mock_scheduler):
        """The CAS replaces the old pre-read guard; verify it still holds."""
        _insert_job(store, job_id="j1", scheduler_job_id="s1")
        reconciler = JobReconciler(mock_scheduler, store, "dev")

        # Scheduler says RUNNING, but the row is cancelled out from under us
        # between the listing read and the write.
        def poll(_ids):
            store.update_job("j1", state=JobState.CANCELLED)
            return {"s1": JobState.RUNNING}

        mock_scheduler.poll_many.side_effect = poll
        changes = reconciler.reconcile()

        assert changes == []
        record = store.get_record("j1")
        assert record is not None
        assert record.state == JobState.CANCELLED
