"""Job monitoring engine for molq.

Provides blocking wait and polling with pluggable strategies.
Internal module — users interact through Submitor and JobHandle.
"""

from __future__ import annotations

import threading

import mollog

from molq.errors import MolqTimeoutError
from molq.models import JobRecord
from molq.reconciler import JobReconciler
from molq.status import JobState
from molq.store import JobStore
from molq.strategies import ExponentialBackoffStrategy, PollingStrategy

logger = mollog.get_logger(__name__)


class JobMonitor:
    """Polling engine with pluggable strategies.

    Not intended for direct user construction.
    """

    def __init__(
        self,
        reconciler: JobReconciler,
        store: JobStore,
        strategy: PollingStrategy | None = None,
    ) -> None:
        self._reconciler = reconciler
        self._store = store
        self._strategy = strategy or ExponentialBackoffStrategy()
        self._stop = threading.Event()

    def wait_one(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
    ) -> JobRecord:
        """Block until a single job reaches a terminal state.

        Raises:
            MolqTimeoutError: If timeout exceeded.
        """
        import time

        start = time.time()
        poll_count = 0

        try:
            while True:
                latest = self._store.get_latest_attempt_record(job_id)
                watched_job_id = latest.job_id if latest is not None else job_id
                state = self._reconciler.reconcile_one(watched_job_id)

                if state is not None and JobState(state).is_terminal:
                    record = self._store.get_latest_attempt_record(job_id)
                    if record is not None:
                        if not record.state.is_terminal:
                            poll_count += 1
                            continue
                        if record.job_id != watched_job_id:
                            poll_count += 1
                            continue
                        return record

                if timeout is not None and (time.time() - start) > timeout:
                    raise MolqTimeoutError(
                        f"Job {job_id} did not complete within {timeout}s",
                        job_id=job_id,
                    )

                interval = self._strategy.next_interval(time.time() - start, poll_count)
                # wait() returns True iff the event was set during the wait,
                # so we observe stop() promptly without a check-then-wait race.
                if self._stop.wait(interval):
                    break
                poll_count += 1

        except KeyboardInterrupt:
            logger.info("Monitoring interrupted by user")
            raise

        # Stopped externally
        record = self._store.get_record(job_id)
        if record is None:
            from molq.errors import JobNotFoundError

            raise JobNotFoundError(job_id)
        return record

    def wait_many(
        self,
        job_ids: list[str] | None,
        cluster_name: str,
        *,
        timeout: float | None = None,
    ) -> list[JobRecord]:
        """Block until specified jobs (or all active) reach terminal state.

        Raises:
            MolqTimeoutError: If timeout exceeded.
        """
        import time

        start = time.time()
        poll_count = 0

        try:
            while True:
                self._reconciler.reconcile()

                if job_ids is not None:
                    records = [
                        self._store.get_latest_attempt_record(jid) for jid in job_ids
                    ]
                    records = [r for r in records if r is not None]
                    all_terminal = all(r.state.is_terminal for r in records)
                else:
                    active = self._store.get_active_records(cluster_name)
                    all_terminal = len(active) == 0

                if all_terminal:
                    if job_ids is not None:
                        return [r for r in records if r is not None]
                    return self._store.list_records(cluster_name, include_terminal=True)

                if timeout is not None and (time.time() - start) > timeout:
                    raise MolqTimeoutError(
                        f"Jobs did not complete within {timeout}s",
                    )

                interval = self._strategy.next_interval(time.time() - start, poll_count)
                if self._stop.wait(interval):
                    break
                poll_count += 1

        except KeyboardInterrupt:
            logger.info("Monitoring interrupted by user")
            raise

        return self._store.list_records(cluster_name, include_terminal=True)

    def stop(self) -> None:
        """Signal the monitor to stop."""
        self._stop.set()
