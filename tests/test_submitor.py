"""Tests for molq.submitor — Submitor and JobHandle."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from molq.errors import (
    CommandError,
    ConfigError,
    JobNotFoundError,
    ScriptError,
)
from molq.models import SubmitorDefaults
from molq.options import LocalSchedulerOptions, SlurmSchedulerOptions
from molq.status import JobState
from molq.store import JobStore
from molq.submitor import JobHandle, Submitor
from molq.types import JobResources, JobScheduling, Memory, Script


@pytest.fixture
def memory_store():
    store = JobStore(":memory:")
    yield store
    store.close()


@pytest.fixture
def mock_scheduler():
    m = MagicMock()
    _counter = iter(range(10000, 99999))
    m.submit.side_effect = lambda spec, job_dir: str(next(_counter))
    m.poll_many.return_value = {}
    m.resolve_terminal.return_value = None
    return m


@pytest.fixture
def submitor(memory_store, mock_scheduler):
    """Submitor with mocked scheduler and in-memory store."""
    with patch("molq.submitor.create_scheduler", return_value=mock_scheduler):
        s = Submitor("dev", "local", store=memory_store)
    return s


# ---------------------------------------------------------------------------
# Submitor.__init__
# ---------------------------------------------------------------------------


class TestSubmitorInit:
    def test_valid_scheduler(self, memory_store):
        with patch("molq.submitor.create_scheduler"):
            s = Submitor("dev", "local", store=memory_store)
        assert s.cluster_name == "dev"

    def test_invalid_scheduler_raises(self, memory_store):
        with pytest.raises(ConfigError, match="Unknown scheduler"):
            Submitor("dev", "invalid", store=memory_store)

    def test_mismatched_options_raises(self, memory_store):
        with pytest.raises(TypeError, match="LocalSchedulerOptions"):
            Submitor(
                "dev",
                "local",
                scheduler_options=SlurmSchedulerOptions(),
                store=memory_store,
            )

    def test_correct_options_accepted(self, memory_store):
        with patch("molq.submitor.create_scheduler"):
            s = Submitor(
                "dev",
                "local",
                scheduler_options=LocalSchedulerOptions(),
                store=memory_store,
            )
        assert s.cluster_name == "dev"

    def test_instances_independent(self, memory_store):
        with patch("molq.submitor.create_scheduler"):
            s1 = Submitor("dev1", "local", store=memory_store)
            s2 = Submitor("dev2", "local", store=memory_store)
        assert s1.cluster_name != s2.cluster_name


# ---------------------------------------------------------------------------
# submit()
# ---------------------------------------------------------------------------


class TestSubmit:
    def test_submit_argv(self, submitor, mock_scheduler):
        handle = submitor.submit(argv=["echo", "hello"])
        assert handle.job_id is not None
        assert handle.scheduler_job_id is not None
        assert handle.status() == JobState.SUBMITTED
        mock_scheduler.submit.assert_called_once()

    def test_submit_command(self, submitor):
        handle = submitor.submit(command="echo hello && echo world")
        assert handle.status() == JobState.SUBMITTED

    def test_submit_script_inline(self, submitor):
        handle = submitor.submit(script=Script.inline("echo hello\necho world"))
        assert handle.status() == JobState.SUBMITTED

    def test_submit_script_path(self, submitor, tmp_path):
        f = tmp_path / "run.sh"
        f.write_text("#!/bin/bash\necho hello")
        handle = submitor.submit(script=Script.path(f))
        assert handle.status() == JobState.SUBMITTED

    def test_submit_no_command_raises(self, submitor):
        with pytest.raises(CommandError, match="Exactly one"):
            submitor.submit()

    def test_submit_two_commands_raises(self, submitor):
        with pytest.raises(CommandError, match="Exactly one"):
            submitor.submit(argv=["echo"], command="echo")

    def test_submit_newline_in_command_raises(self, submitor):
        with pytest.raises(CommandError, match="newline"):
            submitor.submit(command="echo\nhello")

    def test_submit_with_resources(self, submitor, mock_scheduler):
        handle = submitor.submit(
            argv=["python", "train.py"],
            resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
        )
        assert handle.status() == JobState.SUBMITTED

    def test_submit_with_defaults(self, memory_store, mock_scheduler):
        defaults = SubmitorDefaults(
            resources=JobResources(cpu_count=4),
            scheduling=JobScheduling(queue="normal"),
        )
        with patch("molq.submitor.create_scheduler", return_value=mock_scheduler):
            s = Submitor("dev", "local", defaults=defaults, store=memory_store)

        handle = s.submit(argv=["echo"])
        record = s.get(handle.job_id)
        assert record is not None

    def test_submit_stores_record(self, submitor, memory_store):
        handle = submitor.submit(argv=["echo", "hello"])
        record = memory_store.get_record(handle.job_id)
        assert record is not None
        assert record.state == JobState.SUBMITTED
        assert record.scheduler_job_id is not None
        assert record.command_type == "argv"

    def test_submit_script_path_not_found_raises(self, submitor):
        with pytest.raises(ScriptError, match="not found"):
            submitor.submit(script=Script.path("/nonexistent/script.sh"))

    def test_submit_unique_job_ids(self, submitor):
        h1 = submitor.submit(argv=["echo", "1"])
        h2 = submitor.submit(argv=["echo", "2"])
        assert h1.job_id != h2.job_id


# ---------------------------------------------------------------------------
# get / list / cancel
# ---------------------------------------------------------------------------


class TestSubmitorOps:
    def test_get_existing(self, submitor):
        handle = submitor.submit(argv=["echo"])
        record = submitor.get(handle.job_id)
        assert record.job_id == handle.job_id

    def test_get_nonexistent_raises(self, submitor):
        with pytest.raises(JobNotFoundError):
            submitor.get("nonexistent")

    def test_list_active(self, submitor):
        submitor.submit(argv=["echo", "1"])
        submitor.submit(argv=["echo", "2"])
        records = submitor.list()
        assert len(records) == 2

    def test_list_with_terminal(self, submitor, memory_store):
        h = submitor.submit(argv=["echo"])
        memory_store.update_job(h.job_id, state=JobState.SUCCEEDED)

        active = submitor.list(include_terminal=False)
        all_jobs = submitor.list(include_terminal=True)
        assert len(active) == 0
        assert len(all_jobs) == 1

    def test_cancel(self, submitor, mock_scheduler):
        handle = submitor.submit(argv=["echo"])
        submitor.cancel(handle.job_id)

        record = submitor.get(handle.job_id)
        assert record.state == JobState.CANCELLED
        mock_scheduler.cancel.assert_called_once_with(handle.scheduler_job_id)

    def test_cancel_nonexistent_raises(self, submitor):
        with pytest.raises(JobNotFoundError):
            submitor.cancel("nonexistent")


# ---------------------------------------------------------------------------
# JobHandle
# ---------------------------------------------------------------------------


class TestJobHandle:
    def test_status_cached(self, submitor):
        handle = submitor.submit(argv=["echo"])
        assert handle.status() == JobState.SUBMITTED

    def test_cancel(self, submitor, mock_scheduler):
        handle = submitor.submit(argv=["echo"])
        handle.cancel()
        assert handle.status() == JobState.CANCELLED
        mock_scheduler.cancel.assert_called_once()

    def test_refresh(self, submitor, mock_scheduler):
        handle = submitor.submit(argv=["echo"])
        sid = handle.scheduler_job_id
        mock_scheduler.poll_many.return_value = {sid: JobState.RUNNING}

        handle.refresh()
        assert handle.status() == JobState.RUNNING


# ---------------------------------------------------------------------------
# Zero side effects
# ---------------------------------------------------------------------------


class TestZeroSideEffects:
    def test_import_creates_no_files(self, tmp_path, monkeypatch):
        """Importing molq must not create files or directories."""
        monkeypatch.setenv("HOME", str(tmp_path))
        molq_dir = tmp_path / ".molq"

        # Import should not create .molq dir
        import importlib

        import molq

        importlib.reload(molq)
        # The directory may exist from other tests, but importing
        # molq itself should not trigger DB creation
        # The key assertion: no jobs.db created on import
        assert not (molq_dir / "jobs.db").exists()
