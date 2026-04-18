"""Tests for molq.submitor — Submitor and JobHandle."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from molq.errors import (
    CommandError,
    ConfigError,
    JobNotFoundError,
    ScriptError,
)
from molq.models import RetryBackoff, RetryPolicy, SubmitorDefaults
from molq.options import LocalSchedulerOptions, SlurmSchedulerOptions
from molq.status import JobState
from molq.submitor import Submitor
from molq.testing import make_submitor
from molq.types import (
    DependencyRef,
    JobExecution,
    JobResources,
    JobScheduling,
    Memory,
    Script,
)


@pytest.fixture
def submitor(memory_store, mock_scheduler):
    """Submitor with mocked scheduler and in-memory store."""
    return Submitor("dev", "local", store=memory_store, _scheduler=mock_scheduler)


# ---------------------------------------------------------------------------
# Submitor.__init__
# ---------------------------------------------------------------------------


class TestSubmitorInit:
    def test_valid_scheduler(self, memory_store, mocker):
        s = Submitor("dev", "local", store=memory_store, _scheduler=mocker.MagicMock())
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

    def test_correct_options_accepted(self, memory_store, mocker):
        mocker.patch("molq.submitor.create_scheduler")
        s = Submitor(
            "dev",
            "local",
            scheduler_options=LocalSchedulerOptions(),
            store=memory_store,
        )
        assert s.cluster_name == "dev"

    def test_instances_independent(self, memory_store, mocker):
        s1 = Submitor(
            "dev1", "local", store=memory_store, _scheduler=mocker.MagicMock()
        )
        s2 = Submitor(
            "dev2", "local", store=memory_store, _scheduler=mocker.MagicMock()
        )
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
        s = Submitor(
            "dev",
            "local",
            defaults=defaults,
            store=memory_store,
            _scheduler=mock_scheduler,
        )
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
        assert "molq.job_dir" in record.metadata
        assert record.metadata["molq.stdout_path"].endswith("stdout.log")
        assert record.metadata["molq.stderr_path"].endswith("stderr.log")

    def test_submit_defaults_job_dir_under_current_workdir(
        self, memory_store, tmp_path, monkeypatch
    ):
        workdir = tmp_path / "workspace"
        workdir.mkdir()
        monkeypatch.chdir(workdir)

        scheduler = MagicMock()
        scheduler.submit.return_value = "12345"
        s = Submitor("dev", "local", store=memory_store, _scheduler=scheduler)
        handle = s.submit(argv=["echo", "hello"])

        record = s.get(handle.job_id)
        expected_job_dir = workdir / ".molq" / "jobs" / handle.job_id
        assert Path(record.metadata["molq.job_dir"]) == expected_job_dir
        assert (
            Path(record.metadata["molq.stdout_path"]) == expected_job_dir / "stdout.log"
        )
        assert (
            Path(record.metadata["molq.stderr_path"]) == expected_job_dir / "stderr.log"
        )

    def test_submit_uses_execution_cwd_for_default_job_dir(
        self, memory_store, tmp_path
    ):
        submit_cwd = tmp_path / "submit-here"
        submit_cwd.mkdir()

        scheduler = MagicMock()
        scheduler.submit.return_value = "12345"
        s = Submitor("dev", "local", store=memory_store, _scheduler=scheduler)
        handle = s.submit(
            argv=["echo", "hello"],
            execution=JobExecution(cwd=str(submit_cwd)),
        )

        record = s.get(handle.job_id)
        expected_job_dir = submit_cwd / ".molq" / "jobs" / handle.job_id
        assert Path(record.metadata["molq.job_dir"]) == expected_job_dir

    def test_submit_script_path_not_found_raises(self, submitor):
        with pytest.raises(ScriptError, match="not found"):
            submitor.submit(script=Script.path("/nonexistent/script.sh"))

    def test_submit_unique_job_ids(self, submitor):
        h1 = submitor.submit(argv=["echo", "1"])
        h2 = submitor.submit(argv=["echo", "2"])
        assert h1.job_id != h2.job_id

    def test_submit_rejects_unsupported_backend_fields(self, memory_store):
        s = Submitor("dev", "local", store=memory_store)

        with pytest.raises(ConfigError, match="resources.cpu_count"):
            s.submit(
                argv=["echo", "hello"],
                resources=JobResources(cpu_count=2),
            )

    def test_submit_rejects_unsupported_local_queue(self, memory_store):
        s = Submitor("dev", "local", store=memory_store)

        with pytest.raises(ConfigError, match="scheduling.queue"):
            s.submit(
                argv=["echo", "hello"],
                scheduling=JobScheduling(queue="gpu"),
            )

    def test_submit_accepts_supported_local_execution_fields(self, memory_store):
        s = Submitor("dev", "local", store=memory_store)
        handle = s.submit(
            argv=["echo", "hello"],
            execution=JobExecution(
                cwd=".",
                env={"HELLO": "1"},
            ),
        )
        assert handle.status() == JobState.SUBMITTED

    def test_submit_persists_dependencies(self, memory_store, mock_scheduler):
        s = Submitor("dev", "slurm", store=memory_store, _scheduler=mock_scheduler)
        parent = s.submit(argv=["echo", "parent"])
        child = s.submit(
            argv=["echo", "child"],
            after_success=[parent.job_id],
        )
        dependencies = s.get_dependencies(child.job_id)
        assert len(dependencies) == 1
        assert dependencies[0].dependency_job_id == parent.job_id
        submitted_spec = mock_scheduler.submit.call_args_list[-1].args[0]
        assert submitted_spec.scheduling.dependency.startswith("afterok:")

    def test_submit_accepts_dependency_refs(self, memory_store, mock_scheduler):
        s = Submitor("dev", "slurm", store=memory_store, _scheduler=mock_scheduler)
        parent = s.submit(argv=["echo", "parent"])
        child = s.submit(
            argv=["echo", "child"],
            scheduling=JobScheduling(
                dependencies=(DependencyRef(parent.job_id, "after_success"),)
            ),
        )
        dependencies = s.get_dependencies(child.job_id)
        assert len(dependencies) == 1
        assert dependencies[0].dependency_type == "after_success"
        submitted_spec = mock_scheduler.submit.call_args_list[-1].args[0]
        assert submitted_spec.scheduling.dependency.startswith("afterok:")

    def test_submit_after_failure_compiles_afternotok(
        self, memory_store, mock_scheduler
    ):
        s = Submitor("dev", "slurm", store=memory_store, _scheduler=mock_scheduler)
        parent = s.submit(argv=["echo", "parent"])
        child = s.submit(
            argv=["echo", "child"],
            after_failure=[parent.job_id],
        )
        dependencies = s.get_dependencies(child.job_id)
        assert len(dependencies) == 1
        assert dependencies[0].dependency_type == "after_failure"
        assert dependencies[0].scheduler_dependency.startswith("afternotok:")
        submitted_spec = mock_scheduler.submit.call_args_list[-1].args[0]
        assert submitted_spec.scheduling.dependency.startswith("afternotok:")

    def test_submit_rejects_mixed_raw_and_logical_dependencies(
        self, memory_store, mock_scheduler
    ):
        s = Submitor("dev", "slurm", store=memory_store, _scheduler=mock_scheduler)
        parent = s.submit(argv=["echo", "parent"])

        # Mutual exclusion is enforced at JobScheduling construction time.
        with pytest.raises(ValueError, match="mutually exclusive"):
            s.submit(
                argv=["echo", "child"],
                scheduling=JobScheduling(
                    dependency="afterok:manual",
                    dependencies=(DependencyRef(parent.job_id, "after_success"),),
                ),
            )

    def test_retry_policy_creates_new_attempt(self):
        with make_submitor(
            "retry",
            outcomes=["failed", "succeeded"],
            job_duration=0.0,
        ) as s:
            handle = s.submit(
                argv=["echo", "hello"],
                retry=RetryPolicy(
                    max_attempts=2,
                    backoff=RetryBackoff(initial_seconds=0.0, maximum_seconds=0.0),
                ),
            )
            record = handle.wait(timeout=1.0)
            family = s.get_retry_family(handle.job_id)
            assert record.state == JobState.SUCCEEDED
            assert len(family) == 2
            assert family[0].attempt == 1
            assert family[1].attempt == 2
            assert family[1].previous_attempt_job_id == family[0].job_id


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

    def test_get_transitions(self, submitor):
        handle = submitor.submit(argv=["echo"])
        transitions = submitor.get_transitions(handle.job_id)
        assert transitions[0].new_state == JobState.CREATED
        assert transitions[-1].new_state == JobState.SUBMITTED


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
