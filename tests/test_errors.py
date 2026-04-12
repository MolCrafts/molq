"""Tests for molq.errors — exception hierarchy and structured context."""

import pytest

from molq.errors import (
    CommandError,
    ConfigError,
    JobNotFoundError,
    MolqError,
    MolqTimeoutError,
    SchedulerError,
    ScriptError,
    StoreError,
    SubmitError,
)


class TestHierarchy:
    def test_all_inherit_from_molq_error(self):
        classes = [
            ConfigError,
            SubmitError,
            CommandError,
            ScriptError,
            SchedulerError,
            JobNotFoundError,
            MolqTimeoutError,
            StoreError,
        ]
        for cls in classes:
            assert issubclass(cls, MolqError)

    def test_command_error_is_submit_error(self):
        assert issubclass(CommandError, SubmitError)

    def test_script_error_is_submit_error(self):
        assert issubclass(ScriptError, SubmitError)

    def test_timeout_inherits_builtin_timeout(self):
        assert issubclass(MolqTimeoutError, TimeoutError)

    def test_catch_all_via_molq_error(self):
        with pytest.raises(MolqError):
            raise CommandError("bad command")

        with pytest.raises(MolqError):
            raise MolqTimeoutError("timed out")


class TestStructuredContext:
    def test_molq_error_context(self):
        e = MolqError("fail", job_id="abc", cluster_name="dev")
        assert e.context == {"job_id": "abc", "cluster_name": "dev"}
        assert str(e) == "fail"

    def test_scheduler_error_stderr(self):
        e = SchedulerError(
            "sbatch failed",
            stderr="error: invalid partition",
            command=["sbatch", "script.sh"],
        )
        assert e.stderr == "error: invalid partition"
        assert e.command == ["sbatch", "script.sh"]
        assert str(e) == "sbatch failed"

    def test_job_not_found_error(self):
        e = JobNotFoundError("abc-123", "devbox")
        assert e.job_id == "abc-123"
        assert e.cluster_name == "devbox"
        assert "abc-123" in str(e)
        assert "devbox" in str(e)

    def test_job_not_found_error_no_cluster(self):
        e = JobNotFoundError("abc-123")
        assert e.cluster_name is None
        assert "abc-123" in str(e)

    def test_timeout_error_context(self):
        e = MolqTimeoutError("too slow", job_id="x")
        assert e.context == {"job_id": "x"}
        assert str(e) == "too slow"
