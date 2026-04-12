"""Tests for molq.cli.main — CLI commands."""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from molq.cli.main import app
from molq.models import JobRecord, StatusTransition
from molq.status import JobState

runner = CliRunner()


@pytest.fixture
def mock_submitor():
    m = MagicMock()
    m.cluster_name = "cli_local"
    return m


class TestSubmitCommand:
    @patch("molq.cli.main._open_submitor")
    def test_submit_basic(self, mock_create):
        handle = MagicMock()
        handle.job_id = "test-id"
        handle.scheduler_job_id = "12345"

        mock_submitor = MagicMock()
        mock_submitor.submit.return_value = handle
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["submit", "local", "echo", "hello"])
        assert result.exit_code == 0
        assert "Job submitted" in result.output

    def test_submit_no_command(self):
        result = runner.invoke(app, ["submit", "local"])
        assert result.exit_code == 1


class TestListCommand:
    @patch("molq.cli.main._open_submitor")
    def test_list_empty(self, mock_create):
        mock_submitor = MagicMock()
        mock_submitor.list.return_value = []
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["list", "local"])
        assert result.exit_code == 0
        assert "No jobs" in result.output

    @patch("molq.cli.main._open_submitor")
    def test_list_with_jobs(self, mock_create):
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.RUNNING,
            command_type="argv",
            command_display="echo hello",
        )
        mock_submitor = MagicMock()
        mock_submitor.list.return_value = [record]
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["list", "local"])
        assert result.exit_code == 0
        assert "abc-123" in result.output


class TestStatusCommand:
    @patch("molq.cli.main._open_submitor")
    def test_status_found(self, mock_create):
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.RUNNING,
            command_type="argv",
            command_display="echo hello",
        )
        mock_submitor = MagicMock()
        mock_submitor.get.return_value = record
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["status", "abc-123", "local"])
        assert result.exit_code == 0
        assert "running" in result.output

    @patch("molq.cli.main._open_submitor")
    def test_status_not_found(self, mock_create):
        from molq.errors import JobNotFoundError

        mock_submitor = MagicMock()
        mock_submitor.get.side_effect = JobNotFoundError("abc")
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["status", "abc", "local"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestLogsCommand:
    @patch("molq.cli.main._open_submitor")
    def test_logs_stdout(self, mock_create, tmp_path):
        log_path = tmp_path / "stdout.log"
        log_path.write_text("line1\nline2\n")
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.RUNNING,
            command_type="argv",
            command_display="echo hello",
            metadata={"molq.stdout_path": str(log_path)},
        )
        mock_submitor = MagicMock()
        mock_submitor.get.return_value = record
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["logs", "abc-123", "local", "--tail", "1"])
        assert result.exit_code == 0
        assert "line2" in result.output

    @patch("molq.cli.main._open_submitor")
    def test_logs_both(self, mock_create, tmp_path):
        stdout_path = tmp_path / "stdout.log"
        stderr_path = tmp_path / "stderr.log"
        stdout_path.write_text("out\n")
        stderr_path.write_text("err\n")
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.SUCCEEDED,
            command_type="argv",
            command_display="echo hello",
            metadata={
                "molq.stdout_path": str(stdout_path),
                "molq.stderr_path": str(stderr_path),
            },
        )
        mock_submitor = MagicMock()
        mock_submitor.get.return_value = record
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["logs", "abc-123", "local", "--stream", "both"])
        assert result.exit_code == 0
        assert "[stdout] out" in result.output
        assert "[stderr] err" in result.output

    @patch("molq.cli.main._open_submitor")
    def test_logs_follow(self, mock_create, tmp_path):
        stdout_path = tmp_path / "stdout.log"
        stdout_path.write_text("line1\n")
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.SUCCEEDED,
            command_type="argv",
            command_display="echo hello",
            metadata={"molq.stdout_path": str(stdout_path)},
        )
        mock_submitor = MagicMock()
        mock_submitor.get.return_value = record
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["logs", "abc-123", "local", "--follow"])
        assert result.exit_code == 0
        assert "line1" in result.output


class TestHistoryAndInspect:
    @patch("molq.cli.main._open_submitor")
    def test_history(self, mock_create):
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.FAILED,
            scheduler_job_id="12345",
            command_type="argv",
            command_display="echo hello",
        )
        mock_submitor = MagicMock()
        mock_submitor.list.return_value = [record]
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["history", "local", "--all"])
        assert result.exit_code == 0
        assert "History" in result.output
        assert "failed" in result.output

    @patch("molq.cli.main._open_submitor")
    def test_inspect(self, mock_create):
        record = JobRecord(
            job_id="abc-123",
            cluster_name="cli_local",
            scheduler="local",
            state=JobState.RUNNING,
            scheduler_job_id="12345",
            cwd="/tmp/work",
            command_type="argv",
            command_display="echo hello",
            metadata={
                "molq.job_dir": "/tmp/jobs/abc-123",
                "molq.stdout_path": "/tmp/jobs/abc-123/stdout.log",
                "molq.stderr_path": "/tmp/jobs/abc-123/stderr.log",
            },
        )
        transitions = [
            StatusTransition(
                job_id="abc-123",
                old_state=None,
                new_state=JobState.CREATED,
                timestamp=1.0,
                reason="job created",
            ),
            StatusTransition(
                job_id="abc-123",
                old_state=JobState.CREATED,
                new_state=JobState.SUBMITTED,
                timestamp=2.0,
                reason="submitted",
            ),
        ]
        mock_submitor = MagicMock()
        mock_submitor.get.return_value = record
        mock_submitor.get_transitions.return_value = transitions
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["inspect", "abc-123", "local"])
        assert result.exit_code == 0
        assert "Scheduler ID:   12345" in result.output
        assert "Timeline:" in result.output
        assert "created" in result.output


class TestCancelCommand:
    @patch("molq.cli.main._open_submitor")
    def test_cancel_success(self, mock_create):
        mock_submitor = MagicMock()
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["cancel", "abc-123", "local"])
        assert result.exit_code == 0
        assert "cancelled" in result.output

    @patch("molq.cli.main._open_submitor")
    def test_cancel_not_found(self, mock_create):
        from molq.errors import JobNotFoundError

        mock_submitor = MagicMock()
        mock_submitor.cancel.side_effect = JobNotFoundError("abc")
        mock_create.return_value.__enter__.return_value = mock_submitor

        result = runner.invoke(app, ["cancel", "abc", "local"])
        assert result.exit_code == 1
