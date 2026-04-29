"""Tests for molq.scheduler — Scheduler protocol and implementations."""

import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from molq.errors import SchedulerError
from molq.models import Command, JobSpec
from molq.options import (
    SlurmSchedulerOptions,
)
from molq.scheduler import (
    LocalScheduler,
    LSFScheduler,
    PBSScheduler,
    QueueEntry,
    ShellScheduler,
    SlurmScheduler,
    create_scheduler,
)
from molq.transport import LocalTransport
from molq.status import JobState
from molq.types import (
    Duration,
    JobExecution,
    JobResources,
    JobScheduling,
    Memory,
    Script,
)


def _make_spec(
    job_id: str = "test-id",
    argv: list[str] | None = None,
    command: str | None = None,
) -> JobSpec:
    if argv is None and command is None:
        argv = ["echo", "hello"]
    return JobSpec(
        job_id=job_id,
        cluster_name="dev",
        scheduler="local",
        command=Command.from_submit_args(argv=argv, command=command),
    )


def _make_rich_spec() -> JobSpec:
    return JobSpec(
        job_id="rich-id",
        cluster_name="alpha",
        scheduler="slurm",
        command=Command.from_submit_args(argv=["python", "train.py"]),
        resources=JobResources(
            cpu_count=8,
            memory=Memory.gb(32),
            gpu_count=2,
            gpu_type="A100",
            time_limit=Duration.hours(4),
        ),
        scheduling=JobScheduling(partition="gpu", account="team-ml"),
        execution=JobExecution(job_name="train_job"),
    )


# ---------------------------------------------------------------------------
# Local Scheduler
# ---------------------------------------------------------------------------


class TestLocalScheduler:
    def test_submit_creates_scripts(self, tmp_path: Path):
        scheduler = LocalScheduler()
        spec = _make_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        pid = scheduler.submit(spec, job_dir)
        assert pid.isdigit()

        # Script files should exist
        assert (job_dir / "run.sh").exists()
        assert (job_dir / "_wrapper.sh").exists()

        # Wait for process to finish
        import time

        time.sleep(0.5)

    def test_submit_argv_preserves_boundaries(self, tmp_path: Path):
        scheduler = LocalScheduler()
        spec = _make_spec(argv=["echo", "hello world", "arg3"])
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        scheduler.submit(spec, job_dir)
        content = (job_dir / "run.sh").read_text()
        # Arguments with spaces should be quoted
        assert "'hello world'" in content
        assert "arg3" in content

    def test_poll_many_running(self, tmp_path: Path):
        """Submit a sleep job and verify it shows as running."""
        scheduler = LocalScheduler()
        spec = _make_spec(command="sleep 10")
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        pid = scheduler.submit(spec, job_dir)
        result = scheduler.poll_many([pid])
        assert result.get(pid) == JobState.RUNNING

        # Cancel it
        scheduler.cancel(pid)
        import time

        time.sleep(0.3)

    def test_poll_many_nonexistent(self):
        scheduler = LocalScheduler()
        result = scheduler.poll_many(["999999999"])
        assert "999999999" not in result

    def test_resolve_terminal_with_dir(self, tmp_path: Path):
        scheduler = LocalScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        # Simulate exit code file
        (job_dir / ".exit_code").write_text("0")
        assert (
            scheduler.resolve_terminal_with_dir("123", job_dir).state
            == JobState.SUCCEEDED
        )

        (job_dir / ".exit_code").write_text("1")
        result = scheduler.resolve_terminal_with_dir("123", job_dir)
        assert result.state == JobState.FAILED
        assert result.exit_code == 1

    def test_resolve_terminal_missing_file(self, tmp_path: Path):
        scheduler = LocalScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        result = scheduler.resolve_terminal_with_dir("123", job_dir)
        assert result.state == JobState.LOST
        assert "exit code file" in result.failure_reason

    def test_submit_redirects_logs(self, tmp_path: Path):
        scheduler = LocalScheduler()
        stdout_path = tmp_path / "stdout.log"
        stderr_path = tmp_path / "stderr.log"
        spec = JobSpec(
            job_id="with-logs",
            cluster_name="dev",
            scheduler="local",
            command=Command.from_submit_args(command="echo hello && echo boom 1>&2"),
            execution=JobExecution(
                cwd=tmp_path,
                output_file=str(stdout_path),
                error_file=str(stderr_path),
            ),
        )
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        pid = scheduler.submit(spec, job_dir)
        assert pid.isdigit()

        import time

        time.sleep(0.5)
        assert stdout_path.read_text().strip() == "hello"
        assert stderr_path.read_text().strip() == "boom"

    def test_script_permissions(self, tmp_path: Path):
        scheduler = LocalScheduler()
        spec = _make_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        scheduler.submit(spec, job_dir)
        assert (job_dir / "run.sh").stat().st_mode & 0o700 == 0o700


# ---------------------------------------------------------------------------
# SLURM Scheduler
# ---------------------------------------------------------------------------


class TestSlurmScheduler:
    @patch("molq.scheduler.subprocess.run")
    def test_submit_success(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(stdout="12345\n", stderr="", returncode=0)
        scheduler = SlurmScheduler()
        spec = _make_rich_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        job_id = scheduler.submit(spec, job_dir)
        assert job_id == "12345"

        # Verify script was generated
        script = (job_dir / "run_slurm.sh").read_text()
        assert "#SBATCH --partition=gpu" in script
        assert "#SBATCH --ntasks=8" in script
        assert "#SBATCH --mem=32G" in script
        assert "#SBATCH --time=04:00:00" in script
        assert "#SBATCH --gres=gpu:A100:2" in script
        assert "#SBATCH --account=team-ml" in script
        assert "#SBATCH --job-name=train_job" in script

    @patch("molq.scheduler.subprocess.run")
    def test_submit_failure(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(
            stdout="", stderr="error: invalid partition", returncode=1
        )
        scheduler = SlurmScheduler()
        spec = _make_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        with pytest.raises(SchedulerError, match="SLURM submission failed") as exc_info:
            scheduler.submit(spec, job_dir)
        assert exc_info.value.stderr == "error: invalid partition"

    @patch("molq.scheduler.subprocess.run")
    def test_poll_many(self, mock_run):
        mock_run.return_value = MagicMock(stdout="12345 R\n12346 PD\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.poll_many(["12345", "12346"])
        assert result["12345"] == JobState.RUNNING
        assert result["12346"] == JobState.QUEUED

    @patch("molq.scheduler.subprocess.run")
    def test_poll_many_empty(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        scheduler = SlurmScheduler()
        assert scheduler.poll_many(["12345"]) == {}

    @patch("molq.scheduler.subprocess.run")
    def test_resolve_terminal_completed(self, mock_run):
        mock_run.return_value = MagicMock(stdout="COMPLETED|0:0\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.SUCCEEDED
        assert result.exit_code == 0

    @patch("molq.scheduler.subprocess.run")
    def test_resolve_terminal_failed(self, mock_run):
        mock_run.return_value = MagicMock(stdout="FAILED|1:0\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.FAILED
        assert result.exit_code == 1

    @patch("molq.scheduler.subprocess.run")
    def test_resolve_terminal_timeout(self, mock_run):
        mock_run.return_value = MagicMock(stdout="TIMEOUT|0:15\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.TIMED_OUT
        assert result.failure_reason is not None

    @patch("molq.scheduler.subprocess.run")
    def test_submit_script_path_uses_materialized_script(
        self, mock_run, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(stdout="12345\n", stderr="", returncode=0)
        scheduler = SlurmScheduler()
        source = tmp_path / "source.sh"
        source.write_text("#!/bin/bash\necho from-source\n")
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        shutil.copy2(source, job_dir / "user_script.sh")
        spec = JobSpec(
            job_id="script-path",
            cluster_name="alpha",
            scheduler="slurm",
            command=Command.from_submit_args(script=Script.path(source)),
        )

        scheduler.submit(spec, job_dir)
        script = (job_dir / "run_slurm.sh").read_text()
        assert 'bash "' in script
        assert "user_script.sh" in script

    @patch("molq.scheduler.subprocess.run")
    def test_cancel(self, mock_run):
        scheduler = SlurmScheduler()
        scheduler.cancel("12345")
        mock_run.assert_called_once()
        assert "12345" in mock_run.call_args[0][0]

    def test_custom_options(self):
        opts = SlurmSchedulerOptions(sbatch_path="/opt/slurm/bin/sbatch")
        scheduler = SlurmScheduler(opts)
        assert scheduler._opts.sbatch_path == "/opt/slurm/bin/sbatch"


# ---------------------------------------------------------------------------
# PBS Scheduler
# ---------------------------------------------------------------------------


class TestPBSScheduler:
    @patch("molq.scheduler.subprocess.run")
    def test_submit_success(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(
            stdout="12345.pbs01\n", stderr="", returncode=0
        )
        scheduler = PBSScheduler()
        spec = _make_rich_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        job_id = scheduler.submit(spec, job_dir)
        assert job_id == "12345"

        script = (job_dir / "run_pbs.sh").read_text()
        assert "#PBS -l" in script
        assert "mem=32gb" in script

    @patch("molq.scheduler.subprocess.run")
    def test_submit_failure(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(stdout="", stderr="qsub: error", returncode=1)
        scheduler = PBSScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        with pytest.raises(SchedulerError, match="PBS"):
            scheduler.submit(_make_spec(), job_dir)

    @patch("molq.scheduler.subprocess.run")
    def test_resolve_terminal_completed(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="Job: 123\n  Exit_status=0\n", returncode=0
        )
        scheduler = PBSScheduler()
        result = scheduler.resolve_terminal("123")
        assert result.state == JobState.SUCCEEDED
        assert result.exit_code == 0

    @patch("molq.scheduler.subprocess.run")
    def test_submit_script_path_uses_materialized_script(
        self, mock_run, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(
            stdout="12345.pbs01\n", stderr="", returncode=0
        )
        scheduler = PBSScheduler()
        source = tmp_path / "source.sh"
        source.write_text("#!/bin/bash\necho from-source\n")
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        shutil.copy2(source, job_dir / "user_script.sh")
        spec = JobSpec(
            job_id="script-path",
            cluster_name="alpha",
            scheduler="pbs",
            command=Command.from_submit_args(script=Script.path(source)),
        )

        scheduler.submit(spec, job_dir)
        script = (job_dir / "run_pbs.sh").read_text()
        assert "user_script.sh" in script


# ---------------------------------------------------------------------------
# LSF Scheduler
# ---------------------------------------------------------------------------


class TestLSFScheduler:
    @patch("molq.scheduler.subprocess.run")
    def test_submit_success(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(
            stdout="Job <12345> is submitted to queue <normal>.\n",
            stderr="",
            returncode=0,
        )
        scheduler = LSFScheduler()
        spec = _make_rich_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        job_id = scheduler.submit(spec, job_dir)
        assert job_id == "12345"

        script = (job_dir / "run_lsf.sh").read_text()
        assert "#BSUB -q gpu" in script

    @patch("molq.scheduler.subprocess.run")
    def test_submit_failure(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(stdout="", stderr="error", returncode=1)
        scheduler = LSFScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        with pytest.raises(SchedulerError, match="LSF"):
            scheduler.submit(_make_spec(), job_dir)

    @patch("molq.scheduler.subprocess.run")
    def test_resolve_terminal_done(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="Summary: Done successfully.", returncode=0
        )
        scheduler = LSFScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.SUCCEEDED

    @patch("molq.scheduler.subprocess.run")
    def test_submit_script_path_uses_materialized_script(
        self, mock_run, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(
            stdout="Job <12345> is submitted to queue <normal>.\n",
            stderr="",
            returncode=0,
        )
        scheduler = LSFScheduler()
        source = tmp_path / "source.sh"
        source.write_text("#!/bin/bash\necho from-source\n")
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        shutil.copy2(source, job_dir / "user_script.sh")
        spec = JobSpec(
            job_id="script-path",
            cluster_name="alpha",
            scheduler="lsf",
            command=Command.from_submit_args(script=Script.path(source)),
        )

        scheduler.submit(spec, job_dir)
        script = (job_dir / "run_lsf.sh").read_text()
        assert "user_script.sh" in script


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


class TestFactory:
    def test_create_local(self):
        s = create_scheduler("local")
        assert isinstance(s, LocalScheduler)

    def test_create_slurm(self):
        s = create_scheduler("slurm")
        assert isinstance(s, SlurmScheduler)

    def test_create_pbs(self):
        s = create_scheduler("pbs")
        assert isinstance(s, PBSScheduler)

    def test_create_lsf(self):
        s = create_scheduler("lsf")
        assert isinstance(s, LSFScheduler)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown"):
            create_scheduler("unknown")

    def test_with_options(self):
        opts = SlurmSchedulerOptions(sbatch_path="/custom/sbatch")
        s = create_scheduler("slurm", opts)
        assert isinstance(s, SlurmScheduler)
        assert s._opts.sbatch_path == "/custom/sbatch"

    def test_create_shell(self):
        s = create_scheduler("shell")
        assert isinstance(s, ShellScheduler)

    def test_create_with_transport(self):
        from molq.transport import SshTransport
        from molq.options import SshTransportOptions

        t = SshTransport(options=SshTransportOptions(host="cluster"))
        s = create_scheduler("slurm", transport=t)
        assert s._transport is t


# ---------------------------------------------------------------------------
# ShellScheduler — transport-aware "no batch system" dispatcher
# ---------------------------------------------------------------------------


class TestShellScheduler:
    def test_default_transport_is_local(self):
        s = ShellScheduler()
        assert isinstance(s._transport, LocalTransport)

    def test_submit_runs_real_process(self, tmp_path: Path):
        """End-to-end: submit a trivial job and observe it complete."""
        s = ShellScheduler()
        spec = _make_spec(command="echo hello > out.txt")
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        spec = JobSpec(
            job_id="x",
            cluster_name="dev",
            scheduler="shell",
            command=spec.command,
            execution=JobExecution(cwd=tmp_path),
        )

        pid = s.submit(spec, job_dir)
        assert pid.isdigit()

        # Wait for the wrapper's exit_code file to appear
        import time
        for _ in range(50):
            if (job_dir / ".exit_code").exists():
                break
            time.sleep(0.05)

        result = s.resolve_terminal_with_dir(pid, job_dir)
        assert result is not None
        assert result.state == JobState.SUCCEEDED
        assert result.exit_code == 0
        assert (tmp_path / "out.txt").read_text().strip() == "hello"

    def test_submit_records_failure_exit_code(self, tmp_path: Path):
        s = ShellScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        spec = JobSpec(
            job_id="x",
            cluster_name="dev",
            scheduler="shell",
            command=Command.from_submit_args(command="exit 7"),
            execution=JobExecution(cwd=tmp_path),
        )
        pid = s.submit(spec, job_dir)

        import time
        for _ in range(50):
            if (job_dir / ".exit_code").exists():
                break
            time.sleep(0.05)

        result = s.resolve_terminal_with_dir(pid, job_dir)
        assert result.state == JobState.FAILED
        assert result.exit_code == 7

    def test_resolve_terminal_missing_file_returns_lost(self, tmp_path: Path):
        s = ShellScheduler()
        result = s.resolve_terminal_with_dir("123", tmp_path)
        assert result.state == JobState.LOST

    def test_poll_many_running(self, tmp_path: Path):
        s = ShellScheduler()
        spec = JobSpec(
            job_id="x",
            cluster_name="dev",
            scheduler="shell",
            command=Command.from_submit_args(command="sleep 5"),
            execution=JobExecution(cwd=tmp_path),
        )
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        pid = s.submit(spec, job_dir)
        assert s.poll_many([pid]).get(pid) == JobState.RUNNING
        s.cancel(pid)

    def test_uses_injected_transport_for_polling(self, tmp_path: Path):
        """Verify transport.run is called; doesn't matter what state we get back."""
        from unittest.mock import MagicMock

        from molq.transport import CommandResult

        fake = MagicMock()
        fake.run = MagicMock(return_value=CommandResult(
            argv=("bash",), returncode=0, stdout="123=R\n", stderr="",
        ))
        s = ShellScheduler(transport=fake)
        result = s.poll_many(["123"])
        assert result == {"123": JobState.RUNNING}
        fake.run.assert_called_once()


# ---------------------------------------------------------------------------
# list_queue (squeue / qstat / bjobs)
# ---------------------------------------------------------------------------


class TestListQueue:
    def test_local_returns_empty(self):
        assert LocalScheduler().list_queue() == []

    def test_shell_returns_empty(self):
        assert ShellScheduler().list_queue() == []

    @patch("molq.scheduler.subprocess.run")
    def test_slurm_parses_squeue_output(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=(
                "12345|train_job|alice|R|gpu|2024-03-15T14:30:00|2024-03-15T14:31:00\n"
                "12346|eval_job|alice|PD|gpu|2024-03-15T14:32:00|N/A\n"
            ),
            stderr="",
            returncode=0,
        )
        entries = SlurmScheduler().list_queue()
        assert len(entries) == 2
        assert entries[0] == QueueEntry(
            scheduler_job_id="12345",
            name="train_job",
            user="alice",
            state=JobState.RUNNING,
            raw_state="R",
            partition="gpu",
            submit_time=entries[0].submit_time,  # checked below
            start_time=entries[0].start_time,
        )
        assert entries[0].submit_time is not None
        assert entries[0].start_time is not None
        assert entries[1].state == JobState.QUEUED
        assert entries[1].start_time is None  # 'N/A' parsed as None

    @patch("molq.scheduler.subprocess.run")
    def test_slurm_handles_failure(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", stderr="error", returncode=1)
        assert SlurmScheduler().list_queue() == []

    @patch("molq.scheduler.subprocess.run")
    def test_pbs_parses_qstat_output(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=(
                "Job id            User      Queue    Jobname    SessID NDS TSK Mem  Time S Elap\n"
                "----------------  --------- -------- ---------  ------ --- --- ---- ---- - ----\n"
                "12345.pbs01       alice     normal   train_job  1234   1   8   16gb 1:00 R 0:30\n"
            ),
            stderr="",
            returncode=0,
        )
        entries = PBSScheduler().list_queue(user="alice")
        assert len(entries) == 1
        e = entries[0]
        assert e.scheduler_job_id == "12345"
        assert e.user == "alice"
        assert e.partition == "normal"
        assert e.name == "train_job"
        assert e.state == JobState.RUNNING
        assert e.raw_state == "R"

    @patch("molq.scheduler.subprocess.run")
    def test_lsf_parses_bjobs_output(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="12345 RUN train_job alice gpu Mar 15 14:30 Mar 15 14:31\n",
            stderr="",
            returncode=0,
        )
        entries = LSFScheduler().list_queue(user="alice")
        assert len(entries) == 1
        assert entries[0].scheduler_job_id == "12345"
        assert entries[0].state == JobState.RUNNING
        assert entries[0].partition == "gpu"
        assert entries[0].name == "train_job"
