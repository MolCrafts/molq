"""Tests for molq.scheduler — Scheduler protocol and implementations."""

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from molq.errors import SchedulerError
from molq.models import Command, JobSpec
from molq.options import (
    SlurmSchedulerOptions,
)
from molq.scheduler import (
    LSFScheduler,
    PBSScheduler,
    QueueEntry,
    ShellScheduler,
    SlurmScheduler,
    create_scheduler,
)
from molq.status import JobState
from molq.transport import LocalTransport
from molq.types import (
    Duration,
    JobExecution,
    JobResources,
    JobScheduling,
    Memory,
    Script,
)


def _wait_exit_code(job_dir: Path, timeout: float = 5.0) -> None:
    """Block until the wrapper writes ``.exit_code`` (or timeout)."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (job_dir / ".exit_code").exists():
            return
        time.sleep(0.02)


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
# SLURM Scheduler
# ---------------------------------------------------------------------------


class TestSlurmScheduler:
    @patch("molq.transport.subprocess.run")
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

    @patch("molq.transport.subprocess.run")
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

    @patch("molq.transport.subprocess.run")
    def test_poll_many(self, mock_run):
        mock_run.return_value = MagicMock(stdout="12345 R\n12346 PD\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.poll_many(["12345", "12346"])
        assert result["12345"] == JobState.RUNNING
        assert result["12346"] == JobState.QUEUED

    @patch("molq.transport.subprocess.run")
    def test_poll_many_empty(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        scheduler = SlurmScheduler()
        assert scheduler.poll_many(["12345"]) == {}

    @patch("molq.transport.subprocess.run")
    def test_poll_many_configuring_is_queued(self, mock_run):
        mock_run.return_value = MagicMock(stdout="12345 CF\n", returncode=0)
        scheduler = SlurmScheduler()
        assert scheduler.poll_many(["12345"]) == {"12345": JobState.QUEUED}

    @patch("molq.transport.subprocess.run")
    def test_poll_many_unknown_compact_code_stays_visible(self, mock_run):
        mock_run.return_value = MagicMock(stdout="12345 XX\n", returncode=0)
        scheduler = SlurmScheduler()
        assert scheduler.poll_many(["12345"]) == {"12345": JobState.QUEUED}

    @patch("molq.transport.subprocess.run")
    def test_poll_many_strips_batch_suffix(self, mock_run):
        mock_run.return_value = MagicMock(stdout="12345.batch R\n", returncode=0)
        scheduler = SlurmScheduler()
        assert scheduler.poll_many(["12345"]) == {"12345": JobState.RUNNING}

    @patch("molq.transport.subprocess.run")
    def test_resolve_terminal_completed(self, mock_run):
        mock_run.return_value = MagicMock(stdout="COMPLETED|0:0\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.SUCCEEDED
        assert result.exit_code == 0

    @patch("molq.transport.subprocess.run")
    def test_resolve_terminal_failed(self, mock_run):
        mock_run.return_value = MagicMock(stdout="FAILED|1:0\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.FAILED
        assert result.exit_code == 1

    @patch("molq.transport.subprocess.run")
    def test_resolve_terminal_timeout(self, mock_run):
        mock_run.return_value = MagicMock(stdout="TIMEOUT|0:15\n", returncode=0)
        scheduler = SlurmScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.TIMED_OUT
        assert result.failure_reason is not None

    @patch("molq.transport.subprocess.run")
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
        assert f"bash {job_dir / 'user_script.sh'}" in script

    @patch("molq.transport.subprocess.run")
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
    @patch("molq.transport.subprocess.run")
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

    @patch("molq.transport.subprocess.run")
    def test_submit_failure(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(stdout="", stderr="qsub: error", returncode=1)
        scheduler = PBSScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        with pytest.raises(SchedulerError, match="PBS"):
            scheduler.submit(_make_spec(), job_dir)

    @patch("molq.transport.subprocess.run")
    def test_resolve_terminal_completed(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="Job: 123\n  Exit_status=0\n", returncode=0
        )
        scheduler = PBSScheduler()
        result = scheduler.resolve_terminal("123")
        assert result.state == JobState.SUCCEEDED
        assert result.exit_code == 0

    @patch("molq.transport.subprocess.run")
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
    @patch("molq.transport.subprocess.run")
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

    @patch("molq.transport.subprocess.run")
    def test_submit_failure(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(stdout="", stderr="error", returncode=1)
        scheduler = LSFScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        with pytest.raises(SchedulerError, match="LSF"):
            scheduler.submit(_make_spec(), job_dir)

    @patch("molq.transport.subprocess.run")
    def test_resolve_terminal_done(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="Summary: Done successfully.", returncode=0
        )
        scheduler = LSFScheduler()
        result = scheduler.resolve_terminal("12345")
        assert result.state == JobState.SUCCEEDED

    @patch("molq.transport.subprocess.run")
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
    def test_create_local_returns_shell_with_local_transport(self):
        s = create_scheduler("local")
        assert isinstance(s, ShellScheduler)
        assert isinstance(s._transport, LocalTransport)

    def test_create_local_uses_injected_transport(self):
        from molq.options import SshTransportOptions
        from molq.transport import SshTransport

        t = SshTransport(options=SshTransportOptions(host="workstation"))
        s = create_scheduler("local", transport=t)
        assert isinstance(s, ShellScheduler)
        assert s._transport is t

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

    def test_shell_alias_no_longer_exists(self):
        with pytest.raises(ValueError, match="Unknown"):
            create_scheduler("shell")

    def test_with_options(self):
        opts = SlurmSchedulerOptions(sbatch_path="/custom/sbatch")
        s = create_scheduler("slurm", opts)
        assert isinstance(s, SlurmScheduler)
        assert s._opts.sbatch_path == "/custom/sbatch"

    def test_create_with_transport(self):
        from molq.options import SshTransportOptions
        from molq.transport import SshTransport

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

    def test_submit_creates_run_and_wrapper_scripts(self, tmp_path: Path):
        s = ShellScheduler()
        spec = _make_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        pid = s.submit(spec, job_dir)
        assert pid.isdigit()
        assert (job_dir / "run.sh").exists()
        assert (job_dir / "_wrapper.sh").exists()
        _wait_exit_code(job_dir)

    def test_submit_argv_preserves_boundaries(self, tmp_path: Path):
        s = ShellScheduler()
        spec = _make_spec(argv=["echo", "hello world", "arg3"])
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        s.submit(spec, job_dir)
        content = (job_dir / "run.sh").read_text()
        # Arguments containing whitespace must be quoted; bare tokens stay bare.
        assert "'hello world'" in content
        assert "arg3" in content

    def test_script_permissions(self, tmp_path: Path):
        s = ShellScheduler()
        spec = _make_spec()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        s.submit(spec, job_dir)
        assert (job_dir / "run.sh").stat().st_mode & 0o700 == 0o700

    def test_submit_runs_real_process(self, tmp_path: Path):
        """End-to-end: submit a trivial job and observe it complete."""
        s = ShellScheduler()
        spec = JobSpec(
            job_id="x",
            cluster_name="dev",
            scheduler="local",
            command=Command.from_submit_args(command="echo hello > out.txt"),
            execution=JobExecution(cwd=tmp_path),
        )
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        pid = s.submit(spec, job_dir)
        assert pid.isdigit()
        _wait_exit_code(job_dir)

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
            scheduler="local",
            command=Command.from_submit_args(command="exit 7"),
            execution=JobExecution(cwd=tmp_path),
        )
        pid = s.submit(spec, job_dir)
        _wait_exit_code(job_dir)

        result = s.resolve_terminal_with_dir(pid, job_dir)
        assert result.state == JobState.FAILED
        assert result.exit_code == 7

    def test_submit_redirects_logs(self, tmp_path: Path):
        s = ShellScheduler()
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

        s.submit(spec, job_dir)
        _wait_exit_code(job_dir)

        assert stdout_path.read_text().strip() == "hello"
        assert stderr_path.read_text().strip() == "boom"

    def test_resolve_terminal_with_dir_reads_exit_code(self, tmp_path: Path):
        s = ShellScheduler()
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        (job_dir / ".exit_code").write_text("0")
        assert s.resolve_terminal_with_dir("123", job_dir).state == JobState.SUCCEEDED
        (job_dir / ".exit_code").write_text("1")
        result = s.resolve_terminal_with_dir("123", job_dir)
        assert result.state == JobState.FAILED
        assert result.exit_code == 1

    def test_resolve_terminal_missing_file_returns_lost(self, tmp_path: Path):
        s = ShellScheduler()
        result = s.resolve_terminal_with_dir("123", tmp_path)
        assert result.state == JobState.LOST
        assert "exit code file" in result.failure_reason

    def test_poll_many_running(self, tmp_path: Path):
        s = ShellScheduler()
        spec = JobSpec(
            job_id="x",
            cluster_name="dev",
            scheduler="local",
            command=Command.from_submit_args(command="sleep 5"),
            execution=JobExecution(cwd=tmp_path),
        )
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        pid = s.submit(spec, job_dir)
        assert s.poll_many([pid]).get(pid) == JobState.RUNNING
        s.cancel(pid)

    def test_poll_many_nonexistent(self):
        s = ShellScheduler()
        assert s.poll_many(["999999999"]) == {}

    def test_uses_injected_transport_for_polling(self, tmp_path: Path):
        """Verify transport.run is called; doesn't matter what state we get back."""
        from unittest.mock import MagicMock

        from molq.transport import CommandResult

        fake = MagicMock()
        fake.run = MagicMock(
            return_value=CommandResult(
                argv=("bash",),
                returncode=0,
                stdout="123=R\n",
                stderr="",
            )
        )
        s = ShellScheduler(transport=fake)
        result = s.poll_many(["123"])
        assert result == {"123": JobState.RUNNING}
        fake.run.assert_called_once()


# ---------------------------------------------------------------------------
# list_queue (squeue / qstat / bjobs)
# ---------------------------------------------------------------------------


class TestListQueue:
    def test_shell_returns_empty(self):
        assert ShellScheduler().list_queue() == []

    @patch("molq.transport.subprocess.run")
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

    @patch("molq.transport.subprocess.run")
    def test_slurm_handles_failure(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", stderr="error", returncode=1)
        assert SlurmScheduler().list_queue() == []

    @patch("molq.transport.subprocess.run")
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

    @patch("molq.transport.subprocess.run")
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


class TestListQueueResolvesTheCallingUser:
    """``user=None`` means "my jobs" — and each backend answers it its own way.

    Slurm delegates to ``squeue --me``; bare ``bjobs`` already lists only the
    invoking user. Neither needs to know a name, so neither may read the
    environment. PBS has no such form, so ``$USER`` is the single sanctioned
    environment read in molq — identity, never configuration.
    """

    @patch("molq.transport.subprocess.run")
    def test_lsf_omits_the_filter_so_bjobs_defaults_to_the_caller(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
        LSFScheduler().list_queue()
        argv = mock_run.call_args[0][0]
        assert "-u" not in argv, (
            "bare `bjobs` already lists only the invoking user's jobs; passing "
            f"-u re-answers a question LSF answers correctly. argv={argv}"
        )

    @patch("molq.transport.subprocess.run")
    def test_lsf_never_reads_the_environment(self, mock_run, monkeypatch):
        monkeypatch.setenv("USER", "laptop-bob")
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
        LSFScheduler().list_queue()
        assert "laptop-bob" not in mock_run.call_args[0][0]

    @patch("molq.transport.subprocess.run")
    def test_lsf_still_filters_when_a_user_is_named(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
        LSFScheduler().list_queue(user="alice")
        argv = mock_run.call_args[0][0]
        assert argv[argv.index("-u") + 1] == "alice"

    @patch("molq.transport.subprocess.run")
    def test_pbs_falls_back_to_user_because_qstat_has_no_me_form(
        self, mock_run, monkeypatch
    ):
        monkeypatch.setenv("USER", "alice")
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
        PBSScheduler().list_queue()
        argv = mock_run.call_args[0][0]
        assert argv[argv.index("-u") + 1] == "alice"

    @patch("molq.transport.subprocess.run")
    def test_pbs_prefers_an_explicit_user_over_the_environment(
        self, mock_run, monkeypatch
    ):
        monkeypatch.setenv("USER", "laptop-bob")
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
        PBSScheduler().list_queue(user="cluster-alice")
        argv = mock_run.call_args[0][0]
        assert argv[argv.index("-u") + 1] == "cluster-alice"
        assert "laptop-bob" not in argv

    @patch("molq.transport.subprocess.run")
    def test_pbs_never_answers_with_the_whole_cluster(self, mock_run, monkeypatch):
        """No name to filter by is not a licence to return everyone's jobs."""
        monkeypatch.delenv("USER", raising=False)
        mock_run.return_value = MagicMock(
            stdout=(
                "Job id            User      Queue    Jobname\n"
                "----------------  --------- -------- ---------\n"
                "12345.pbs01       carol     normal   other_job  1 1 8 16gb 1:00 R 0:30\n"
            ),
            stderr="",
            returncode=0,
        )
        assert PBSScheduler().list_queue() == []
        assert not mock_run.called, (
            "bare qstat asks for the whole cluster — a different question "
            "than the one list_queue() was given"
        )


class TestMolcfgOwnsTheEnvironment:
    """molq never touches :data:`os.environ`; molcfg is the only door.

    Reading through ``molcfg.environment`` forces a declaration, which is what
    makes every variable the ecosystem depends on listable. A direct read is
    invisible to any command that reports configuration — and a setting that
    lives in one shell cannot be reported at all, so two processes launched
    differently would silently disagree about it.
    """

    def test_src_never_reads_the_environment_directly(self):
        import ast

        src = Path(__file__).resolve().parent.parent / "src" / "molq"
        offenders: list[str] = []
        for path in sorted(src.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                # os.environ[...] / os.environ.get(...) / os.getenv(...)
                if isinstance(node, ast.Attribute) and node.attr == "environ":
                    offenders.append(f"{path.name}:{node.lineno} touches os.environ")
                elif (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "getenv"
                ):
                    offenders.append(f"{path.name}:{node.lineno} calls os.getenv")
        assert offenders == [], (
            "read through molcfg (`get_env_var`, after `declare_env_var`) so "
            "the variable stays listable: " + "; ".join(offenders)
        )

    def test_user_is_declared_so_it_can_be_listed(self):
        from molcfg import describe_env

        import molq.scheduler.pbs  # noqa: F401  (registers the declaration)

        row = next((r for r in describe_env() if r["name"] == "USER"), None)
        assert row is not None, "molq reads $USER but never declared it"
        assert row["project"] == "molq"
        assert row["purpose"].strip()


class TestGeneratedScriptQuoting:
    """Generated job scripts must survive paths with shell metacharacters."""

    @patch("molq.transport.subprocess.run")
    def test_script_path_with_spaces_is_quoted(self, mock_run, tmp_path: Path):
        mock_run.return_value = MagicMock(stdout="12345\n", stderr="", returncode=0)
        source = tmp_path / "source.sh"
        source.write_text("#!/bin/bash\necho hi\n")
        job_dir = tmp_path / "job dir with spaces"
        job_dir.mkdir()
        spec = JobSpec(
            job_id="spaced",
            cluster_name="alpha",
            scheduler="slurm",
            command=Command.from_submit_args(script=Script.path(source)),
        )

        SlurmScheduler().submit(spec, job_dir)
        script = (job_dir / "run_slurm.sh").read_text()
        # The interpreter line must be a single quoted argument, not a bare
        # path that the shell would split on the spaces.
        assert f"bash '{job_dir / 'user_script.sh'}'" in script


class TestLSFTerminalParsing:
    """`bhist -l` is prose that echoes the job's own command line back."""

    def _bhist(self, body: str):
        return MagicMock(stdout=body, stderr="", returncode=0)

    @patch("molq.transport.subprocess.run")
    def test_done_successfully(self, mock_run):
        mock_run.return_value = self._bhist(
            "Job <99>, User <alice>, Command <sleep 5>\n"
            "Mon Jan  1 10:01:45: Done successfully. The CPU time used is 5.0 seconds.\n"
        )
        result = LSFScheduler().resolve_terminal("99")
        assert result.state == JobState.SUCCEEDED
        assert result.exit_code == 0

    @patch("molq.transport.subprocess.run")
    def test_exited_with_code(self, mock_run):
        mock_run.return_value = self._bhist(
            "Job <99>, User <alice>, Command <false>\n"
            "Mon Jan  1 10:01:45: Exited with exit code 3. The CPU time used is 0.1s.\n"
        )
        result = LSFScheduler().resolve_terminal("99")
        assert result.state == JobState.FAILED
        assert result.exit_code == 3

    @patch("molq.transport.subprocess.run")
    def test_killed_by_owner_is_cancelled(self, mock_run):
        mock_run.return_value = self._bhist(
            "Job <99>, User <alice>, Command <sleep 500>\n"
            "Mon Jan  1 10:01:45: Signal <KILL> requested by user <alice>;\n"
            "Mon Jan  1 10:01:46: Exited by signal 15. TERM_OWNER: job killed by owner.\n"
        )
        result = LSFScheduler().resolve_terminal("99")
        assert result.state == JobState.CANCELLED

    @patch("molq.transport.subprocess.run")
    def test_command_name_containing_done_does_not_decide(self, mock_run):
        # Regression: a bare `"done" in output` check called this job a success
        # purely because of its command name.
        mock_run.return_value = self._bhist(
            "Job <99>, User <alice>, Command </work/rundone.sh>\n"
            "Mon Jan  1 10:00:05: Dispatched to <node1>;\n"
        )
        assert LSFScheduler().resolve_terminal("99") is None

    @patch("molq.transport.subprocess.run")
    def test_path_containing_exit_does_not_decide(self, mock_run):
        mock_run.return_value = self._bhist(
            "Job <99>, User <alice>, Command </work/exitpoll/run.sh>\n"
            "Mon Jan  1 10:00:05: Dispatched to <node1>;\n"
        )
        assert LSFScheduler().resolve_terminal("99") is None

    @patch("molq.transport.subprocess.run")
    def test_still_running_returns_none(self, mock_run):
        mock_run.return_value = self._bhist(
            "Job <99>, User <alice>, Command <sleep 500>\n"
            "Mon Jan  1 10:00:05: Dispatched to <node1>;\n"
            "Mon Jan  1 10:00:06: Starting (Pid 4242);\n"
        )
        assert LSFScheduler().resolve_terminal("99") is None


class TestDependencyFormatting:
    """Each backend owns its own dependency syntax."""

    def _edges(self, *pairs):
        from molq.scheduler import DependencyEdge

        return [DependencyEdge(condition=c, scheduler_job_id=i) for c, i in pairs]

    def test_slurm_comma_joins_keyword_id_pairs(self):
        edges = self._edges(("after_success", "1"), ("after_failure", "2"))
        assert SlurmScheduler().format_dependencies(edges) == "afterok:1,afternotok:2"

    def test_slurm_single_edge(self):
        (edge,) = self._edges(("after", "42"))
        assert SlurmScheduler().format_dependency(edge) == "afterany:42"

    def test_pbs_groups_ids_sharing_a_keyword(self):
        edges = self._edges(
            ("after_success", "1"), ("after_success", "2"), ("after_failure", "3")
        )
        assert PBSScheduler().format_dependencies(edges) == "afterok:1:2,afternotok:3"

    def test_lsf_builds_a_boolean_expression(self):
        edges = self._edges(("after_success", "1"), ("after_failure", "2"))
        assert LSFScheduler().format_dependencies(edges) == "done(1) && exit(2)"

    def test_lsf_single_edge(self):
        (edge,) = self._edges(("after_started", "7"))
        assert LSFScheduler().format_dependency(edge) == "started(7)"

    @pytest.mark.parametrize(
        "scheduler", [SlurmScheduler(), PBSScheduler(), LSFScheduler()]
    )
    def test_unknown_condition_is_rejected(self, scheduler):
        from molq.errors import ConfigError

        (edge,) = self._edges(("after_lunch", "1"))
        with pytest.raises(ConfigError, match="Unsupported dependency condition"):
            scheduler.format_dependency(edge)

    def test_shell_scheduler_refuses_dependencies(self):
        from molq.errors import ConfigError

        (edge,) = self._edges(("after_success", "1"))
        with pytest.raises(ConfigError, match="does not support job dependencies"):
            ShellScheduler().format_dependency(edge)

    def test_shell_scheduler_declares_no_dependency_support(self):
        assert ShellScheduler().capabilities().supports_dependency is False

    def test_empty_edge_set_is_empty_string(self):
        assert SlurmScheduler().format_dependencies([]) == ""
        assert PBSScheduler().format_dependencies([]) == ""
        assert LSFScheduler().format_dependencies([]) == ""
