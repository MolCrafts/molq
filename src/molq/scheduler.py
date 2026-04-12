"""Scheduler protocol and implementations for molq.

Internal module — users interact through Submitor, not directly with Schedulers.
"""

from __future__ import annotations

import getpass
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)

from molq.errors import SchedulerError
from molq.models import JobSpec
from molq.options import (
    LocalSchedulerOptions,
    LSFSchedulerOptions,
    PBSSchedulerOptions,
    SchedulerOptions,
    SlurmSchedulerOptions,
)
from molq.status import JobState

# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class Scheduler(Protocol):
    """Internal protocol for scheduler backends."""

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        """Submit a job. Returns scheduler_job_id."""
        ...

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        """Batch query. Returns scheduler_job_id -> JobState."""
        ...

    def cancel(self, scheduler_job_id: str) -> None:
        """Cancel a job."""
        ...

    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None:
        """Determine terminal status for a disappeared job."""
        ...


# ---------------------------------------------------------------------------
# Local Scheduler
# ---------------------------------------------------------------------------


class LocalScheduler:
    """Execute jobs as local subprocesses.

    Each spawned process runs in its own session (``start_new_session=True``)
    so that signal-based cancellation can target the entire process group.
    A background reaper thread ``wait()``s every handle to make sure no
    completed job is left as a zombie attached to this process.
    """

    def __init__(self, options: LocalSchedulerOptions | None = None) -> None:
        self._opts = options or LocalSchedulerOptions()
        self._procs: dict[int, subprocess.Popen] = {}
        self._procs_lock = threading.Lock()

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        script_path = self._materialize_script(spec, job_dir)
        exit_code_path = job_dir / ".exit_code"

        # Wrapper that records exit code.  fsync after writing so the bytes
        # are durable on disk before bash tries to interpret them — matters on
        # NFS / slow disks where the kernel buffer hasn't flushed yet.
        wrapper_path = job_dir / "_wrapper.sh"
        _atomic_write_script(
            wrapper_path,
            f'#!/bin/bash\nbash "{script_path}"\necho $? > "{exit_code_path}"\n',
        )

        cwd = spec.execution.cwd or spec.cwd
        env = None
        if spec.execution.env:
            env = {**os.environ, **spec.execution.env}

        proc = subprocess.Popen(
            ["bash", str(wrapper_path)],
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._track(proc)
        return str(proc.pid)

    def _track(self, proc: subprocess.Popen) -> None:
        """Hold a reference to the Popen and reap it when it exits."""
        with self._procs_lock:
            self._procs[proc.pid] = proc

        def _reap() -> None:
            try:
                proc.wait()
            except Exception:
                logger.debug("reaper failed for pid=%s", proc.pid, exc_info=True)
            finally:
                with self._procs_lock:
                    self._procs.pop(proc.pid, None)

        t = threading.Thread(target=_reap, name=f"molq-reaper-{proc.pid}", daemon=True)
        t.start()

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        result: dict[str, JobState] = {}
        for pid_str in scheduler_job_ids:
            try:
                pid = int(pid_str)
                os.kill(pid, 0)
                result[pid_str] = JobState.RUNNING
            except ProcessLookupError:
                # Process gone — will be resolved via resolve_terminal
                pass
            except (ValueError, PermissionError):
                pass
        return result

    def cancel(self, scheduler_job_id: str) -> None:
        try:
            pid = int(scheduler_job_id)
        except ValueError:
            return
        # Signal the whole session (negative pid) so children die too.
        try:
            os.killpg(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                return

        # Brief grace period, then SIGKILL whatever is left.
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.02)
        try:
            os.killpg(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None:
        # We cannot determine exit code without the job_dir here.
        # The reconciler passes through resolve_terminal_with_dir instead.
        return None

    def resolve_terminal_with_dir(
        self, scheduler_job_id: str, job_dir: Path
    ) -> JobState | None:
        """Check the exit code file written by the wrapper script."""
        exit_code_path = job_dir / ".exit_code"
        if exit_code_path.exists():
            try:
                code = int(exit_code_path.read_text().strip())
                return JobState.SUCCEEDED if code == 0 else JobState.FAILED
            except (ValueError, OSError):
                pass
        return JobState.LOST

    def _materialize_script(self, spec: JobSpec, job_dir: Path) -> Path:
        cmd = spec.command
        script_path = job_dir / "run.sh"

        if cmd.script is not None:
            if cmd.script.variant == "inline":
                _atomic_write_script(script_path, f"#!/bin/bash\n{cmd.script.text}\n")
            elif cmd.script.variant == "path" and cmd.script.file_path:
                shutil.copy2(cmd.script.file_path, script_path)
                script_path.chmod(0o700)
        elif cmd.argv is not None:
            args_escaped = " ".join(_shell_quote(a) for a in cmd.argv)
            _atomic_write_script(script_path, f"#!/bin/bash\n{args_escaped}\n")
        elif cmd.command is not None:
            _atomic_write_script(script_path, f"#!/bin/bash\n{cmd.command}\n")

        return script_path


# ---------------------------------------------------------------------------
# SLURM Scheduler
# ---------------------------------------------------------------------------

_SLURM_STATE_MAP: dict[str, JobState] = {
    "R": JobState.RUNNING,
    "PD": JobState.QUEUED,
    "CD": JobState.SUCCEEDED,
    "CG": JobState.RUNNING,
    "CA": JobState.CANCELLED,
    "F": JobState.FAILED,
    "TO": JobState.TIMED_OUT,
    "NF": JobState.FAILED,
    "OOM": JobState.FAILED,
}

_SLURM_SACCT_MAP: dict[str, JobState] = {
    "COMPLETED": JobState.SUCCEEDED,
    "FAILED": JobState.FAILED,
    "CANCELLED": JobState.CANCELLED,
    "TIMEOUT": JobState.TIMED_OUT,
    "OUT_OF_MEMORY": JobState.FAILED,
    "NODE_FAIL": JobState.FAILED,
    "PREEMPTED": JobState.CANCELLED,
}


class SlurmScheduler:
    """Submit and manage jobs via SLURM."""

    def __init__(self, options: SlurmSchedulerOptions | None = None) -> None:
        self._opts = options or SlurmSchedulerOptions()

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        script_path = self._generate_script(spec, job_dir)
        cmd = [self._opts.sbatch_path, "--parsable", str(script_path)]
        cmd.extend(self._opts.extra_sbatch_flags)

        try:
            proc = subprocess.run(
                cmd, capture_output=True, check=True, text=True, timeout=60
            )
            return proc.stdout.strip().split(";")[0]
        except subprocess.CalledProcessError as e:
            raise SchedulerError(
                "SLURM submission failed",
                stderr=e.stderr,
                command=cmd,
            ) from e
        except subprocess.TimeoutExpired as e:
            raise SchedulerError(
                "SLURM submission timed out",
                command=cmd,
            ) from e

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        if not scheduler_job_ids:
            return {}

        ids_str = ",".join(scheduler_job_ids)
        cmd = [
            self._opts.squeue_path,
            "-j",
            ids_str,
            "-h",
            "-o",
            "%i %t",
        ]

        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, check=False, timeout=30
            )
            if not proc.stdout.strip():
                return {}

            result: dict[str, JobState] = {}
            for line in proc.stdout.strip().split("\n"):
                parts = line.split()
                if len(parts) >= 2:
                    jid, st = parts[0], parts[1]
                    state = _SLURM_STATE_MAP.get(st)
                    if state is not None:
                        result[jid] = state
            return result
        except subprocess.TimeoutExpired:
            logger.warning("squeue timed out for ids=%s", ids_str)
            return {}
        except OSError as e:
            logger.error("squeue invocation failed: %s", e)
            return {}

    def cancel(self, scheduler_job_id: str) -> None:
        subprocess.run(
            [self._opts.scancel_path, scheduler_job_id],
            capture_output=True,
            timeout=30,
        )

    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None:
        try:
            proc = subprocess.run(
                [
                    self._opts.sacct_path,
                    "-j",
                    scheduler_job_id,
                    "-o",
                    "State,ExitCode",
                    "-n",
                    "-P",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
            if proc.returncode != 0 or not proc.stdout.strip():
                return None

            first_line = proc.stdout.strip().split("\n")[0]
            state_str = first_line.split("|")[0].strip().split()[0]
            return _SLURM_SACCT_MAP.get(state_str)
        except (subprocess.TimeoutExpired, FileNotFoundError, IndexError):
            return None

    def _generate_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run_slurm.sh"
        lines = ["#!/bin/bash"]

        # SBATCH directives
        directives = self._map_resources(spec)
        for key, value in directives.items():
            if value == "":
                lines.append(f"#SBATCH --{key}")
            else:
                lines.append(f"#SBATCH --{key}={value}")

        lines.append("")
        lines.append(self._command_text(spec))

        script_path.write_text("\n".join(lines) + "\n")
        script_path.chmod(0o700)
        return script_path

    def _map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped: dict[str, str] = {}
        r, s, e = spec.resources, spec.scheduling, spec.execution

        if s.queue:
            mapped["partition"] = s.queue
        if r.cpu_count:
            mapped["ntasks"] = str(r.cpu_count)
        if r.memory:
            mapped["mem"] = r.memory.to_slurm()
        if r.time_limit:
            mapped["time"] = r.time_limit.to_slurm()
        if e.job_name:
            mapped["job-name"] = e.job_name
        if e.output_file:
            mapped["output"] = e.output_file
        if e.error_file:
            mapped["error"] = e.error_file
        if r.gpu_count:
            gres = f"gpu:{r.gpu_count}"
            if r.gpu_type:
                gres = f"gpu:{r.gpu_type}:{r.gpu_count}"
            mapped["gres"] = gres
        if s.exclusive_node:
            mapped["exclusive"] = ""
        if s.account:
            mapped["account"] = s.account
        if s.qos:
            mapped["qos"] = s.qos
        if s.dependency:
            mapped["dependency"] = s.dependency
        if s.array_spec:
            mapped["array"] = s.array_spec
        if s.reservation:
            mapped["reservation"] = s.reservation

        return mapped

    def _command_text(self, spec: JobSpec) -> str:
        cmd = spec.command
        if cmd.argv is not None:
            return " ".join(_shell_quote(a) for a in cmd.argv)
        if cmd.command is not None:
            return cmd.command
        if cmd.script is not None:
            if cmd.script.variant == "inline":
                return cmd.script.text or ""
            if cmd.script.variant == "path" and cmd.script.file_path:
                return f'bash "{cmd.script.file_path}"'
        return ""


# ---------------------------------------------------------------------------
# PBS Scheduler
# ---------------------------------------------------------------------------

_PBS_STATE_MAP: dict[str, JobState] = {
    "R": JobState.RUNNING,
    "Q": JobState.QUEUED,
    "H": JobState.QUEUED,
    "E": JobState.RUNNING,
    "C": JobState.SUCCEEDED,
    "T": JobState.RUNNING,
    "W": JobState.QUEUED,
    "S": JobState.QUEUED,
}


class PBSScheduler:
    """Submit and manage jobs via PBS/Torque."""

    def __init__(self, options: PBSSchedulerOptions | None = None) -> None:
        self._opts = options or PBSSchedulerOptions()

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        script_path = self._generate_script(spec, job_dir)
        cmd = [self._opts.qsub_path, str(script_path)]
        cmd.extend(self._opts.extra_qsub_flags)

        try:
            proc = subprocess.run(
                cmd, capture_output=True, check=True, text=True, timeout=60
            )
            raw_id = proc.stdout.strip()
            return raw_id.split(".")[0]
        except subprocess.CalledProcessError as e:
            raise SchedulerError(
                "PBS submission failed",
                stderr=e.stderr,
                command=cmd,
            ) from e

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        if not scheduler_job_ids:
            return {}

        user = getpass.getuser()
        cmd = [self._opts.qstat_path, "-u", user]

        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, check=False, timeout=30
            )
            if not proc.stdout.strip():
                return {}

            wanted = set(scheduler_job_ids)
            result: dict[str, JobState] = {}
            for line in proc.stdout.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("-") or line.startswith("Job"):
                    continue
                parts = line.split()
                if len(parts) < 5:
                    continue
                jid = parts[0].split(".")[0]
                if jid in wanted:
                    st = parts[4] if len(parts) > 4 else ""
                    state = _PBS_STATE_MAP.get(st)
                    if state is not None:
                        result[jid] = state
            return result
        except subprocess.TimeoutExpired:
            logger.warning("qstat timed out (user=%s)", user)
            return {}
        except OSError as e:
            logger.error("qstat invocation failed: %s", e)
            return {}

    def cancel(self, scheduler_job_id: str) -> None:
        subprocess.run(
            [self._opts.qdel_path, scheduler_job_id],
            capture_output=True,
            timeout=30,
        )

    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None:
        try:
            proc = subprocess.run(
                [self._opts.tracejob_path, scheduler_job_id],
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
            if proc.returncode != 0 or not proc.stdout.strip():
                return None

            for line in proc.stdout.split("\n"):
                if "Exit_status=" in line:
                    code = line.split("Exit_status=")[1].split()[0]
                    if code == "0":
                        return JobState.SUCCEEDED
                    if code == "-11":
                        return JobState.CANCELLED
                    return JobState.FAILED
            return None
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

    def _generate_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run_pbs.sh"
        lines = ["#!/bin/bash"]

        directives = self._map_resources(spec)
        for key, value in directives.items():
            if key == "-l":
                for item in value.split(","):
                    lines.append(f"#PBS -l {item}")
            else:
                lines.append(f"#PBS {key} {value}")

        lines.append("")
        lines.append(self._command_text(spec))

        script_path.write_text("\n".join(lines) + "\n")
        script_path.chmod(0o700)
        return script_path

    def _map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped: dict[str, str] = {}
        r, s, e = spec.resources, spec.scheduling, spec.execution

        resource_parts: list[str] = []
        node_count = s.node_count or 1
        ppn = r.cpu_count if s.node_count is None else None
        if ppn:
            resource_parts.append(f"nodes={node_count}:ppn={ppn}")
        else:
            resource_parts.append(f"nodes={node_count}")

        if r.memory:
            resource_parts.append(f"mem={r.memory.to_pbs()}")
        if r.time_limit:
            resource_parts.append(f"walltime={r.time_limit.to_pbs()}")
        if r.gpu_count:
            resource_parts.append(f"gpus={r.gpu_count}")

        mapped["-l"] = ",".join(resource_parts)

        if e.job_name:
            mapped["-N"] = e.job_name
        if e.output_file:
            mapped["-o"] = e.output_file
        if e.error_file:
            mapped["-e"] = e.error_file
        if s.queue:
            mapped["-q"] = s.queue
        if s.account:
            mapped["-A"] = s.account

        return mapped

    def _command_text(self, spec: JobSpec) -> str:
        cmd = spec.command
        if cmd.argv is not None:
            return " ".join(_shell_quote(a) for a in cmd.argv)
        if cmd.command is not None:
            return cmd.command
        if cmd.script is not None and cmd.script.variant == "inline":
            return cmd.script.text or ""
        return ""


# ---------------------------------------------------------------------------
# LSF Scheduler
# ---------------------------------------------------------------------------

_LSF_STATE_MAP: dict[str, JobState] = {
    "RUN": JobState.RUNNING,
    "PEND": JobState.QUEUED,
    "DONE": JobState.SUCCEEDED,
    "EXIT": JobState.FAILED,
    "USUSP": JobState.QUEUED,
    "SSUSP": JobState.QUEUED,
    "PSUSP": JobState.QUEUED,
    "WAIT": JobState.QUEUED,
    "ZOMBI": JobState.FAILED,
}


class LSFScheduler:
    """Submit and manage jobs via IBM Spectrum LSF."""

    def __init__(self, options: LSFSchedulerOptions | None = None) -> None:
        self._opts = options or LSFSchedulerOptions()

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        script_path = self._generate_script(spec, job_dir)
        cmd = [self._opts.bsub_path]
        cmd.extend(self._opts.extra_bsub_flags)

        try:
            proc = subprocess.run(
                cmd,
                input=script_path.read_text(),
                capture_output=True,
                check=True,
                text=True,
                timeout=60,
            )
            match = re.search(r"Job <(\d+)>", proc.stdout)
            if not match:
                raise SchedulerError(
                    f"Could not parse job ID from bsub output: {proc.stdout}",
                    command=cmd,
                )
            return match.group(1)
        except subprocess.CalledProcessError as e:
            raise SchedulerError(
                "LSF submission failed",
                stderr=e.stderr,
                command=cmd,
            ) from e

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        if not scheduler_job_ids:
            return {}

        cmd = [self._opts.bjobs_path, "-noheader"] + list(scheduler_job_ids)

        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, check=False, timeout=30
            )
            if not proc.stdout.strip():
                return {}

            wanted = set(scheduler_job_ids)
            result: dict[str, JobState] = {}
            for line in proc.stdout.strip().split("\n"):
                parts = line.split()
                if len(parts) < 3:
                    continue
                jid = parts[0]
                if jid in wanted:
                    st = parts[2]
                    state = _LSF_STATE_MAP.get(st)
                    if state is not None:
                        result[jid] = state
            return result
        except subprocess.TimeoutExpired:
            logger.warning("bjobs timed out for ids=%s", scheduler_job_ids)
            return {}
        except OSError as e:
            logger.error("bjobs invocation failed: %s", e)
            return {}

    def cancel(self, scheduler_job_id: str) -> None:
        subprocess.run(
            [self._opts.bkill_path, scheduler_job_id],
            capture_output=True,
            timeout=30,
        )

    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None:
        try:
            proc = subprocess.run(
                [self._opts.bhist_path, "-l", scheduler_job_id],
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
            if proc.returncode != 0 or not proc.stdout.strip():
                return None

            lower = proc.stdout.lower()
            if "done" in lower:
                return JobState.SUCCEEDED
            if "exit" in lower:
                return JobState.FAILED
            return None
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

    def _generate_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run_lsf.sh"
        lines = ["#!/bin/bash"]

        directives = self._map_resources(spec)
        for key, value in directives.items():
            lines.append(f"#BSUB {key} {value}")

        lines.append("")
        lines.append(self._command_text(spec))

        script_path.write_text("\n".join(lines) + "\n")
        script_path.chmod(0o700)
        return script_path

    def _map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped: dict[str, str] = {}
        r, s, e = spec.resources, spec.scheduling, spec.execution

        if s.queue:
            mapped["-q"] = s.queue
        if r.cpu_count:
            mapped["-n"] = str(r.cpu_count)
        if r.memory:
            mapped["-M"] = str(r.memory.to_lsf_kb())
        if r.time_limit:
            mapped["-W"] = str(r.time_limit.to_lsf_minutes())
        if e.job_name:
            name = e.job_name
            if s.array_spec:
                name = f"{name}[{s.array_spec}]"
            mapped["-J"] = name
        if e.output_file:
            mapped["-o"] = e.output_file
        if e.error_file:
            mapped["-e"] = e.error_file
        if s.account:
            mapped["-P"] = s.account
        if r.gpu_count:
            gpu_str = f"num={r.gpu_count}"
            if r.gpu_type:
                gpu_str += f":mode=exclusive_process:gmodel={r.gpu_type}"
            mapped["-gpu"] = gpu_str

        return mapped

    def _command_text(self, spec: JobSpec) -> str:
        cmd = spec.command
        if cmd.argv is not None:
            return " ".join(_shell_quote(a) for a in cmd.argv)
        if cmd.command is not None:
            return cmd.command
        if cmd.script is not None and cmd.script.variant == "inline":
            return cmd.script.text or ""
        return ""


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_scheduler(
    scheduler_name: str,
    options: SchedulerOptions | None = None,
) -> LocalScheduler | SlurmScheduler | PBSScheduler | LSFScheduler:
    """Create a Scheduler implementation by name."""
    if scheduler_name == "local":
        return LocalScheduler(
            options if isinstance(options, LocalSchedulerOptions) else None
        )
    if scheduler_name == "slurm":
        return SlurmScheduler(
            options if isinstance(options, SlurmSchedulerOptions) else None
        )
    if scheduler_name == "pbs":
        return PBSScheduler(
            options if isinstance(options, PBSSchedulerOptions) else None
        )
    if scheduler_name == "lsf":
        return LSFScheduler(
            options if isinstance(options, LSFSchedulerOptions) else None
        )
    raise ValueError(f"Unknown scheduler: {scheduler_name!r}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _shell_quote(s: str) -> str:
    """Quote a string for safe shell usage."""
    if not s:
        return "''"
    if re.match(r"^[a-zA-Z0-9_/.\-=:@]+$", s):
        return s
    return "'" + s.replace("'", "'\"'\"'") + "'"


def _atomic_write_script(path: Path, content: str) -> None:
    """Write a shell script and ``fsync`` it before chmod.

    Without ``fsync`` the contents may still sit in the page cache when bash
    is exec'd against the file — usually fine on a local SSD, but on NFS or
    a slow disk under heavy concurrency this can lead to bash seeing an
    empty script.
    """
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(str(path), 0o700)
