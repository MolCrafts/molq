"""Scheduler protocol and implementations for molq.

Internal module — users interact through Submitor, not directly with Schedulers.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import mollog

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

logger = mollog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class Scheduler(Protocol):
    """Internal protocol for scheduler backends."""

    def capabilities(self) -> SchedulerCapabilities:
        """Return the backend capability contract."""
        ...

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        """Submit a job. Returns scheduler_job_id."""
        ...

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        """Batch query. Returns scheduler_job_id -> JobState."""
        ...

    def cancel(self, scheduler_job_id: str) -> None:
        """Cancel a job."""
        ...

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        """Determine terminal status for a disappeared job."""
        ...


@dataclass(frozen=True)
class TerminalStatus:
    """Terminal scheduler resolution with optional failure metadata."""

    state: JobState
    exit_code: int | None = None
    failure_reason: str | None = None
    raw_state: str | None = None


@dataclass(frozen=True)
class SchedulerCapabilities:
    """Declared scheduler support matrix used for submit-time validation."""

    supports_cwd: bool = False
    supports_env: bool = False
    supports_output_file: bool = False
    supports_error_file: bool = False
    supports_job_name: bool = False
    supports_cpu_count: bool = False
    supports_memory: bool = False
    supports_gpu_count: bool = False
    supports_gpu_type: bool = False
    supports_time_limit: bool = False
    supports_queue: bool = False
    supports_account: bool = False
    supports_priority: bool = False
    supports_dependency: bool = False
    supports_node_count: bool = False
    supports_exclusive_node: bool = False
    supports_array_jobs: bool = False
    supports_email: bool = False
    supports_qos: bool = False
    supports_reservation: bool = False


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

    def capabilities(self) -> SchedulerCapabilities:
        return SchedulerCapabilities(
            supports_cwd=True,
            supports_env=True,
            supports_output_file=True,
            supports_error_file=True,
        )

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

        stdout_handle = _open_output_handle(spec.execution.output_file)
        stderr_handle = _open_output_handle(spec.execution.error_file)

        try:
            proc = subprocess.Popen(
                ["bash", str(wrapper_path)],
                cwd=str(cwd),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout_handle or subprocess.DEVNULL,
                stderr=stderr_handle or subprocess.DEVNULL,
                start_new_session=True,
            )
        finally:
            if stdout_handle is not None:
                stdout_handle.close()
            if stderr_handle is not None:
                stderr_handle.close()
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

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        # We cannot determine exit code without the job_dir here.
        # The reconciler passes through resolve_terminal_with_dir instead.
        return None

    def resolve_terminal_with_dir(
        self, scheduler_job_id: str, job_dir: Path
    ) -> TerminalStatus | None:
        """Check the exit code file written by the wrapper script."""
        exit_code_path = job_dir / ".exit_code"
        if exit_code_path.exists():
            try:
                code = int(exit_code_path.read_text().strip())
                state = JobState.SUCCEEDED if code == 0 else JobState.FAILED
                reason = None if code == 0 else f"process exited with code {code}"
                return TerminalStatus(
                    state=state, exit_code=code, failure_reason=reason
                )
            except (ValueError, OSError):
                pass
        return TerminalStatus(
            state=JobState.LOST,
            failure_reason="exit code file missing for local job",
        )

    def _materialize_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run.sh"
        _atomic_write_script(script_path, _render_job_script(spec, job_dir))
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

    def capabilities(self) -> SchedulerCapabilities:
        return SchedulerCapabilities(
            supports_cwd=True,
            supports_env=True,
            supports_output_file=True,
            supports_error_file=True,
            supports_job_name=True,
            supports_cpu_count=True,
            supports_memory=True,
            supports_gpu_count=True,
            supports_gpu_type=True,
            supports_time_limit=True,
            supports_queue=True,
            supports_account=True,
            supports_dependency=True,
            supports_node_count=True,
            supports_exclusive_node=True,
            supports_array_jobs=True,
            supports_qos=True,
            supports_reservation=True,
        )

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

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
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
            parts = first_line.split("|")
            raw_state = parts[0].strip()
            state_str = raw_state.split()[0]
            state = _SLURM_SACCT_MAP.get(state_str)
            if state is None:
                return None
            exit_code = _parse_exit_code(parts[1]) if len(parts) > 1 else None
            return TerminalStatus(
                state=state,
                exit_code=exit_code,
                failure_reason=_default_failure_reason(state, exit_code, raw_state),
                raw_state=raw_state,
            )
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

        lines.extend(_render_job_lines(spec, job_dir))

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
        if s.node_count:
            mapped["nodes"] = str(s.node_count)
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

    def capabilities(self) -> SchedulerCapabilities:
        return SchedulerCapabilities(
            supports_cwd=True,
            supports_env=True,
            supports_output_file=True,
            supports_error_file=True,
            supports_job_name=True,
            supports_cpu_count=True,
            supports_memory=True,
            supports_gpu_count=True,
            supports_time_limit=True,
            supports_queue=True,
            supports_account=True,
            supports_node_count=True,
            supports_dependency=True,
        )

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

        # Query specific job IDs directly — O(queried) not O(all user jobs).
        # qstat exits non-zero for unknown/finished jobs; that's fine — those
        # jobs will be resolved as terminal by the reconciler.
        cmd = [self._opts.qstat_path] + list(scheduler_job_ids)

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
                    st = parts[4]
                    state = _PBS_STATE_MAP.get(st)
                    if state is not None:
                        result[jid] = state
            return result
        except subprocess.TimeoutExpired:
            logger.warning("qstat timed out for ids=%s", scheduler_job_ids)
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

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
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
                    exit_code = int(code)
                    if exit_code == 0:
                        return TerminalStatus(state=JobState.SUCCEEDED, exit_code=0)
                    if exit_code < 0:
                        return TerminalStatus(
                            state=JobState.CANCELLED,
                            exit_code=exit_code,
                            failure_reason=f"PBS job cancelled (exit {exit_code})",
                        )
                    return TerminalStatus(
                        state=JobState.FAILED,
                        exit_code=exit_code,
                        failure_reason=f"PBS job failed with exit code {exit_code}",
                    )
            return None
        except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
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

        lines.extend(_render_job_lines(spec, job_dir))

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
        if s.dependency:
            mapped["-W"] = f"depend={s.dependency}"

        return mapped


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

    def capabilities(self) -> SchedulerCapabilities:
        return SchedulerCapabilities(
            supports_cwd=True,
            supports_env=True,
            supports_output_file=True,
            supports_error_file=True,
            supports_job_name=True,
            supports_cpu_count=True,
            supports_memory=True,
            supports_gpu_count=True,
            supports_gpu_type=True,
            supports_time_limit=True,
            supports_queue=True,
            supports_account=True,
            supports_dependency=True,
        )

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

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
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
                return TerminalStatus(state=JobState.SUCCEEDED, raw_state="done")
            match = re.search(r"exit code\s+(\d+)", lower)
            if "exit" in lower:
                code = int(match.group(1)) if match else None
                return TerminalStatus(
                    state=JobState.FAILED,
                    exit_code=code,
                    failure_reason=_default_failure_reason(
                        JobState.FAILED, code, "exit"
                    ),
                    raw_state="exit",
                )
            return None
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

    def _generate_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run_lsf.sh"
        lines = ["#!/bin/bash"]

        directives = self._map_resources(spec)
        for key, value in directives.items():
            lines.append(f"#BSUB {key} {value}")

        lines.extend(_render_job_lines(spec, job_dir))

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
        if s.dependency:
            mapped["-w"] = f'"{s.dependency}"'

        return mapped


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


def _render_job_script(spec: JobSpec, job_dir: Path) -> str:
    return "\n".join(["#!/bin/bash", *_render_job_lines(spec, job_dir)]) + "\n"


def _render_job_lines(spec: JobSpec, job_dir: Path) -> list[str]:
    lines: list[str] = [""]

    if spec.execution.cwd:
        lines.append(f"cd {_shell_quote(str(spec.execution.cwd))}")

    if spec.execution.env:
        for key, value in sorted(spec.execution.env.items()):
            lines.append(f"export {key}={_shell_quote(value)}")

    payload = _payload_lines(spec, job_dir)
    if payload:
        lines.extend(payload)
    return lines


def _payload_lines(spec: JobSpec, job_dir: Path) -> list[str]:
    cmd = spec.command
    if cmd.argv is not None:
        return [" ".join(_shell_quote(a) for a in cmd.argv)]
    if cmd.command is not None:
        return [cmd.command]
    if cmd.script is not None:
        if cmd.script.variant == "inline":
            return (cmd.script.text or "").splitlines()
        if cmd.script.variant == "path":
            return [f'bash "{job_dir / "user_script.sh"}"']
    return []


def _parse_exit_code(field: str) -> int | None:
    try:
        return int(field.split(":")[0])
    except (ValueError, IndexError):
        return None


def _default_failure_reason(
    state: JobState, exit_code: int | None, raw_state: str | None = None
) -> str | None:
    if state == JobState.SUCCEEDED:
        return None
    if state == JobState.CANCELLED:
        return f"job was cancelled ({raw_state})" if raw_state else "job was cancelled"
    if state == JobState.TIMED_OUT:
        return "job exceeded its time limit"
    if state == JobState.LOST:
        return "job disappeared from scheduler"
    if exit_code is not None:
        return f"job failed with exit code {exit_code}"
    if raw_state:
        return f"job failed with scheduler state {raw_state}"
    return "job failed"


def _open_output_handle(path: str | None):
    if path is None:
        return None
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path.open("ab")
