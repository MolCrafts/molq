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
from datetime import datetime
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
from molq.transport import LocalTransport, Transport, TransportError

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

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        """Return the scheduler's current queue snapshot.

        Equivalent to ``squeue --me`` (SLURM), ``qstat -u $USER`` (PBS), or
        ``bjobs`` (LSF).  Local-style schedulers return an empty list.
        """
        ...


@dataclass(frozen=True)
class QueueEntry:
    """A row from the scheduler's current queue.

    Independent of molq's own ``JobRecord``: this represents jobs as the
    scheduler sees them, including ones submitted outside molq.
    """

    scheduler_job_id: str
    name: str | None = None
    user: str | None = None
    state: JobState = JobState.QUEUED
    raw_state: str = ""
    partition: str | None = None
    submit_time: float | None = None
    start_time: float | None = None


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
    supports_partition: bool = False
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
                logger.debug(f"reaper failed for pid={proc.pid}", exc_info=True)
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

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        return []

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
# Shell Scheduler (transport-aware "no batch system, just run it")
# ---------------------------------------------------------------------------


class ShellScheduler:
    """Run jobs via a plain shell on whatever the transport points at.

    Where :class:`LocalScheduler` uses an in-process :class:`subprocess.Popen`
    and a reaper thread, ``ShellScheduler`` is transport-agnostic: it writes a
    wrapper script that backgrounds the user command, captures its pid into a
    sibling file, and writes the exit code on completion.  Polling reads those
    files via the transport instead of touching local OS state.

    The trade-off vs ``LocalScheduler`` is one extra fork (the wrapper) per
    job — negligible against any real workload.  The benefit is that
    ``ShellScheduler(transport=SshTransport(...))`` runs jobs directly on a
    remote workstation with no batch system, which is the canonical "I just
    want to ssh into my desktop and run this" use case.
    """

    def __init__(
        self,
        options: LocalSchedulerOptions | None = None,
        *,
        transport: Transport | None = None,
    ) -> None:
        self._opts = options or LocalSchedulerOptions()
        self._transport: Transport = transport or LocalTransport()

    def capabilities(self) -> SchedulerCapabilities:
        return SchedulerCapabilities(
            supports_cwd=True,
            supports_env=True,
            supports_output_file=True,
            supports_error_file=True,
            # job_name is purely metadata for the shell path — accept it so
            # callers like molexp's SubmitHandler can set it uniformly.
            supports_job_name=True,
        )

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        # Materialize the user script locally first (it may reference files
        # the user expects on disk).  Submitor is responsible for staging the
        # job_dir to the transport's filesystem before submit when the
        # transport is non-local.
        script_path = self._materialize_script(spec, job_dir)
        wrapper_path = job_dir / "_wrapper.sh"
        pid_path = job_dir / ".pid"
        exit_code_path = job_dir / ".exit_code"

        # The wrapper runs the user script in the background, records its
        # pid, waits for it, and stores the exit code.  We then daemonise the
        # wrapper itself so closing the ssh channel doesn't kill the job.
        cwd = spec.execution.cwd or spec.cwd
        env_lines = ""
        if spec.execution.env:
            env_lines = (
                "\n".join(
                    f"export {k}={_shell_quote(v)}"
                    for k, v in sorted(spec.execution.env.items())
                )
                + "\n"
            )
        cd_line = f"cd {_shell_quote(str(cwd))}\n" if cwd else ""

        out_redir = (
            f" > {_shell_quote(spec.execution.output_file)}"
            if spec.execution.output_file
            else " > /dev/null"
        )
        err_redir = (
            f" 2> {_shell_quote(spec.execution.error_file)}"
            if spec.execution.error_file
            else " 2> /dev/null"
        )

        wrapper = (
            f"#!/bin/bash\n"
            f"{env_lines}"
            f"{cd_line}"
            f"( bash {_shell_quote(str(script_path))}{out_redir}{err_redir} ) &\n"
            f"pid=$!\n"
            f"echo $pid > {_shell_quote(str(pid_path))}\n"
            f"wait $pid\n"
            f"echo $? > {_shell_quote(str(exit_code_path))}\n"
        )
        # Wrapper script lands on the transport's filesystem so that ssh-routed
        # ShellScheduler instances find it on the remote at the same path the
        # remote shell will see.
        self._transport.write_text(str(wrapper_path), wrapper, mode=0o700)

        # Launch the wrapper as a detached background process via the transport
        # and capture the pid the wrapper writes into .pid.  We also put the
        # wrapper itself into nohup + background so closing the ssh session
        # doesn't terminate it.
        launch = (
            f"nohup bash {_shell_quote(str(wrapper_path))} "
            f"> /dev/null 2>&1 < /dev/null &\n"
            f"echo $!\n"
        )
        try:
            result = self._transport.run(["bash", "-c", launch], timeout=30)
        except TransportError as e:
            raise SchedulerError(
                "shell submission failed",
                command=["bash", "-c", launch],
            ) from e
        if result.returncode != 0:
            raise SchedulerError(
                "shell submission failed",
                stderr=result.stderr,
                command=["bash", "-c", launch],
            )
        # The pid we get back is the wrapper's pid; the actual job pid is in
        # .pid on the transport's filesystem.  Either pid identifies the job
        # for kill/poll purposes — we use the inner job pid because that's the
        # one users would expect (e.g. matches `ps`).  Wait briefly for the
        # wrapper to write .pid (typical: <50ms).
        import time as _time

        deadline = _time.monotonic() + 5.0
        while _time.monotonic() < deadline:
            if self._transport.exists(str(pid_path)):
                try:
                    return self._transport.read_text(str(pid_path)).strip()
                except (FileNotFoundError, TransportError):
                    pass
            _time.sleep(0.02)
        # Fallback: return the wrapper pid; cancel/poll still work via this pid.
        return result.stdout.strip()

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        # Use `kill -0 <pid>` which exits 0 if the process is alive, 1 otherwise.
        # Batch into a single shell to keep round-trip count to one.
        if not scheduler_job_ids:
            return {}
        checks = " ; ".join(
            f"kill -0 {_shell_quote(p)} 2>/dev/null && echo {_shell_quote(p)}=R || true"
            for p in scheduler_job_ids
        )
        try:
            result = self._transport.run(["bash", "-c", checks], timeout=15)
        except TransportError:
            return {}
        out: dict[str, JobState] = {}
        for line in result.stdout.strip().split("\n"):
            line = line.strip()
            if line.endswith("=R"):
                out[line[:-2]] = JobState.RUNNING
        return out

    def cancel(self, scheduler_job_id: str) -> None:
        # SIGTERM, brief grace period, SIGKILL — match LocalScheduler semantics.
        cmd = (
            f"kill -TERM {_shell_quote(scheduler_job_id)} 2>/dev/null ; "
            f"sleep 0.5 ; "
            f"kill -KILL {_shell_quote(scheduler_job_id)} 2>/dev/null ; true"
        )
        try:
            self._transport.run(["bash", "-c", cmd], timeout=10)
        except TransportError:
            pass

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        # Without job_dir we can't read .exit_code; the reconciler must call
        # resolve_terminal_with_dir.  Mirror LocalScheduler.
        return None

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        return []

    def resolve_terminal_with_dir(
        self, scheduler_job_id: str, job_dir: Path
    ) -> TerminalStatus | None:
        exit_code_path = str(job_dir / ".exit_code")
        try:
            if not self._transport.exists(exit_code_path):
                return TerminalStatus(
                    state=JobState.LOST,
                    failure_reason="exit code file missing for shell job",
                )
            text = self._transport.read_text(exit_code_path).strip()
            code = int(text)
        except (FileNotFoundError, TransportError, ValueError):
            return TerminalStatus(
                state=JobState.LOST,
                failure_reason="exit code file unreadable for shell job",
            )
        state = JobState.SUCCEEDED if code == 0 else JobState.FAILED
        reason = None if code == 0 else f"process exited with code {code}"
        return TerminalStatus(state=state, exit_code=code, failure_reason=reason)

    def _materialize_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run.sh"
        self._transport.write_text(
            str(script_path),
            _render_job_script(spec, job_dir),
            mode=0o700,
        )
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
    """Submit and manage jobs via SLURM.

    All shell calls (``sbatch``, ``squeue``, ``scancel``, ``sacct``) go through
    the injected :class:`~molq.transport.Transport` — defaulting to
    :class:`~molq.transport.LocalTransport` for byte-identical behaviour to
    pre-transport molq.  Pass an :class:`~molq.transport.SshTransport` to drive
    a remote SLURM cluster from a laptop.
    """

    def __init__(
        self,
        options: SlurmSchedulerOptions | None = None,
        *,
        transport: Transport | None = None,
    ) -> None:
        self._opts = options or SlurmSchedulerOptions()
        self._transport: Transport = transport or LocalTransport()

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
            supports_partition=True,
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
            result = self._transport.run(cmd, timeout=60)
        except TransportError as e:
            raise SchedulerError(
                "SLURM submission timed out",
                command=cmd,
            ) from e
        if result.returncode != 0:
            raise SchedulerError(
                "SLURM submission failed",
                stderr=result.stderr,
                command=cmd,
            )
        return result.stdout.strip().split(";")[0]

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
            result = self._transport.run(cmd, timeout=30)
        except TransportError as e:
            logger.warning(f"squeue invocation failed: {e}")
            return {}
        if not result.stdout.strip():
            return {}

        out: dict[str, JobState] = {}
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) >= 2:
                jid, st = parts[0], parts[1]
                state = _SLURM_STATE_MAP.get(st)
                if state is not None:
                    out[jid] = state
        return out

    def cancel(self, scheduler_job_id: str) -> None:
        try:
            self._transport.run(
                [self._opts.scancel_path, scheduler_job_id],
                timeout=30,
            )
        except TransportError:
            pass

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        try:
            result = self._transport.run(
                [
                    self._opts.sacct_path,
                    "-j",
                    scheduler_job_id,
                    "-o",
                    "State,ExitCode",
                    "-n",
                    "-P",
                ],
                timeout=15,
            )
        except TransportError:
            return None
        if result.returncode != 0 or not result.stdout.strip():
            return None
        try:
            first_line = result.stdout.strip().split("\n")[0]
            parts = first_line.split("|")
            raw_state = parts[0].strip()
            state_str = raw_state.split()[0]
        except IndexError:
            return None
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

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        cmd: list[str] = [self._opts.squeue_path, "-h", "-o", "%i|%j|%u|%t|%P|%V|%S"]
        if user is None:
            cmd.append("--me")
        else:
            cmd += ["-u", user]
        try:
            result = self._transport.run(cmd, timeout=30)
        except TransportError as exc:
            logger.warning(f"squeue invocation failed: {exc}")
            return []
        if result.returncode != 0 or not result.stdout.strip():
            return []
        entries: list[QueueEntry] = []
        for line in result.stdout.strip().split("\n"):
            parts = line.split("|")
            if len(parts) < 7:
                continue
            jid, name, usr, raw_state, part, sub_t, start_t = parts[:7]
            entries.append(
                QueueEntry(
                    scheduler_job_id=jid,
                    name=name or None,
                    user=usr or None,
                    state=_SLURM_STATE_MAP.get(raw_state, JobState.QUEUED),
                    raw_state=raw_state,
                    partition=part or None,
                    submit_time=_parse_slurm_time(sub_t),
                    start_time=_parse_slurm_time(start_t),
                )
            )
        return entries

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

        self._transport.write_text(
            str(script_path), "\n".join(lines) + "\n", mode=0o700
        )
        return script_path

    def _map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped: dict[str, str] = {}
        r, s, e = spec.resources, spec.scheduling, spec.execution

        if s.partition:
            mapped["partition"] = s.partition
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
    """Submit and manage jobs via PBS/Torque.

    All shell calls (``qsub``, ``qstat``, ``qdel``, ``tracejob``) go through
    the injected :class:`~molq.transport.Transport`.
    """

    def __init__(
        self,
        options: PBSSchedulerOptions | None = None,
        *,
        transport: Transport | None = None,
    ) -> None:
        self._opts = options or PBSSchedulerOptions()
        self._transport: Transport = transport or LocalTransport()

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
            supports_partition=True,
            supports_account=True,
            supports_node_count=True,
            supports_dependency=True,
        )

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        script_path = self._generate_script(spec, job_dir)
        cmd = [self._opts.qsub_path, str(script_path)]
        cmd.extend(self._opts.extra_qsub_flags)

        try:
            result = self._transport.run(cmd, timeout=60)
        except TransportError as e:
            raise SchedulerError("PBS submission timed out", command=cmd) from e
        if result.returncode != 0:
            raise SchedulerError(
                "PBS submission failed",
                stderr=result.stderr,
                command=cmd,
            )
        return result.stdout.strip().split(".")[0]

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        if not scheduler_job_ids:
            return {}

        # Query specific job IDs directly — O(queried) not O(all user jobs).
        # qstat exits non-zero for unknown/finished jobs; that's fine — those
        # jobs will be resolved as terminal by the reconciler.
        cmd = [self._opts.qstat_path] + list(scheduler_job_ids)

        try:
            result = self._transport.run(cmd, timeout=30)
        except TransportError as e:
            logger.warning(f"qstat invocation failed: {e}")
            return {}
        if not result.stdout.strip():
            return {}

        wanted = set(scheduler_job_ids)
        out: dict[str, JobState] = {}
        for line in result.stdout.strip().split("\n"):
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
                    out[jid] = state
        return out

    def cancel(self, scheduler_job_id: str) -> None:
        try:
            self._transport.run(
                [self._opts.qdel_path, scheduler_job_id],
                timeout=30,
            )
        except TransportError:
            pass

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        try:
            result = self._transport.run(
                [self._opts.tracejob_path, scheduler_job_id],
                timeout=15,
            )
        except TransportError:
            return None
        if result.returncode != 0 or not result.stdout.strip():
            return None
        for line in result.stdout.split("\n"):
            if "Exit_status=" in line:
                try:
                    code = line.split("Exit_status=")[1].split()[0]
                    exit_code = int(code)
                except (IndexError, ValueError):
                    return None
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

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        target_user = user or os.environ.get("USER") or ""
        cmd: list[str] = [self._opts.qstat_path]
        if target_user:
            cmd += ["-u", target_user]
        try:
            result = self._transport.run(cmd, timeout=30)
        except TransportError as exc:
            logger.warning(f"qstat invocation failed: {exc}")
            return []
        if result.returncode != 0 or not result.stdout.strip():
            return []
        entries: list[QueueEntry] = []
        for line in result.stdout.split("\n"):
            line = line.strip()
            if not line or line.startswith("-") or line.startswith("Job"):
                continue
            parts = line.split()
            # Typical columns: id user queue jobname sessid nds tsk req_mem req_time s elap
            if len(parts) < 10:
                continue
            jid = parts[0].split(".")[0]
            usr = parts[1]
            partition = parts[2]
            name = parts[3]
            raw_state = parts[9] if len(parts) > 9 else ""
            entries.append(
                QueueEntry(
                    scheduler_job_id=jid,
                    name=name or None,
                    user=usr or None,
                    state=_PBS_STATE_MAP.get(raw_state, JobState.QUEUED),
                    raw_state=raw_state,
                    partition=partition or None,
                )
            )
        return entries

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

        self._transport.write_text(
            str(script_path), "\n".join(lines) + "\n", mode=0o700
        )
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
        if s.partition:
            mapped["-q"] = s.partition
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
    """Submit and manage jobs via IBM Spectrum LSF.

    All shell calls (``bsub``, ``bjobs``, ``bkill``, ``bhist``) go through the
    injected :class:`~molq.transport.Transport`.
    """

    def __init__(
        self,
        options: LSFSchedulerOptions | None = None,
        *,
        transport: Transport | None = None,
    ) -> None:
        self._opts = options or LSFSchedulerOptions()
        self._transport: Transport = transport or LocalTransport()

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
            supports_partition=True,
            supports_account=True,
            supports_dependency=True,
        )

    def submit(self, spec: JobSpec, job_dir: Path) -> str:
        script_path = self._generate_script(spec, job_dir)
        cmd = [self._opts.bsub_path]
        cmd.extend(self._opts.extra_bsub_flags)

        # bsub reads the job script from stdin.  Read it back via the same
        # transport we just wrote it through so SSH-routed schedulers don't
        # try to read a remote-only file from the local filesystem.
        script_content = self._transport.read_text(str(script_path))
        try:
            result = self._transport.run(cmd, input=script_content, timeout=60)
        except TransportError as e:
            raise SchedulerError("LSF submission timed out", command=cmd) from e
        if result.returncode != 0:
            raise SchedulerError(
                "LSF submission failed",
                stderr=result.stderr,
                command=cmd,
            )
        match = re.search(r"Job <(\d+)>", result.stdout)
        if not match:
            raise SchedulerError(
                f"Could not parse job ID from bsub output: {result.stdout}",
                command=cmd,
            )
        return match.group(1)

    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]:
        if not scheduler_job_ids:
            return {}

        cmd = [self._opts.bjobs_path, "-noheader"] + list(scheduler_job_ids)

        try:
            result = self._transport.run(cmd, timeout=30)
        except TransportError as e:
            logger.warning(f"bjobs invocation failed: {e}")
            return {}
        if not result.stdout.strip():
            return {}

        wanted = set(scheduler_job_ids)
        out: dict[str, JobState] = {}
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) < 3:
                continue
            jid = parts[0]
            if jid in wanted:
                st = parts[2]
                state = _LSF_STATE_MAP.get(st)
                if state is not None:
                    out[jid] = state
        return out

    def cancel(self, scheduler_job_id: str) -> None:
        try:
            self._transport.run(
                [self._opts.bkill_path, scheduler_job_id],
                timeout=30,
            )
        except TransportError:
            pass

    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None:
        try:
            result = self._transport.run(
                [self._opts.bhist_path, "-l", scheduler_job_id],
                timeout=15,
            )
        except TransportError:
            return None
        if result.returncode != 0 or not result.stdout.strip():
            return None

        lower = result.stdout.lower()
        if "done" in lower:
            return TerminalStatus(state=JobState.SUCCEEDED, raw_state="done")
        match = re.search(r"exit code\s+(\d+)", lower)
        if "exit" in lower:
            code = int(match.group(1)) if match else None
            return TerminalStatus(
                state=JobState.FAILED,
                exit_code=code,
                failure_reason=_default_failure_reason(JobState.FAILED, code, "exit"),
                raw_state="exit",
            )
        return None

    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        target_user = user or os.environ.get("USER") or ""
        cmd: list[str] = [
            self._opts.bjobs_path,
            "-noheader",
            "-o",
            "jobid stat job_name user queue submit_time start_time",
        ]
        if target_user:
            cmd += ["-u", target_user]
        try:
            result = self._transport.run(cmd, timeout=30)
        except TransportError as exc:
            logger.warning(f"bjobs invocation failed: {exc}")
            return []
        if result.returncode != 0 or not result.stdout.strip():
            return []
        entries: list[QueueEntry] = []
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) < 5:
                continue
            jid = parts[0]
            raw_state = parts[1]
            name = parts[2]
            usr = parts[3]
            partition = parts[4]
            sub_t = " ".join(parts[5:8]) if len(parts) >= 8 else ""
            start_t = " ".join(parts[8:11]) if len(parts) >= 11 else ""
            entries.append(
                QueueEntry(
                    scheduler_job_id=jid,
                    name=None if name in ("-", "") else name,
                    user=usr or None,
                    state=_LSF_STATE_MAP.get(raw_state, JobState.QUEUED),
                    raw_state=raw_state,
                    partition=partition or None,
                    submit_time=_parse_lsf_time(sub_t),
                    start_time=_parse_lsf_time(start_t),
                )
            )
        return entries

    def _generate_script(self, spec: JobSpec, job_dir: Path) -> Path:
        script_path = job_dir / "run_lsf.sh"
        lines = ["#!/bin/bash"]

        directives = self._map_resources(spec)
        for key, value in directives.items():
            lines.append(f"#BSUB {key} {value}")

        lines.extend(_render_job_lines(spec, job_dir))

        self._transport.write_text(
            str(script_path), "\n".join(lines) + "\n", mode=0o700
        )
        return script_path

    def _map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped: dict[str, str] = {}
        r, s, e = spec.resources, spec.scheduling, spec.execution

        if s.partition:
            mapped["-q"] = s.partition
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
    *,
    transport: Transport | None = None,
) -> LocalScheduler | ShellScheduler | SlurmScheduler | PBSScheduler | LSFScheduler:
    """Create a Scheduler implementation by name.

    The optional *transport* parameter routes shell/file ops through the given
    :class:`~molq.transport.Transport`.  Defaults preserve pre-transport
    behaviour: ``"local"`` ignores *transport* and uses the legacy
    in-process :class:`LocalScheduler` (Popen + reaper); the new
    transport-aware ``"shell"`` returns :class:`ShellScheduler` which works
    over both :class:`~molq.transport.LocalTransport` and
    :class:`~molq.transport.SshTransport`.
    """
    if scheduler_name == "local":
        # LocalScheduler keeps the original in-process Popen path; transport
        # is intentionally ignored (it has no remote semantics by design).
        return LocalScheduler(
            options if isinstance(options, LocalSchedulerOptions) else None
        )
    if scheduler_name == "shell":
        return ShellScheduler(
            options if isinstance(options, LocalSchedulerOptions) else None,
            transport=transport,
        )
    if scheduler_name == "slurm":
        return SlurmScheduler(
            options if isinstance(options, SlurmSchedulerOptions) else None,
            transport=transport,
        )
    if scheduler_name == "pbs":
        return PBSScheduler(
            options if isinstance(options, PBSSchedulerOptions) else None,
            transport=transport,
        )
    if scheduler_name == "lsf":
        return LSFScheduler(
            options if isinstance(options, LSFSchedulerOptions) else None,
            transport=transport,
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
            return list((cmd.script.text or "").splitlines())
        if cmd.script.variant == "path":
            return [f'bash "{job_dir / "user_script.sh"}"']
    return []


def _parse_exit_code(field: str) -> int | None:
    try:
        return int(field.split(":")[0])
    except (ValueError, IndexError):
        return None


_SLURM_TIME_FORMATS: tuple[str, ...] = ("%Y-%m-%dT%H:%M:%S",)
_LSF_TIME_FORMATS: tuple[str, ...] = (
    "%Y-%m-%dT%H:%M:%S",
    "%b %d %H:%M %Y",
    "%b %d %H:%M",
)
_QUEUE_TIME_SENTINELS = frozenset({"", "-", "N/A", "Unknown"})


def _parse_queue_time(field: str, formats: tuple[str, ...]) -> float | None:
    """Parse a scheduler timestamp; return None for sentinel values.

    Year-less LSF timestamps are anchored to the current year so they match
    the scheduler client's display behavior.
    """
    field = field.strip()
    if field in _QUEUE_TIME_SENTINELS:
        return None
    for fmt in formats:
        try:
            ts = datetime.strptime(field, fmt)
        except ValueError:
            continue
        if ts.year == 1900:
            ts = ts.replace(year=datetime.now().year)
        return ts.timestamp()
    return None


def _parse_slurm_time(field: str) -> float | None:
    return _parse_queue_time(field, _SLURM_TIME_FORMATS)


def _parse_lsf_time(field: str) -> float | None:
    return _parse_queue_time(field, _LSF_TIME_FORMATS)


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
