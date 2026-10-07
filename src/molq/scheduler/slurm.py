"""SLURM backend (sbatch / squeue / scancel / sacct)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from molq._log import get_logger
from molq.errors import SchedulerError
from molq.models import JobSpec
from molq.options import SlurmSchedulerOptions
from molq.scheduler.base import (
    DependencyEdge,
    QueueEntry,
    SchedulerCapabilities,
    TerminalStatus,
    _dependency_keyword,
)
from molq.scheduler.script import (
    _default_failure_reason,
    _parse_exit_code,
    _parse_slurm_time,
    _render_job_lines,
)
from molq.status import JobState
from molq.transport import LocalTransport, Transport, TransportError

logger = get_logger(__name__)

_SLURM_STATE_MAP: dict[str, JobState] = {
    "R": JobState.RUNNING,
    "PD": JobState.QUEUED,
    "CF": JobState.QUEUED,
    "RQ": JobState.QUEUED,
    "RD": JobState.QUEUED,
    "RH": JobState.QUEUED,
    "RF": JobState.QUEUED,
    "RS": JobState.QUEUED,
    "S": JobState.QUEUED,
    "ST": JobState.QUEUED,
    "SI": JobState.RUNNING,
    "SO": JobState.RUNNING,
    "CG": JobState.RUNNING,
    "CD": JobState.SUCCEEDED,
    "CA": JobState.CANCELLED,
    "F": JobState.FAILED,
    "TO": JobState.TIMED_OUT,
    "NF": JobState.FAILED,
    "OOM": JobState.FAILED,
    "PR": JobState.CANCELLED,
    "DL": JobState.FAILED,
    "SE": JobState.FAILED,
    "RV": JobState.CANCELLED,
    "BF": JobState.FAILED,
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

        wanted = set(scheduler_job_ids)
        out: dict[str, JobState] = {}
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) < 2:
                continue
            jid, st = parts[0], parts[1]
            # `%i` is the job id; ignore `.batch` / array suffixes if present.
            bare = jid.split(".", 1)[0]
            key = jid if jid in wanted else bare if bare in wanted else None
            if key is None:
                continue
            out[key] = _SLURM_STATE_MAP.get(st, JobState.QUEUED)
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

    # --dependency=<keyword>:<jobid>[,<keyword>:<jobid2>...]
    _DEP_KEYWORDS: dict[str, str] = {
        "after_started": "after",
        "after_success": "afterok",
        "after_failure": "afternotok",
        "after": "afterany",
    }

    def format_dependency(self, edge: DependencyEdge) -> str:
        keyword = _dependency_keyword(self._DEP_KEYWORDS, edge.condition, "slurm")
        return f"{keyword}:{edge.scheduler_job_id}"

    def format_dependencies(self, edges: Sequence[DependencyEdge]) -> str:
        return ",".join(self.format_dependency(edge) for edge in edges)

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
