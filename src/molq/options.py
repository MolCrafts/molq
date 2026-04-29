"""Scheduler-specific option types for molq.

Each scheduler defines its own frozen dataclass. No dict[str, Any] allowed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LocalSchedulerOptions:
    """Options for local (subprocess) scheduler."""


@dataclass(frozen=True)
class SlurmSchedulerOptions:
    """Options for SLURM scheduler."""

    sbatch_path: str = "sbatch"
    squeue_path: str = "squeue"
    scancel_path: str = "scancel"
    sacct_path: str = "sacct"
    extra_sbatch_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class PBSSchedulerOptions:
    """Options for PBS/Torque scheduler."""

    qsub_path: str = "qsub"
    qstat_path: str = "qstat"
    qdel_path: str = "qdel"
    tracejob_path: str = "tracejob"
    extra_qsub_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class LSFSchedulerOptions:
    """Options for IBM Spectrum LSF scheduler."""

    bsub_path: str = "bsub"
    bjobs_path: str = "bjobs"
    bkill_path: str = "bkill"
    bhist_path: str = "bhist"
    extra_bsub_flags: tuple[str, ...] = ()


SchedulerOptions = (
    LocalSchedulerOptions
    | SlurmSchedulerOptions
    | PBSSchedulerOptions
    | LSFSchedulerOptions
)

OPTIONS_TYPE_MAP: dict[str, type[SchedulerOptions]] = {
    "local": LocalSchedulerOptions,
    "slurm": SlurmSchedulerOptions,
    "pbs": PBSSchedulerOptions,
    "lsf": LSFSchedulerOptions,
}


# ---------------------------------------------------------------------------
# Transport options (orthogonal to scheduler options — see molq.transport).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SshTransportOptions:
    """Options for :class:`molq.transport.SshTransport`.

    ``host`` is the only required field — everything else falls back to the
    user's ``~/.ssh/config``.  ``rsync_opts`` defaults preserve partial
    transfers across flaky links and avoid extra renames on busy schedulers.
    """

    host: str                                       # "user@host" or alias from ssh_config
    port: int | None = None
    identity_file: str | None = None
    ssh_opts: tuple[str, ...] = ()                  # extra "-o" pairs / flags forwarded to ssh
    rsync_opts: tuple[str, ...] = ("-a", "--partial", "--inplace")
