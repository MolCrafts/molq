"""Scheduler-specific option types for molq.

Each scheduler defines its own frozen dataclass. No dict[str, Any] allowed.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalSchedulerOptions:
    """Options for local (subprocess) scheduler."""

    runner_shim: str | Path = "molq-runner"
    max_concurrent: int | None = None


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
