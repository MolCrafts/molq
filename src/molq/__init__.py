"""Public API for the molq package.

Usage::

    from molq import Submitor, JobResources, Memory

    local = Submitor("devbox", "local")

    job = local.submit(
        argv=["python", "train.py"],
        resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
    )
    print(job.status())
    record = job.wait()
    print(record.state)
"""

from molq.dashboard import DashboardState, JobRow, MolqMonitor, RunDashboard
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
from molq.models import JobRecord, SubmitorDefaults
from molq.options import (
    LocalSchedulerOptions,
    LSFSchedulerOptions,
    PBSSchedulerOptions,
    SlurmSchedulerOptions,
)
from molq.status import JobState
from molq.submitor import JobHandle, Submitor
from molq.types import (
    Duration,
    JobExecution,
    JobResources,
    JobScheduling,
    Memory,
    Script,
)

__all__ = [
    # Dashboard
    "RunDashboard",
    "MolqMonitor",
    "DashboardState",
    "JobRow",
    # Core
    "Submitor",
    "JobHandle",
    # Types
    "Memory",
    "Duration",
    "Script",
    "JobResources",
    "JobScheduling",
    "JobExecution",
    # Models
    "SubmitorDefaults",
    "JobRecord",
    "JobState",
    # Options
    "LocalSchedulerOptions",
    "SlurmSchedulerOptions",
    "PBSSchedulerOptions",
    "LSFSchedulerOptions",
    # Errors
    "MolqError",
    "ConfigError",
    "SubmitError",
    "CommandError",
    "ScriptError",
    "SchedulerError",
    "JobNotFoundError",
    "MolqTimeoutError",
    "StoreError",
]
