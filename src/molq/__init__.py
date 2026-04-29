"""Public API for the molq package.

Usage::

    from molq import Cluster, Submitor, JobResources, Memory

    # Destination — pure config, no lifecycle state.
    cluster  = Cluster("devbox", "local")

    # Lifecycle engine — bound to a target Cluster.
    submitor = Submitor(target=cluster)

    handle = submitor.submit_job(
        argv=["python", "train.py"],
        resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
    )
    print(handle.status())
    record = handle.wait()
    print(record.state)

    # Scheduler-side queue snapshot:
    cluster.get_queue()  # squeue --me / qstat -u $USER / bjobs (or [] for local)
"""

from molq.callbacks import EventBus, EventPayload, EventType
from molq.cluster import Cluster
from molq.config import MolqConfig, MolqProfile, load_config, load_profile
from molq.workspace import Project, Workspace
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
from molq.models import (
    DependencyPreview,
    DependencyPreviewItem,
    JobDependency,
    JobRecord,
    RetentionPolicy,
    RetryBackoff,
    RetryPolicy,
    StatusTransition,
    SubmitorDefaults,
)
from molq.options import (
    LocalSchedulerOptions,
    LSFSchedulerOptions,
    PBSSchedulerOptions,
    SlurmSchedulerOptions,
)
from molq.scheduler import QueueEntry, SchedulerCapabilities
from molq.status import JobState
from molq.store import dependency_relation_state
from molq.submitor import JobHandle, Submitor
from molq.types import (
    DependencyCondition,
    DependencyRef,
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
    "EventBus",
    "EventPayload",
    "EventType",
    # Core
    "Cluster",
    "Submitor",
    "JobHandle",
    "QueueEntry",
    "Workspace",
    "Project",
    # Types
    "Memory",
    "Duration",
    "Script",
    "DependencyCondition",
    "DependencyRef",
    "JobResources",
    "JobScheduling",
    "JobExecution",
    # Dependency helpers
    "dependency_relation_state",
    # Models
    "SubmitorDefaults",
    "JobRecord",
    "JobDependency",
    "DependencyPreview",
    "DependencyPreviewItem",
    "StatusTransition",
    "RetryBackoff",
    "RetryPolicy",
    "RetentionPolicy",
    "JobState",
    "SchedulerCapabilities",
    # Config
    "MolqConfig",
    "MolqProfile",
    "load_config",
    "load_profile",
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
