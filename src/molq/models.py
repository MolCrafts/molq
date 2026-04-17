"""Data models for molq.

Public: JobRecord, StatusTransition, SubmitorDefaults, RetryPolicy,
RetentionPolicy, JobDependency, DependencyPreview
Internal: Command, JobSpec
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from molq.errors import CommandError
from molq.status import JobState
from molq.types import JobExecution, JobResources, JobScheduling, Script

# ---------------------------------------------------------------------------
# Command (internal)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Command:
    """Three-way exclusive command representation."""

    argv: tuple[str, ...] | None = None
    command: str | None = None
    script: Script | None = None

    @classmethod
    def from_submit_args(
        cls,
        *,
        argv: list[str] | None = None,
        command: str | None = None,
        script: Script | None = None,
    ) -> Command:
        """Validate and create a Command from submit arguments."""
        provided = sum(x is not None for x in (argv, command, script))
        if provided == 0:
            raise CommandError(
                "Exactly one of argv, command, or script must be provided"
            )
        if provided > 1:
            raise CommandError(
                "Exactly one of argv, command, or script must be provided"
            )

        if command is not None and "\n" in command:
            raise CommandError(
                "command must not contain newlines; use Script.inline() for multi-line"
            )

        return cls(
            argv=tuple(argv) if argv is not None else None,
            command=command,
            script=script,
        )

    @property
    def command_type(self) -> str:
        if self.argv is not None:
            return "argv"
        if self.command is not None:
            return "command"
        return "script"

    @property
    def display(self) -> str:
        if self.argv is not None:
            return " ".join(self.argv)
        if self.command is not None:
            return self.command
        if self.script is not None:
            if self.script.variant == "path" and self.script.file_path:
                return f"script:{self.script.file_path}"
            return "script:inline"
        return ""


# ---------------------------------------------------------------------------
# JobSpec (internal)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryBackoff:
    """Retry delay policy."""

    mode: Literal["fixed", "exponential"] = "exponential"
    initial_seconds: float = 5.0
    maximum_seconds: float = 300.0
    factor: float = 2.0


@dataclass(frozen=True)
class RetryPolicy:
    """Retry policy applied by molq after terminal failures."""

    max_attempts: int = 1
    retry_on_states: tuple[JobState, ...] = (JobState.FAILED, JobState.TIMED_OUT)
    retry_on_exit_codes: tuple[int, ...] | None = None
    backoff: RetryBackoff = field(default_factory=RetryBackoff)


@dataclass(frozen=True)
class RetentionPolicy:
    """Retention policy for job artifacts and terminal records."""

    keep_job_dirs_for_days: int = 30
    keep_terminal_records_for_days: int = 90
    keep_failed_job_dirs: bool = True


@dataclass(frozen=True)
class JobDependency:
    """Persisted dependency edge between two molq jobs."""

    job_id: str
    dependency_job_id: str
    dependency_type: str
    scheduler_dependency: str


@dataclass(frozen=True)
class DependencyPreviewItem:
    """Dependency relation enriched with related job state."""

    job_id: str
    dependency_type: str
    relation_state: str
    job_state: JobState
    command_display: str = ""
    scheduler_dependency: str | None = None


@dataclass(frozen=True)
class DependencyPreview:
    """Depth-1 dependency preview for a single job."""

    job_id: str
    upstream_total: int = 0
    upstream_satisfied: int = 0
    upstream: tuple[DependencyPreviewItem, ...] = ()
    downstream_total: int = 0
    downstream: tuple[DependencyPreviewItem, ...] = ()


@dataclass(frozen=True)
class JobSpec:
    """Internal canonical job specification. Not exported."""

    job_id: str
    cluster_name: str
    scheduler: str
    command: Command
    resources: JobResources = field(default_factory=JobResources)
    scheduling: JobScheduling = field(default_factory=JobScheduling)
    execution: JobExecution = field(default_factory=JobExecution)
    metadata: dict[str, str] = field(default_factory=dict)
    cwd: str = field(default_factory=lambda: str(Path.cwd()))
    root_job_id: str = ""
    attempt: int = 1
    previous_attempt_job_id: str | None = None
    retry_group_id: str | None = None
    request_json: str = "{}"
    profile_name: str | None = None
    dir_name: str | None = None

    @staticmethod
    def new_job_id() -> str:
        return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# JobRecord (public)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JobRecord:
    """Immutable snapshot of a job's full lifecycle state."""

    job_id: str
    cluster_name: str
    scheduler: str
    state: JobState
    scheduler_job_id: str | None = None
    submitted_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    failure_reason: str | None = None
    cwd: str = ""
    command_type: str = ""
    command_display: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    root_job_id: str = ""
    attempt: int = 1
    previous_attempt_job_id: str | None = None
    retry_group_id: str | None = None
    profile_name: str | None = None
    cleaned_at: float | None = None


@dataclass(frozen=True)
class StatusTransition:
    """Immutable persisted lifecycle transition for a job."""

    job_id: str
    old_state: JobState | None
    new_state: JobState
    timestamp: float
    reason: str | None = None


# ---------------------------------------------------------------------------
# SubmitorDefaults (public)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SubmitorDefaults:
    """Default resource, scheduling, and execution parameters for a Submitor."""

    resources: JobResources | None = None
    scheduling: JobScheduling | None = None
    execution: JobExecution | None = None
