"""Data models for molq.

Public: JobRecord, SubmitorDefaults
Internal: Command, JobSpec
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path

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


# ---------------------------------------------------------------------------
# SubmitorDefaults (public)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SubmitorDefaults:
    """Default resource, scheduling, and execution parameters for a Submitor."""

    resources: JobResources | None = None
    scheduling: JobScheduling | None = None
    execution: JobExecution | None = None
