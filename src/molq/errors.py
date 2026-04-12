"""Unified exception hierarchy for molq."""

from __future__ import annotations


class MolqError(Exception):
    """Base exception for all molq errors."""

    def __init__(self, message: str, **context: object) -> None:
        self.context = context
        super().__init__(message)


class ConfigError(MolqError):
    """Submitor initialization parameter error."""


class SubmitError(MolqError):
    """Job submission failed."""


class CommandError(SubmitError):
    """Command validation failed (argv/command/script exclusion, newline in command)."""


class ScriptError(SubmitError):
    """Script materialization failed (file not found, copy failure)."""


class SchedulerError(MolqError):
    """Scheduler communication failed."""

    def __init__(
        self,
        message: str,
        *,
        stderr: str | None = None,
        command: list[str] | None = None,
        **context: object,
    ) -> None:
        self.stderr = stderr
        self.command = command
        super().__init__(message, **context)


class JobNotFoundError(MolqError):
    """Requested job_id does not exist."""

    def __init__(
        self,
        job_id: str,
        cluster_name: str | None = None,
    ) -> None:
        self.job_id = job_id
        self.cluster_name = cluster_name
        msg = f"Job {job_id!r} not found"
        if cluster_name:
            msg += f" in cluster {cluster_name!r}"
        super().__init__(msg, job_id=job_id, cluster_name=cluster_name)


class MolqTimeoutError(TimeoutError, MolqError):
    """Watch/wait timeout exceeded.

    Inherits from both builtins.TimeoutError and MolqError so that
    ``except TimeoutError`` and ``except MolqError`` both catch it.
    """

    def __init__(self, message: str, **context: object) -> None:
        # TimeoutError.__init__ only takes *args
        TimeoutError.__init__(self, message)
        self.context = context


class StoreError(MolqError):
    """Database operation failed."""
