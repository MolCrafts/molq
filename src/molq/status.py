"""Job state enum for molq."""

from enum import Enum


class JobState(str, Enum):
    """Terminal-aware job state."""

    CREATED = "created"
    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    LOST = "lost"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATES


_TERMINAL_STATES = frozenset(
    {
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.CANCELLED,
        JobState.TIMED_OUT,
        JobState.LOST,
    }
)
