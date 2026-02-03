"""Job status representation for molq."""

import enum


class JobStatus:
    """Lightweight representation of a job's state."""

    class Status(enum.Enum):
        PENDING = 1
        RUNNING = 2
        COMPLETED = 3
        FAILED = 4
        FINISHED = 5

    def __init__(self, job_id: int, status: Status, name: str = "", **others: str):
        """Create a :class:`JobStatus` instance."""
        self.name: str = name
        self.job_id: int = job_id
        self.status: JobStatus.Status = status
        self.others: dict[str, str] = others

    def __repr__(self):
        return f"<Job {self.name}({self.job_id}): {self.status}>"

    @property
    def is_finish(self) -> bool:
        return self.status in [
            JobStatus.Status.COMPLETED,
            JobStatus.Status.FAILED,
            JobStatus.Status.FINISHED,
        ]
