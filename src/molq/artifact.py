"""Explicit artifact delivery over the Python SDK's file operations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from stat import S_ISDIR, S_ISREG
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from molq.client.path import Path

__all__ = ["Artifact", "ArtifactDelivery", "CopyArtifact", "SharedPathArtifact"]


@dataclass(frozen=True)
class ArtifactDelivery:
    """The delivered location and optional raw transfer response."""

    location: "Path"
    transfer_result: dict | None = None


class Artifact(ABC):
    """A file or directory that can be delivered independently of a job."""

    @abstractmethod
    def deliver(self) -> ArtifactDelivery:
        """Deliver this artifact or propagate the underlying error."""


@dataclass(frozen=True)
class CopyArtifact(Artifact):
    """Copy one source using Path.transfer and caller-supplied rsync options.

    For a file, supply a destination filename. For a directory, supply the
    path that should receive its contents and an option such as `-a`.
    """

    source: "Path"
    destination: "Path"
    options: tuple[str, ...] = ()
    timeout: int = 90
    directory: bool | None = None

    def deliver(self) -> ArtifactDelivery:
        """Run one transfer and return the destination and transfer result."""
        result = self.source.transfer(
            self.destination,
            *self.options,
            timeout=self.timeout,
            directory=self.directory,
        )
        return ArtifactDelivery(self.destination, result)


@dataclass(frozen=True)
class SharedPathArtifact(Artifact):
    """Deliver a reference to an existing file or directory.

    Bind a cluster-qualified Path to check it through that cluster's RPC.
    An unqualified Path is checked on the Python client's filesystem.
    """

    path: "Path"

    def deliver(self) -> ArtifactDelivery:
        """Check the path and return it without copying."""
        mode = self.path.stat().st_mode
        if not (S_ISREG(mode) or S_ISDIR(mode)):
            raise ValueError(f"Expected a file or directory: {self.path}")
        return ArtifactDelivery(self.path)
