"""molq 0.9: peer RPC clients with an immutable OOP Python façade."""

from __future__ import annotations

from molq.artifact import Artifact, ArtifactDelivery, CopyArtifact, SharedPathArtifact
from molq.domain import (
    Accelerator,
    Execution,
    ExecutionUnit,
    JobArray,
    JobSpec,
    Launch,
    Parallel,
    Resources,
    Scheduling,
    Sequence,
    UnitRef,
)
from molq.environment import (
    CondaEnvironment,
    DockerEnvironment,
    DockerMount,
    Environment,
    UvEnvironment,
)
from molq.errors import MolqError

__version__ = "0.9.0"
__all__ = [
    "submit",
    "asubmit",
    "Molq",
    "AsyncMolq",
    "Cluster",
    "Job",
    "JobCollection",
    "Path",
    "Workspace",
    "MolqError",
    "Accelerator",
    "Resources",
    "Scheduling",
    "Launch",
    "Execution",
    "ExecutionUnit",
    "UnitRef",
    "Sequence",
    "Parallel",
    "JobArray",
    "JobSpec",
    "Environment",
    "CondaEnvironment",
    "UvEnvironment",
    "DockerEnvironment",
    "DockerMount",
    "ArtifactManifest",
    "Catalog",
    "Glob",
    "OutputPath",
    "ResolvedArtifacts",
    "ManifestDelivery",
    "Artifact",
    "ArtifactDelivery",
    "CopyArtifact",
    "SharedPathArtifact",
]


def __getattr__(name: str):
    """Load client façades without importing Runtime or its dependencies."""
    if name in {"submit", "Molq", "Cluster", "Job", "JobCollection"}:
        from molq.client import objects

        return getattr(objects, name)
    if name in {
        "ArtifactManifest",
        "Catalog",
        "Glob",
        "OutputPath",
        "ResolvedArtifacts",
        "ManifestDelivery",
    }:
        from molq import artifact_manifest

        return getattr(artifact_manifest, name)
    if name == "Workspace":
        from molq.workspace import Workspace

        return Workspace
    if name == "Path":
        from molq.client.path import Path

        return Path
    if name == "AsyncMolq":
        from molq.client.async_objects import AsyncMolq

        return AsyncMolq
    if name == "asubmit":
        from molq.client.async_objects import asubmit

        return asubmit
    raise AttributeError(name)
