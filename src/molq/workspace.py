"""Workspace and Project — remote directory handles tied to a Cluster.

A :class:`Workspace` is a base directory on the cluster's filesystem; a
:class:`Project` is a sub-namespace under a workspace.  Both expose a small
file-ops surface that goes through the cluster's :class:`~molq.transport.Transport`,
so the same code works against a local filesystem or a remote cluster over
SSH.

Project.submit_job is sugar that overrides :class:`~molq.types.JobExecution`
``cwd`` to the project's path and forwards to ``submitor.submit_job(...)``.

Decisions:
  - ``path`` is supplied explicitly; we do not silently SSH to query ``$HOME``.
  - There is no auto-staging of files referenced in argv — callers run
    :meth:`upload` explicitly.
"""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from molq.cluster import Cluster
    from molq.submitor import JobHandle, Submitor


class _RemoteDir:
    """Mixin: file-ops backed by ``self.cluster.transport`` rooted at ``self.path``.

    Subclasses provide ``cluster`` and ``path`` (as a field or property).
    """

    cluster: Cluster
    path: str

    def upload(self, local: str, *, recursive: bool = False) -> None:
        """Copy ``local`` into ``self.path`` on the cluster's filesystem."""
        self.cluster.transport.upload(local, self.path, recursive=recursive)

    def download(
        self,
        remote_rel: str,
        local: str,
        *,
        recursive: bool = False,
    ) -> None:
        remote = f"{self.path.rstrip('/')}/{remote_rel.lstrip('/')}"
        self.cluster.transport.download(remote, local, recursive=recursive)

    def exists(self) -> bool:
        return self.cluster.transport.exists(self.path)

    def ensure(self) -> None:
        """Create the directory if it doesn't exist (mkdir -p)."""
        self.cluster.transport.mkdir(self.path, parents=True, exist_ok=True)

    def mirror(
        self,
        local_dir: str | Path,
        *,
        exclude: Sequence[str] = (),
    ) -> Path:
        """Pull the entire remote directory to *local_dir* (mirror).

        Equivalent to ``rsync -a <transport>:<path>/ <local_dir>/``.  Creates
        *local_dir* if needed.  Subsequent calls perform an incremental
        sync — only changed files travel.

        Returns the local destination path for convenience.
        """
        target = Path(local_dir).expanduser()
        target.mkdir(parents=True, exist_ok=True)
        self.cluster.transport.download(
            self.path,
            str(target),
            recursive=True,
            exclude=tuple(exclude),
        )
        return target

    def list_files(self, rel: str = "") -> list[str]:
        """Return a flat listing of paths under the remote directory.

        Uses ``ls -1A`` on the transport — works for both local and SSH.
        ``rel`` is a path relative to ``self.path`` (use ``""`` for the root).
        """
        target = self.path.rstrip("/")
        if rel:
            target = f"{target}/{rel.lstrip('/')}"
        result = self.cluster.transport.run(
            ["sh", "-c", f"ls -1A {shlex.quote(target)}"]
        )
        if result.returncode != 0:
            return []
        return [line for line in result.stdout.splitlines() if line]

    def read_text(self, rel: str) -> str:
        """Read a remote file relative to this dir as text."""
        remote = f"{self.path.rstrip('/')}/{rel.lstrip('/')}"
        return self.cluster.transport.read_text(remote)

    def tail(self, rel: str, *, lines: int = 80) -> str:
        """Cheap remote tail — runs ``tail -n <lines>`` over the transport.

        Useful for log peeking without dragging the whole file across.
        Returns ``""`` when the file does not exist yet.
        """
        remote = f"{self.path.rstrip('/')}/{rel.lstrip('/')}"
        result = self.cluster.transport.run(
            [
                "sh",
                "-c",
                f"tail -n {int(lines)} {shlex.quote(remote)} 2>/dev/null || true",
            ]
        )
        return result.stdout


@dataclass
class Workspace(_RemoteDir):
    """A base directory on the cluster's filesystem."""

    cluster: Cluster
    name: str
    path: str

    def get_project(self, name: str) -> Project:
        return Project(workspace=self, name=name)


@dataclass
class Project(_RemoteDir):
    """A sub-namespace under a :class:`Workspace`."""

    workspace: Workspace
    name: str

    @property
    def cluster(self) -> Cluster:  # type: ignore[override]
        return self.workspace.cluster

    @property
    def path(self) -> str:  # type: ignore[override]
        return f"{self.workspace.path.rstrip('/')}/{self.name}"

    def submit_job(
        self,
        submitor: Submitor,
        **submit_kwargs: Any,
    ) -> JobHandle:
        """Submit a job whose working directory is this project's path.

        Forwards to :meth:`~molq.submitor.Submitor.submit_job`, overriding
        ``execution.cwd`` to ``self.path``.  Other ``execution`` fields the
        caller passes pass through unchanged.
        """
        from molq.types import JobExecution

        if submitor.target is not self.cluster:
            raise ValueError(
                f"Project belongs to cluster {self.cluster.name!r} but submitor "
                f"is bound to {submitor.target.name!r}"
            )
        execution = submit_kwargs.pop("execution", None)
        if execution is None:
            execution = JobExecution(cwd=self.path)
        else:
            execution = replace(execution, cwd=self.path)
        return submitor.submit_job(execution=execution, **submit_kwargs)


__all__ = ["Workspace", "Project"]
