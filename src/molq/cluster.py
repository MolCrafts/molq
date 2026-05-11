"""Cluster — destination spec for molq job submissions.

A :class:`Cluster` describes *where* jobs run: the scheduler kind (local /
slurm / pbs / lsf), the Transport (local subprocess vs SSH), and any
scheduler-specific options.  It holds no per-job state — that lives on
:class:`~molq.submitor.Submitor`, which is bound to a Cluster as its
``target``.

Cluster + Submitor are orthogonal:
  - **Cluster** owns destination (transport, scheduler kind/options).
  - **Submitor** owns lifecycle (store, monitor, reconciler, event bus).

A typical session::

    cluster  = mq.Cluster("hpc", scheduler="slurm", host="user@hpc.example")
    submitor = mq.Submitor(target=cluster)

    handle = submitor.submit_job(argv=["python", "train.py"])
    snapshot = cluster.get_queue()
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from molq.errors import ConfigError
from molq.options import (
    OPTIONS_TYPE_MAP,
    SchedulerOptions,
    SshTransportOptions,
)
from molq.scheduler import QueueEntry, create_scheduler
from molq.transport import LocalTransport, SshTransport, Transport

if TYPE_CHECKING:
    from molq.workspace import Project, Workspace


class Cluster:
    """A submission destination — scheduler + transport bound together.

    Args:
        name: Cluster label used to scope persisted records.
        scheduler: One of ``"local"``, ``"slurm"``, ``"pbs"``, ``"lsf"``.
            ``"local"`` is the no-batch-system backend; pair it with a
            non-default ``transport`` (or pass ``host=`` for SSH) to run jobs
            on a remote workstation without a queue manager.
        host: SSH shortcut.  If provided, builds an :class:`~molq.transport.SshTransport`
            from :class:`~molq.options.SshTransportOptions(host=host)`.  Mutually
            exclusive with ``transport``.
        transport: Explicit Transport.  Mutually exclusive with ``host``.
        scheduler_options: Backend-specific options dataclass.  When omitted,
            defaults are used.
    """

    def __init__(
        self,
        name: str,
        scheduler: str = "local",
        *,
        host: str | None = None,
        transport: Transport | None = None,
        scheduler_options: SchedulerOptions | None = None,
        _scheduler_impl: Any | None = None,
    ) -> None:
        if host is not None and transport is not None:
            raise ConfigError(
                "Cluster: pass either host=... or transport=..., not both",
                cluster=name,
            )
        if scheduler not in OPTIONS_TYPE_MAP:
            raise ConfigError(
                f"Unknown scheduler {scheduler!r}. "
                f"Must be one of: {', '.join(OPTIONS_TYPE_MAP)}",
                scheduler=scheduler,
            )
        if scheduler_options is not None:
            expected = OPTIONS_TYPE_MAP[scheduler]
            if not isinstance(scheduler_options, expected):
                raise TypeError(
                    f"scheduler_options must be {expected.__name__} "
                    f"for scheduler {scheduler!r}, "
                    f"got {type(scheduler_options).__name__}"
                )

        if transport is not None:
            self._transport: Transport = transport
        elif host is not None:
            self._transport = SshTransport(SshTransportOptions(host=host))
        else:
            self._transport = LocalTransport()

        self._name = name
        self._scheduler = scheduler
        self._scheduler_options = scheduler_options
        if _scheduler_impl is not None:
            self._scheduler_impl = _scheduler_impl
        else:
            self._scheduler_impl = create_scheduler(
                scheduler,
                scheduler_options,
                transport=self._transport,
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def scheduler(self) -> str:
        return self._scheduler

    @property
    def transport(self) -> Transport:
        return self._transport

    @property
    def scheduler_impl(self) -> Any:
        return self._scheduler_impl

    @property
    def scheduler_options(self) -> SchedulerOptions | None:
        return self._scheduler_options

    # ------------------------------------------------------------------
    # Introspection / file ops
    # ------------------------------------------------------------------

    def get_queue(self, *, user: str | None = None) -> list[QueueEntry]:
        """Return the scheduler's current queue snapshot.

        Backed by ``squeue --me`` (SLURM), ``qstat -u $USER`` (PBS), or
        ``bjobs`` (LSF).  Local-style schedulers return an empty list.
        """
        return self._scheduler_impl.list_queue(user=user)

    def get_workspace(self, name: str, *, path: str) -> Workspace:
        """Return a :class:`~molq.workspace.Workspace` rooted at *path*.

        ``path`` is the absolute path on the cluster's filesystem (i.e., on the
        Transport).  No remote I/O is performed by this call — invoke
        ``workspace.ensure()`` if you need to create the directory.
        """
        from molq.workspace import Workspace

        return Workspace(cluster=self, name=name, path=path)

    def get_project(self, name: str, *, workspace: Workspace) -> Project:
        """Return a :class:`~molq.workspace.Project` under *workspace*.

        ``workspace`` must be supplied explicitly — the cluster's filesystem
        layout is unknown to molq, and silently defaulting to the driver's
        ``cwd`` would point to the wrong place on a remote cluster.
        """
        from molq.workspace import Project

        return Project(workspace=workspace, name=name)

    # ------------------------------------------------------------------
    # Profile loading
    # ------------------------------------------------------------------

    @classmethod
    def from_profile(
        cls,
        profile_name: str,
        *,
        config_path: str | Path | None = None,
    ) -> Cluster:
        """Load destination half of a profile (scheduler, host, scheduler_options)."""
        from molq.config import load_profile

        profile = load_profile(profile_name, config_path)
        return cls(
            profile.cluster_name,
            profile.scheduler,
            scheduler_options=profile.scheduler_options,
        )

    @classmethod
    def from_ssh_alias(
        cls,
        alias: str,
        *,
        scheduler: str = "slurm",
        name: str | None = None,
        scheduler_options: SchedulerOptions | None = None,
        config_path: str | Path | None = None,
    ) -> Cluster:
        """Build a Cluster from a ``Host`` alias in ``~/.ssh/config``.

        ``alias`` is resolved through ``ssh -G`` so the resulting
        :class:`~molq.transport.SshTransport` carries the effective
        hostname/user/port/identityfile.  ``name`` defaults to *alias*; pass
        an explicit value when the cluster name should differ from the SSH
        alias (e.g., when persisting records under a stable label).
        """
        from molq.ssh_config import resolve_ssh_host

        host = resolve_ssh_host(alias)
        opts = SshTransportOptions(
            host=alias,
            port=host.port,
            identity_file=host.identity_file,
        )
        return cls(
            name or alias,
            scheduler,
            transport=SshTransport(opts),
            scheduler_options=scheduler_options,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Cluster(name={self._name!r}, scheduler={self._scheduler!r}, "
            f"transport={type(self._transport).__name__})"
        )


__all__ = ["Cluster"]
