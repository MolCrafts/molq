"""Tests for molq.cluster — Cluster class."""

from unittest.mock import MagicMock

import pytest

from molq.cluster import Cluster
from molq.errors import ConfigError
from molq.options import (
    LocalSchedulerOptions,
    SlurmSchedulerOptions,
    SshTransportOptions,
)
from molq.scheduler import ShellScheduler, SlurmScheduler
from molq.transport import LocalTransport, SshTransport


class TestClusterInit:
    def test_default_uses_local_transport(self):
        cluster = Cluster("dev", "local")
        assert cluster.name == "dev"
        assert cluster.scheduler == "local"
        assert isinstance(cluster.transport, LocalTransport)

    def test_host_shortcut_builds_ssh_transport(self):
        cluster = Cluster("hpc", "slurm", host="user@example.com")
        assert isinstance(cluster.transport, SshTransport)
        assert cluster.transport.options.host == "user@example.com"

    def test_explicit_transport_used_directly(self):
        ssh = SshTransport(SshTransportOptions(host="my-host"))
        cluster = Cluster("hpc", "slurm", transport=ssh)
        assert cluster.transport is ssh

    def test_host_and_transport_mutually_exclusive(self):
        ssh = SshTransport(SshTransportOptions(host="other"))
        with pytest.raises(ConfigError, match="host=.*or transport=.*not both"):
            Cluster("hpc", "slurm", host="user@host", transport=ssh)

    def test_unknown_scheduler_raises(self):
        with pytest.raises(ConfigError, match="Unknown scheduler"):
            Cluster("dev", "frobnitz")

    def test_shell_scheduler_kind_no_longer_accepted(self):
        # "shell" was the pre-unification alias for the transport-aware backend.
        # It's now spelled "local" — verify the old name is rejected so
        # callers updating from <0.4 get a clear error instead of a silent
        # behavior change.
        with pytest.raises(ConfigError, match="Unknown scheduler"):
            Cluster("dev", "shell")

    def test_repr_is_informative(self):
        cluster = Cluster("dev", "local")
        r = repr(cluster)
        assert "Cluster" in r and "dev" in r and "local" in r


class TestClusterSchedulerImpl:
    """The Cluster wires a Scheduler implementation to the chosen Transport.

    These tests pin down the post-unification contract: there is exactly one
    "no batch system" backend (``ShellScheduler``), and ``host=`` is honored
    for *every* scheduler kind — including ``"local"``, which used to ignore
    it silently.
    """

    def test_local_uses_shell_scheduler_with_local_transport(self):
        cluster = Cluster("dev", "local")
        assert isinstance(cluster.scheduler_impl, ShellScheduler)
        assert isinstance(cluster.transport, LocalTransport)
        # Scheduler holds the same Transport the Cluster exposes.
        assert cluster.scheduler_impl._transport is cluster.transport

    def test_local_with_host_uses_ssh_transport(self):
        cluster = Cluster("workstation", "local", host="user@workstation")
        assert isinstance(cluster.scheduler_impl, ShellScheduler)
        assert isinstance(cluster.transport, SshTransport)
        # The Scheduler must share the SSH Transport, not silently fall back
        # to LocalTransport — that was the LocalScheduler regression.
        assert cluster.scheduler_impl._transport is cluster.transport
        assert cluster.transport.options.host == "user@workstation"

    def test_local_with_explicit_transport_threads_through(self):
        ssh = SshTransport(SshTransportOptions(host="custom-host"))
        cluster = Cluster("workstation", "local", transport=ssh)
        assert cluster.scheduler_impl._transport is ssh

    def test_slurm_with_host_uses_ssh_transport(self):
        cluster = Cluster("hpc", "slurm", host="user@hpc")
        assert isinstance(cluster.scheduler_impl, SlurmScheduler)
        assert cluster.scheduler_impl._transport is cluster.transport

    def test_local_options_accepted(self):
        cluster = Cluster("dev", "local", scheduler_options=LocalSchedulerOptions())
        assert isinstance(cluster.scheduler_impl, ShellScheduler)

    def test_local_rejects_wrong_options_type(self):
        with pytest.raises(TypeError, match="LocalSchedulerOptions"):
            Cluster(
                "dev",
                "local",
                scheduler_options=SlurmSchedulerOptions(),  # type: ignore[arg-type]
            )


class TestClusterGetQueue:
    def test_local_returns_empty(self):
        assert Cluster("dev", "local").get_queue() == []

    def test_get_queue_delegates_to_scheduler_impl(self):
        impl = MagicMock()
        impl.list_queue.return_value = ["sentinel"]
        cluster = Cluster("dev", "local", _scheduler_impl=impl)
        result = cluster.get_queue(user="alice")
        assert result == ["sentinel"]
        impl.list_queue.assert_called_once_with(user="alice")


class TestClusterWorkspace:
    def test_get_workspace_returns_workspace_handle(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = cluster.get_workspace("scratch", path=str(tmp_path))
        assert ws.cluster is cluster
        assert ws.name == "scratch"
        assert ws.path == str(tmp_path)

    def test_get_project_requires_workspace(self):
        cluster = Cluster("dev", "local")
        with pytest.raises(TypeError, match="workspace"):
            cluster.get_project("alpha")  # type: ignore[call-arg]

    def test_get_project_uses_passed_workspace(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = cluster.get_workspace("ws", path=str(tmp_path))
        proj = cluster.get_project("alpha", workspace=ws)
        assert proj.workspace is ws
