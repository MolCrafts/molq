"""Tests for molq.cluster — Cluster class."""

from unittest.mock import MagicMock

import pytest

from molq.cluster import Cluster
from molq.errors import ConfigError
from molq.options import SshTransportOptions
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

    def test_repr_is_informative(self):
        cluster = Cluster("dev", "local")
        r = repr(cluster)
        assert "Cluster" in r and "dev" in r and "local" in r


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
