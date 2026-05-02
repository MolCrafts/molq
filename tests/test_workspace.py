"""Tests for molq.workspace — Workspace and Project."""

from unittest.mock import MagicMock

import pytest

from molq.cluster import Cluster
from molq.testing import make_submitor
from molq.workspace import Project, Workspace


class TestWorkspace:
    def test_path_and_attrs(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = Workspace(cluster=cluster, name="scratch", path=str(tmp_path))
        assert ws.path == str(tmp_path)
        assert ws.cluster is cluster
        assert ws.name == "scratch"

    def test_ensure_creates_directory(self, tmp_path):
        cluster = Cluster("dev", "local")
        target = tmp_path / "new_ws"
        ws = Workspace(cluster=cluster, name="ws", path=str(target))
        assert not target.exists()
        ws.ensure()
        assert target.exists()

    def test_exists_uses_transport(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = Workspace(cluster=cluster, name="ws", path=str(tmp_path))
        assert ws.exists() is True
        ws_missing = Workspace(
            cluster=cluster,
            name="missing",
            path=str(tmp_path / "absent"),
        )
        assert ws_missing.exists() is False

    def test_get_project_links_back_to_workspace(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = Workspace(cluster=cluster, name="ws", path=str(tmp_path))
        proj = ws.get_project("alpha")
        assert proj.workspace is ws
        assert proj.name == "alpha"


class TestProject:
    def test_path_joins_workspace_and_name(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = Workspace(cluster=cluster, name="ws", path=str(tmp_path))
        proj = Project(workspace=ws, name="alpha")
        assert proj.path == f"{tmp_path}/alpha"

    def test_cluster_property(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = Workspace(cluster=cluster, name="ws", path=str(tmp_path))
        proj = Project(workspace=ws, name="alpha")
        assert proj.cluster is cluster

    def test_ensure_creates_project_dir(self, tmp_path):
        cluster = Cluster("dev", "local")
        ws = Workspace(cluster=cluster, name="ws", path=str(tmp_path))
        proj = Project(workspace=ws, name="alpha")
        assert not (tmp_path / "alpha").exists()
        proj.ensure()
        assert (tmp_path / "alpha").exists()

    def test_submit_job_overrides_cwd(self, tmp_path):
        with make_submitor("dev", outcomes="succeeded", job_duration=0.0) as sub:
            ws = Workspace(cluster=sub.target, name="ws", path=str(tmp_path))
            proj = Project(workspace=ws, name="alpha")
            proj.ensure()
            handle = proj.submit_job(sub, argv=["pwd"])
            record = handle.wait()
            assert record.cwd == proj.path

    def test_submit_job_rejects_mismatched_submitor(self, tmp_path):
        cluster_a = Cluster("a", "local")
        cluster_b = Cluster("b", "local")
        ws = Workspace(cluster=cluster_a, name="ws", path=str(tmp_path))
        proj = Project(workspace=ws, name="alpha")
        sub_b = MagicMock()
        sub_b.target = cluster_b
        with pytest.raises(ValueError, match="Project belongs to cluster"):
            proj.submit_job(sub_b, argv=["echo"])
