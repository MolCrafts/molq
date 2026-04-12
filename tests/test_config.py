"""Tests for molq config loading via molcfg."""

from __future__ import annotations

import pytest

from molq.config import MolqConfig, MolqProfile, load_config, load_profile
from molq.errors import ConfigError


def _write_toml(tmp_path, content: str):
    p = tmp_path / "config.toml"
    p.write_text(content)
    return p


class TestLoadConfig:
    def test_missing_file_returns_empty(self, tmp_path):
        path = tmp_path / "nonexistent.toml"
        cfg = load_config(path)
        assert cfg == MolqConfig(profiles={})

    def test_empty_file_returns_empty(self, tmp_path):
        path = _write_toml(tmp_path, "")
        cfg = load_config(path)
        assert cfg == MolqConfig(profiles={})

    def test_no_profiles_key_returns_empty(self, tmp_path):
        path = _write_toml(tmp_path, "[other]\nkey = 1\n")
        cfg = load_config(path)
        assert cfg == MolqConfig(profiles={})

    def test_minimal_profile(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.local]\nscheduler = "local"\ncluster_name = "dev"\n',
        )
        cfg = load_config(path)
        assert "local" in cfg.profiles
        p = cfg.profiles["local"]
        assert p.scheduler == "local"
        assert p.cluster_name == "dev"

    def test_multiple_profiles(self, tmp_path):
        toml = (
            '[profiles.a]\nscheduler = "local"\ncluster_name = "x"\n'
            '[profiles.b]\nscheduler = "slurm"\ncluster_name = "y"\n'
        )
        cfg = load_config(_write_toml(tmp_path, toml))
        assert set(cfg.profiles) == {"a", "b"}

    def test_profile_name_stored(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.myjob]\nscheduler = "local"\ncluster_name = "c"\n',
        )
        p = load_config(path).profiles["myjob"]
        assert p.name == "myjob"

    def test_jobs_dir_optional(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.x]\nscheduler = "local"\ncluster_name = "c"\njobs_dir = "/tmp/jobs"\n',
        )
        p = load_config(path).profiles["x"]
        assert p.jobs_dir == "/tmp/jobs"


class TestValidation:
    def test_missing_scheduler_raises(self, tmp_path):
        path = _write_toml(tmp_path, '[profiles.bad]\ncluster_name = "c"\n')
        with pytest.raises(ConfigError):
            load_config(path)

    def test_missing_cluster_name_raises(self, tmp_path):
        path = _write_toml(tmp_path, '[profiles.bad]\nscheduler = "local"\n')
        with pytest.raises(ConfigError):
            load_config(path)

    def test_unknown_scheduler_raises(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.bad]\nscheduler = "bogus"\ncluster_name = "c"\n',
        )
        with pytest.raises(ConfigError):
            load_config(path)

    @pytest.mark.parametrize("sched", ["local", "slurm", "pbs", "lsf"])
    def test_all_known_schedulers_accepted(self, tmp_path, sched):
        path = _write_toml(
            tmp_path,
            f'[profiles.p]\nscheduler = "{sched}"\ncluster_name = "c"\n',
        )
        p = load_config(path).profiles["p"]
        assert p.scheduler == sched


class TestLoadProfile:
    def test_returns_named_profile(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.gpu]\nscheduler = "slurm"\ncluster_name = "hpc"\n',
        )
        p = load_profile("gpu", path)
        assert isinstance(p, MolqProfile)
        assert p.name == "gpu"

    def test_missing_profile_raises(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.gpu]\nscheduler = "slurm"\ncluster_name = "hpc"\n',
        )
        with pytest.raises(ConfigError):
            load_profile("cpu", path)


class TestDefaults:
    def test_resources_defaults(self, tmp_path):
        toml = (
            '[profiles.p]\nscheduler = "slurm"\ncluster_name = "c"\n'
            "[profiles.p.defaults.resources]\ncpu_count = 8\n"
        )
        p = load_config(_write_toml(tmp_path, toml)).profiles["p"]
        assert p.defaults.resources is not None
        assert p.defaults.resources.cpu_count == 8

    def test_scheduling_defaults(self, tmp_path):
        toml = (
            '[profiles.p]\nscheduler = "slurm"\ncluster_name = "c"\n'
            '[profiles.p.defaults.scheduling]\nqueue = "gpu"\n'
        )
        p = load_config(_write_toml(tmp_path, toml)).profiles["p"]
        assert p.defaults.scheduling is not None
        assert p.defaults.scheduling.queue == "gpu"

    def test_no_defaults_gives_none_fields(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.p]\nscheduler = "local"\ncluster_name = "c"\n',
        )
        p = load_config(path).profiles["p"]
        assert p.defaults.resources is None
        assert p.defaults.scheduling is None
        assert p.defaults.execution is None


class TestRetry:
    def test_retry_policy_parsed(self, tmp_path):
        toml = (
            '[profiles.p]\nscheduler = "slurm"\ncluster_name = "c"\n'
            "[profiles.p.retry]\nmax_attempts = 3\n"
        )
        p = load_config(_write_toml(tmp_path, toml)).profiles["p"]
        assert p.retry is not None
        assert p.retry.max_attempts == 3

    def test_no_retry_gives_none(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.p]\nscheduler = "local"\ncluster_name = "c"\n',
        )
        assert load_config(path).profiles["p"].retry is None


class TestRetention:
    def test_retention_defaults_applied(self, tmp_path):
        path = _write_toml(
            tmp_path,
            '[profiles.p]\nscheduler = "local"\ncluster_name = "c"\n',
        )
        p = load_config(path).profiles["p"]
        assert p.retention.keep_job_dirs_for_days == 30

    def test_retention_override(self, tmp_path):
        toml = (
            '[profiles.p]\nscheduler = "local"\ncluster_name = "c"\n'
            "[profiles.p.retention]\nkeep_job_dirs_for_days = 7\n"
        )
        p = load_config(_write_toml(tmp_path, toml)).profiles["p"]
        assert p.retention.keep_job_dirs_for_days == 7
