"""Test cleanup_temp_files functionality in ResourceSpec."""

from pathlib import Path

import pytest

from molq import submit
from molq.resources import BaseResourceSpec, ComputeResourceSpec


class TestCleanupTempFiles:
    """Test the cleanup_temp_files parameter in ResourceSpec."""

    def test_default_cleanup_enabled(self, isolated_temp_dir, cleanup_after_test):
        """Test that cleanup is enabled by default."""
        local_submitter = submit("test_cleanup_default", "local")

        @local_submitter
        def job_default_cleanup():
            spec = BaseResourceSpec(
                cmd=["echo", "default cleanup test"],
                job_name="default_cleanup",
                block=True,  # cleanup_temp_files defaults to True
            )
            config = spec.model_dump()
            config["script_name"] = "test_default_cleanup.sh"
            job_id = yield config
            return job_id

        # Run job
        job_id = job_default_cleanup()
        assert job_id is not None

        # Script should be cleaned up
        script_path = isolated_temp_dir / "test_default_cleanup.sh"
        assert not script_path.exists(), "Script should be cleaned up by default"

    def test_explicit_cleanup_enabled(self, isolated_temp_dir, cleanup_after_test):
        """Test explicit cleanup enabled."""
        local_submitter = submit("test_cleanup_explicit", "local")

        @local_submitter
        def job_explicit_cleanup():
            spec = BaseResourceSpec(
                cmd=["echo", "explicit cleanup test"],
                job_name="explicit_cleanup",
                cleanup_temp_files=True,
                block=True,
            )
            config = spec.model_dump()
            config["script_name"] = "test_explicit_cleanup.sh"
            job_id = yield config
            return job_id

        job_id = job_explicit_cleanup()
        assert job_id is not None

        script_path = Path("test_explicit_cleanup.sh")
        assert (
            not script_path.exists()
        ), "Script should be cleaned up when explicitly enabled"

    def test_cleanup_disabled(self):
        """Test cleanup disabled."""
        local_submitter = submit("test_cleanup_disabled", "local")

        @local_submitter
        def job_no_cleanup():
            spec = BaseResourceSpec(
                cmd=["echo", "no cleanup test"],
                job_name="no_cleanup",
                cleanup_temp_files=False,
                block=True,
            )
            config = spec.model_dump()
            config["script_name"] = "test_no_cleanup.sh"
            job_id = yield config
            return job_id

        job_id = job_no_cleanup()
        assert job_id is not None

        script_path = Path("test_no_cleanup.sh")
        assert script_path.exists(), "Script should be kept when cleanup is disabled"

        # Clean up manually
        script_path.unlink()

    def test_compute_resource_spec_cleanup(self):
        """Test cleanup with ComputeResourceSpec."""
        local_submitter = submit("test_compute_cleanup", "local")

        @local_submitter
        def compute_job():
            spec = ComputeResourceSpec(
                cmd=["echo", "compute cleanup test"],
                job_name="compute_cleanup",
                cpu_count=2,
                memory="1GB",
                cleanup_temp_files=True,
                block=True,
            )
            config = spec.model_dump()
            config["script_name"] = "test_compute_cleanup.sh"
            job_id = yield config
            return job_id

        job_id = compute_job()
        assert job_id is not None

        script_path = Path("test_compute_cleanup.sh")
        assert (
            not script_path.exists()
        ), "ComputeResourceSpec should also support cleanup"

    def test_backwards_compatibility_dict_config(self):
        """Test that cleanup works with traditional dict config."""
        local_submitter = submit("test_dict_cleanup", "local")

        @local_submitter
        def dict_job():
            config = {
                "cmd": ["echo", "dict config test"],
                "job_name": "dict_cleanup",
                "cleanup_temp_files": False,
                "block": True,
                "script_name": "test_dict_cleanup.sh",
            }
            job_id = yield config
            return job_id

        job_id = dict_job()
        assert job_id is not None

        script_path = Path("test_dict_cleanup.sh")
        assert (
            script_path.exists()
        ), "Dict config should respect cleanup_temp_files=False"

        # Clean up manually
        script_path.unlink()

    def test_resource_spec_model_dump(self):
        """Test that cleanup_temp_files is properly included in model_dump."""
        spec = BaseResourceSpec(
            cmd=["echo", "test"], job_name="test", cleanup_temp_files=False
        )

        config = spec.model_dump()
        assert "cleanup_temp_files" in config
        assert config["cleanup_temp_files"] is False

        # Test default value
        spec_default = BaseResourceSpec(cmd=["echo", "test"], job_name="test")

        config_default = spec_default.model_dump()
        assert "cleanup_temp_files" in config_default
        assert config_default["cleanup_temp_files"] is True
