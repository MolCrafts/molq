"""
Tests for the test infrastructure itself and integration testing.
"""

import subprocess
from pathlib import Path

from molq.submitor.base import JobStatus
from molq.submitor.local import LocalSubmitor


class TestTestInfrastructure:
    """Test the test infrastructure fixtures."""

    def test_temp_workdir_isolation(self, temp_workdir):
        """Test that temp_workdir provides proper isolation."""
        # We should be in a temporary directory
        current_dir = Path.cwd()
        assert current_dir.is_absolute()
        assert "molexp_test_" in str(current_dir)

        # Create a test file
        test_file = current_dir / "test_isolation.txt"
        test_file.write_text("test content")
        assert test_file.exists()

    def test_tmp_molq_home_isolation(self, tmp_molq_home):
        """Test that tmp_molq_home properly isolates molq files."""
        # HOME should be set to the temporary directory
        home_dir = Path.home()
        assert str(home_dir) == str(tmp_molq_home)

        # .molq directory should exist
        molq_dir = home_dir / ".molq"
        assert molq_dir.exists()
        assert molq_dir.is_dir()

    def test_isolated_temp_dir(self, isolated_temp_dir):
        """Test that isolated_temp_dir provides per-test isolation."""
        # Should have an isolated directory
        assert isolated_temp_dir.exists()
        assert isolated_temp_dir.is_dir()

        # Create files that won't interfere with other tests
        test_script = isolated_temp_dir / "isolated_test.sh"
        test_script.write_text("#!/bin/bash\necho 'isolated test'")
        assert test_script.exists()

    def test_mock_job_environment(self, mock_job_environment):
        """Test the complete mock job environment."""
        env = mock_job_environment

        # Check all expected keys
        assert "home" in env
        assert "workdir" in env
        assert "molq_dir" in env

        # HOME should be set correctly
        assert str(Path.home()) == str(env["home"])

        # We should be in the work directory
        assert Path.cwd() == env["workdir"]

        # Molq directory should exist
        assert env["molq_dir"].exists()


class TestIntegratedJobExecution:
    """Integration tests using the full test infrastructure."""

    def test_local_job_with_complete_isolation(self, mock_job_environment):
        """Test local job execution with complete isolation."""
        env = mock_job_environment

        # Create a local submitter
        submitter = LocalSubmitor("test")

        # Submit a job that creates a file
        output_file = env["workdir"] / "job_output.txt"
        job_id = submitter.local_submit(
            job_name="isolated_test_job",
            cmd=["echo", "Hello from isolated job", ">", str(output_file)],
            cwd=str(env["workdir"]),
            block=True,
        )

        assert job_id is not None

        # Check that job was registered in the database
        jobs = submitter.list_jobs("local")
        assert len(jobs) > 0
        assert any(job["name"] == "isolated_test_job" for job in jobs)

    def test_job_database_isolation(self, tmp_molq_home):
        """Test that job databases properly separate jobs by section."""
        # Create submitters (they share the same database but use different sections)
        submitter1 = LocalSubmitor("test1")
        submitter2 = LocalSubmitor("test2")

        # Register jobs in different sections
        submitter1.register_job(
            "test1",
            1001,
            "job1",
            JobStatus.Status.RUNNING,
            "echo test1",
            str(tmp_molq_home),
            1234567890,
        )

        submitter2.register_job(
            "test2",
            2001,
            "job2",
            JobStatus.Status.PENDING,
            "echo test2",
            str(tmp_molq_home),
            1234567890,
        )

        # Each should only see their own section's jobs
        jobs1 = submitter1.list_jobs("test1")
        jobs2 = submitter2.list_jobs("test2")

        assert len(jobs1) == 1
        assert len(jobs2) == 1
        assert jobs1[0]["name"] == "job1"
        assert jobs2[0]["name"] == "job2"

        # But cross-section queries should show jobs from different sections
        # (since they share the same database)
        cross_jobs1 = submitter1.list_jobs("test2")
        cross_jobs2 = submitter2.list_jobs("test1")
        assert len(cross_jobs1) == 1  # Can see test2 section
        assert len(cross_jobs2) == 1  # Can see test1 section

        # Query all jobs should show both
        all_jobs = submitter1.list_jobs()
        assert len(all_jobs) == 2

    def test_script_generation_and_cleanup(self, test_script_dir, cleanup_after_test):
        """Test script generation with proper cleanup."""
        submitter = LocalSubmitor("test")

        # Generate a script in the test script directory
        script_path = test_script_dir / "test_script.sh"
        cmd = ["echo", "Hello World"]

        generated_script = submitter._gen_script(script_path, cmd, conda_env=None)

        # Script should be created
        assert generated_script.exists()
        assert generated_script == script_path

        # Content should be correct
        content = generated_script.read_text()
        assert "#!/bin/bash" in content
        assert "echo Hello World" in content

        # cleanup_after_test fixture will handle cleanup

    def test_file_operations_in_temp_workdir(self, temp_workdir):
        """Test that file operations work correctly in temp workdir."""
        # Create various types of files
        text_file = temp_workdir / "test.txt"
        script_file = temp_workdir / "test_script.sh"
        data_dir = temp_workdir / "data"

        # Write files
        text_file.write_text("test data")
        script_file.write_text("#!/bin/bash\necho 'test'")
        data_dir.mkdir()
        (data_dir / "config.json").write_text('{"test": true}')

        # Verify they exist
        assert text_file.exists()
        assert script_file.exists()
        assert data_dir.exists()
        assert (data_dir / "config.json").exists()

        # Test that we can execute scripts
        script_file.chmod(0o755)
        result = subprocess.run(
            ["bash", str(script_file)], capture_output=True, text=True
        )
        assert result.returncode == 0
        assert "test" in result.stdout
