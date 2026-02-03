import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from molq.submitor import SlurmSubmitor
from molq.resources import JobSpec, ExecutionSpec, ResourceSpec


class MockedSlurm:
    @staticmethod
    def run(cmd, capture_output=None, **kwargs) -> subprocess.CompletedProcess:
        if cmd[0] == "sbatch":
            return MockedSlurm.sbatch(cmd)
        elif cmd[0] == "squeue":
            return MockedSlurm.squeue(cmd)
        elif cmd[0] == "scancel":
            return MockedSlurm.scancel(cmd)
        # Default mock if command is not recognized
        mock = MagicMock()
        mock.returncode = 1
        mock.stdout = b""
        mock.stderr = b"Unknown command"
        return mock

    @staticmethod
    def sbatch(cmd):
        mock = MagicMock()
        mock.returncode = 0
        if "--test-only" in cmd:
            mock.stderr = "sbatch: Job 3676091 to start at 2024-04-26T20:02:12 using 256 processors on nodes nid001000 in partition main"
        else:
            mock.stdout = "3676091"
        return mock

    @staticmethod
    def squeue(cmd):
        mock = MagicMock()
        mock.stdout = "3676091 R test main user RUNNING 0:01 1 nid001000 2024-04-26T20:02:12"
        return mock

    @staticmethod
    def scancel(cmd):
        mock = MagicMock()
        mock.stdout = b"Job 3676091 has been cancelled."
        return mock


class TestSlurmAdapter:
    @pytest.fixture(scope="class", autouse=True)
    def delete_script(self):
        yield
        script_path = Path("run_slurm.sh")
        if script_path.exists():
            script_path.unlink()

    def test_gen_script(self):
        """Test script generation with new architecture."""
        submitor = SlurmSubmitor("test_submitor")
        
        # Create a JobSpec with the required parameters
        spec = JobSpec(
            execution=ExecutionSpec(cmd=["ls"], job_name="test"),
            resources=ResourceSpec(cpu_count=1)
        )
        
        # Use the script generator directly
        from molq.scripts import SlurmScriptGenerator
        generator = SlurmScriptGenerator()
        path = generator.generate(spec, Path("run_slurm.sh"))
        
        with open(path, "r") as f:
            content = f.read()
        
        assert "#!/bin/bash" in content
        assert "#SBATCH --job-name=test" in content
        assert "#SBATCH --ntasks=1" in content
        assert "ls" in content

    def test_submit(self, mocker: MockerFixture):
        """Test job submission with new architecture."""
        mocker.patch.object(subprocess, "run", MockedSlurm.run)

        submitor = SlurmSubmitor("test_submitor")
        # Use the new format - validate_config will handle legacy format
        # Set block=False to avoid waiting for job completion in tests
        job_id = submitor.submit({"cmd": ["ls"], "job_name": "test", "cpu_count": 1, "block": False})
        assert isinstance(job_id, int)

    def test_submit_test_only(self, mocker: MockerFixture):
        """Test job submission with test_only flag."""
        mocker.patch.object(subprocess, "run", MockedSlurm.run)

        submitor = SlurmSubmitor("test_submitor")
        job_id = submitor.submit(
            {"cmd": ["ls"], "job_name": "test", "cpu_count": 1, "test_only": True, "block": False}
        )
        assert isinstance(job_id, int)

    def test_monitoring(self, mocker: MockerFixture):
        """Test job monitoring."""
        mocker.patch.object(subprocess, "run", MockedSlurm.run)

        submitor = SlurmSubmitor("test_submitor")
        job_id = submitor.submit(
            {"cmd": ["sleep 1"], "job_name": "test1", "cpu_count": 1, "block": False}
        )
        
        # Query the job status
        statuses = submitor.query(job_id)
        assert job_id in statuses
