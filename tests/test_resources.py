"""
Tests for the unified resource specification system.
"""

import pytest

from molq.resources import (
    ResourceSpec,
    ExecutionSpec,
    ClusterSpec,
    JobSpec,
    EmailEvent,
    PriorityLevel,
    ResourceManager,
    SlurmMapper,
    PbsMapper,
    LsfMapper,
    TimeParser,
    MemoryParser,
    create_array_job,
    create_compute_job,
    create_gpu_job,
    create_high_memory_job,
)


class TestTimeParser:
    """Test time parsing functionality."""

    def test_parse_hms_format(self):
        """Test HH:MM:SS format parsing."""
        assert TimeParser.parse_duration("02:30:00") == 9000
        assert TimeParser.parse_duration("00:30:00") == 1800
        assert TimeParser.parse_duration("24:00:00") == 86400

    def test_parse_human_readable(self):
        """Test human-readable format parsing."""
        assert TimeParser.parse_duration("2h30m") == 9000
        assert TimeParser.parse_duration("90m") == 5400
        assert TimeParser.parse_duration("1.5h") == 5400
        assert TimeParser.parse_duration("3600s") == 3600
        assert TimeParser.parse_duration("1d") == 86400
        assert TimeParser.parse_duration("1w") == 604800

    def test_to_slurm_format(self):
        """Test conversion to SLURM format."""
        assert TimeParser.to_slurm_format("2h30m") == "02:30:00"
        assert TimeParser.to_slurm_format("25h") == "1-01:00:00"
        assert TimeParser.to_slurm_format("1d2h") == "1-02:00:00"


class TestMemoryParser:
    """Test memory parsing functionality."""

    def test_parse_memory_units(self):
        """Test parsing with different units."""
        assert MemoryParser.parse_memory("1GB") == 1024**3
        assert MemoryParser.parse_memory("512MB") == 512 * 1024**2
        assert MemoryParser.parse_memory("2TB") == 2 * 1024**4
        assert MemoryParser.parse_memory("4.5GB") == int(4.5 * 1024**3)

    def test_parse_plain_bytes(self):
        """Test parsing plain byte values."""
        assert MemoryParser.parse_memory("1073741824") == 1024**3
        assert MemoryParser.parse_memory("512") == 512

    def test_to_slurm_format(self):
        """Test conversion to SLURM format."""
        assert MemoryParser.to_slurm_format("1GB") == "1G"
        assert MemoryParser.to_slurm_format("512MB") == "512M"
        assert MemoryParser.to_slurm_format("2TB") == "2T"


class TestJobSpec:
    """Test the unified job specification."""

    def test_initialization(self):
        """Test basic initialization."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python train.py", workdir="/tmp", job_name="test_job")
        )

        assert spec.execution.cmd == "python train.py"
        assert spec.execution.workdir == "/tmp"
        assert spec.execution.job_name == "test_job"
        assert spec.execution.block is True  # Default value

    def test_alias_support(self):
        """Test that aliases work correctly."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py", cwd="/home/user")
        )

        assert spec.execution.workdir == "/home/user"

    def test_validation(self):
        """Test parameter validation."""
        # Invalid time format should raise error
        with pytest.raises(ValueError):
            JobSpec(execution=ExecutionSpec(cmd="python test.py"), resources=ResourceSpec(time_limit="invalid_time"))

        # Invalid memory format should raise error
        with pytest.raises(ValueError):
            JobSpec(execution=ExecutionSpec(cmd="python test.py"), resources=ResourceSpec(memory="invalid_memory"))

    def test_gpu_validation(self):
        """Test GPU consistency validation."""
        # Valid GPU spec should work
        JobSpec(execution=ExecutionSpec(cmd="python test.py"), resources=ResourceSpec(gpu_count=2, gpu_type="v100"))

        # GPU type without count should fail
        with pytest.raises(ValueError):
            JobSpec(execution=ExecutionSpec(cmd="python test.py"), resources=ResourceSpec(gpu_type="v100"))

    def test_cpu_validation(self):
        """Test CPU consistency validation."""
        # Valid CPU distribution
        JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            resources=ResourceSpec(cpu_count=16),
            cluster=ClusterSpec(cpu_per_node=8, node_count=2)
        )

        # Invalid CPU distribution should fail
        with pytest.raises(ValueError):
            JobSpec(
                execution=ExecutionSpec(cmd="python test.py"),
                resources=ResourceSpec(cpu_count=16),
                cluster=ClusterSpec(cpu_per_node=8, node_count=3)
            )


class TestSlurmMapper:
    """Test SLURM parameter mapping."""

    def test_basic_mapping(self):
        """Test basic parameter mapping."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py", job_name="test_job"),
            resources=ResourceSpec(cpu_count=4, memory="8GB", time_limit="2h"),
            cluster=ClusterSpec(queue="compute")
        )

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--partition"] == "compute"
        assert mapped["--ntasks"] == "4"
        assert mapped["--mem"] == "8G"
        assert mapped["--time"] == "02:00:00"
        assert mapped["--job-name"] == "test_job"

    def test_gpu_mapping(self):
        """Test GPU resource mapping."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            resources=ResourceSpec(gpu_count=2, gpu_type="v100")
        )

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--gres"] == "gpu:v100:2"

    def test_priority_mapping(self):
        """Test priority level mapping."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            cluster=ClusterSpec(priority=PriorityLevel.HIGH)
        )

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--priority"] == "750"

    def test_email_events_mapping(self):
        """Test email events mapping."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            cluster=ClusterSpec(email="user@example.com", email_events=[EmailEvent.START, EmailEvent.END])
        )

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--mail-user"] == "user@example.com"
        assert mapped["--mail-type"] == "BEGIN,END"


class TestPbsMapper:
    """Test PBS parameter mapping."""

    def test_nodes_and_ppn_mapping(self):
        """Test nodes and ppn combination."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            cluster=ClusterSpec(node_count=2, cpu_per_node=8)
        )

        mapper = PbsMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-l"] == "nodes=2:ppn=8"

    def test_single_node_cpu_mapping(self):
        """Test single node with CPU count."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            resources=ResourceSpec(cpu_count=16)
        )

        mapper = PbsMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-l"] == "nodes=1:ppn=16"


class TestLsfMapper:
    """Test LSF parameter mapping."""

    def test_basic_mapping(self):
        """Test basic parameter mapping."""
        spec = JobSpec(
            execution=ExecutionSpec(cmd="python test.py"),
            resources=ResourceSpec(cpu_count=8, memory="16GB", time_limit="2h"),
            cluster=ClusterSpec(queue="normal")
        )

        mapper = LsfMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-q"] == "normal"
        assert mapped["-n"] == "8"
        assert mapped["-M"] == str(16 * 1024 * 1024)
        assert mapped["-W"] == "120"


class TestResourceManager:
    """Test resource manager functionality."""

    def test_get_mapper(self):
        """Test mapper retrieval."""
        slurm_mapper = ResourceManager.get_mapper("slurm")
        assert isinstance(slurm_mapper, SlurmMapper)

        pbs_mapper = ResourceManager.get_mapper("pbs")
        assert isinstance(pbs_mapper, PbsMapper)

    def test_unsupported_scheduler(self):
        """Test unsupported scheduler handling."""
        with pytest.raises(ValueError):
            ResourceManager.get_mapper("unsupported")


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_create_compute_job(self):
        """Test compute job creation."""
        spec = create_compute_job(
            cmd="python analysis.py",
            cpu_count=8,
            memory="16GB",
            time_limit="4h",
            job_name="analysis",
        )

        assert isinstance(spec, JobSpec)
        assert spec.execution.cmd == "python analysis.py"
        assert spec.resources.cpu_count == 8
        assert spec.resources.memory == "16GB"
        assert spec.resources.time_limit == "4h"
        assert spec.execution.job_name == "analysis"
