"""
Tests for the unified resource specification system.
"""

import pytest

from molq.resources import (
    BaseResourceSpec,
    ClusterResourceSpec,
    ComputeResourceSpec,
    EmailEvent,
    LsfMapper,
    MemoryParser,
    PbsMapper,
    PriorityLevel,
    ResourceManager,
    SlurmMapper,
    TimeParser,
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


class TestBaseResourceSpec:
    """Test basic resource specification."""

    def test_initialization(self):
        """Test basic initialization."""
        spec = BaseResourceSpec(
            cmd="python train.py", workdir="/tmp", job_name="test_job"
        )

        assert spec.cmd == "python train.py"
        assert spec.workdir == "/tmp"
        assert spec.job_name == "test_job"
        assert spec.block is True  # Default value

    def test_alias_support(self):
        """Test that aliases work correctly."""
        spec = BaseResourceSpec(
            cmd="python test.py", cwd="/home/user"  # Using alias instead of workdir
        )

        assert spec.workdir == "/home/user"  # Should be accessible via original name

    def test_to_dict(self):
        """Test conversion to dictionary."""
        spec = BaseResourceSpec(
            cmd="python test.py", workdir="/tmp", env={"VAR": "value"}
        )

        data = spec.model_dump()
        assert data["cmd"] == "python test.py"
        assert data["workdir"] == "/tmp"
        assert data["env"] == {"VAR": "value"}


class TestComputeResourceSpec:
    """Test compute resource specification."""

    def test_initialization(self):
        """Test compute spec initialization."""
        spec = ComputeResourceSpec(
            cmd="python train.py", cpu_count=8, memory="16GB", time_limit="4h"
        )

        assert spec.cmd == "python train.py"
        assert spec.cpu_count == 8
        assert spec.memory == "16GB"
        assert spec.time_limit == "4h"

    def test_validation(self):
        """Test parameter validation."""
        # Valid spec should work
        spec = ComputeResourceSpec(cmd="python test.py", memory="8GB", time_limit="2h")

        # Invalid time format should raise error
        with pytest.raises(ValueError):
            ComputeResourceSpec(cmd="python test.py", time_limit="invalid_time")

        # Invalid memory format should raise error
        with pytest.raises(ValueError):
            ComputeResourceSpec(cmd="python test.py", memory="invalid_memory")


class TestClusterResourceSpec:
    """Test cluster resource specification."""

    def test_initialization(self):
        """Test cluster spec initialization."""
        spec = ClusterResourceSpec(
            cmd="python distributed.py",
            queue="gpu",
            cpu_count=16,
            memory="32GB",
            time_limit="8h",
            gpu_count=2,
            gpu_type="v100",
        )

        assert spec.cmd == "python distributed.py"
        assert spec.queue == "gpu"
        assert spec.cpu_count == 16
        assert spec.gpu_count == 2
        assert spec.gpu_type == "v100"

    def test_gpu_validation(self):
        """Test GPU consistency validation."""
        # Valid GPU spec should work
        spec = ClusterResourceSpec(cmd="python test.py", gpu_count=2, gpu_type="v100")

        # GPU type without count should fail
        with pytest.raises(ValueError):
            ClusterResourceSpec(
                cmd="python test.py", gpu_type="v100"  # Missing gpu_count
            )

    def test_cpu_validation(self):
        """Test CPU consistency validation."""
        # Valid CPU distribution
        spec = ClusterResourceSpec(
            cmd="python test.py", cpu_count=16, cpu_per_node=8, node_count=2
        )

        # Invalid CPU distribution should fail
        with pytest.raises(ValueError):
            ClusterResourceSpec(
                cmd="python test.py",
                cpu_count=16,
                cpu_per_node=8,
                node_count=3,  # 8*3=24 != 16
            )


class TestSlurmMapper:
    """Test SLURM parameter mapping."""

    def test_basic_mapping(self):
        """Test basic parameter mapping."""
        spec = ClusterResourceSpec(
            cmd="python test.py",
            queue="compute",
            cpu_count=4,
            memory="8GB",
            time_limit="2h",
            job_name="test_job",
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
        spec = ClusterResourceSpec(cmd="python test.py", gpu_count=2, gpu_type="v100")

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--gres"] == "gpu:v100:2"

    def test_priority_mapping(self):
        """Test priority level mapping."""
        spec = ClusterResourceSpec(cmd="python test.py", priority=PriorityLevel.HIGH)

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--priority"] == "750"

    def test_email_events_mapping(self):
        """Test email events mapping."""
        spec = ClusterResourceSpec(
            cmd="python test.py",
            email="user@example.com",
            email_events=[EmailEvent.START, EmailEvent.END],
        )

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["--mail-user"] == "user@example.com"
        assert mapped["--mail-type"] == "BEGIN,END"

    def test_command_args_formatting(self):
        """Test command arguments formatting."""
        spec = ClusterResourceSpec(
            cmd="python test.py", queue="compute", cpu_count=4, exclusive_node=True
        )

        mapper = SlurmMapper()
        mapped = mapper.map_resources(spec)
        args = mapper.format_command_args(mapped)

        assert "--partition" in args
        assert "compute" in args
        assert "--ntasks" in args
        assert "4" in args
        assert "--exclusive" in args


class TestPbsMapper:
    """Test PBS parameter mapping."""

    def test_nodes_and_ppn_mapping(self):
        """Test nodes and ppn combination."""
        spec = ClusterResourceSpec(cmd="python test.py", node_count=2, cpu_per_node=8)

        mapper = PbsMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-l nodes"] == "2:ppn=8"

    def test_single_node_cpu_mapping(self):
        """Test single node with CPU count."""
        spec = ClusterResourceSpec(cmd="python test.py", cpu_count=16)

        mapper = PbsMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-l nodes"] == "1:ppn=16"

    def test_time_and_memory_mapping(self):
        """Test time and memory mapping."""
        spec = ClusterResourceSpec(
            cmd="python test.py", time_limit="4h30m", memory="32GB"
        )

        mapper = PbsMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-l walltime"] == "04:30:00"
        assert mapped["-l mem"] == "32gb"


class TestLsfMapper:
    """Test LSF parameter mapping."""

    def test_basic_mapping(self):
        """Test basic parameter mapping."""
        spec = ClusterResourceSpec(
            cmd="python test.py",
            queue="normal",
            cpu_count=8,
            memory="16GB",
            time_limit="2h",
        )

        mapper = LsfMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-q"] == "normal"
        assert mapped["-n"] == "8"
        assert mapped["-M"] == str(16 * 1024 * 1024)  # MB
        assert mapped["-W"] == "120"  # Minutes

    def test_array_job_mapping(self):
        """Test array job mapping."""
        spec = ClusterResourceSpec(
            cmd="python test.py", job_name="my_job", array_spec="1-100"
        )

        mapper = LsfMapper()
        mapped = mapper.map_resources(spec)

        assert mapped["-J"] == "my_job[1-100]"


class TestResourceManager:
    """Test resource manager functionality."""

    def test_create_spec(self):
        """Test spec creation."""
        spec = ResourceManager.create_spec(
            spec_type="cluster",
            cmd="python test.py",
            queue="compute",
            cpu_count=4,
            memory="8GB",
        )

        assert isinstance(spec, ClusterResourceSpec)
        assert spec.queue == "compute"
        assert spec.cpu_count == 4

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

    def test_map_to_scheduler(self):
        """Test mapping to different schedulers."""
        spec = ClusterResourceSpec(
            cmd="python test.py",
            queue="compute",
            cpu_count=4,
            memory="8GB",
            time_limit="2h",
        )

        # Test SLURM mapping
        slurm_mapped = ResourceManager.map_to_scheduler(spec, "slurm")
        assert "--partition" in slurm_mapped
        assert slurm_mapped["--partition"] == "compute"

        # Test PBS mapping
        pbs_mapped = ResourceManager.map_to_scheduler(spec, "pbs")
        assert "-q" in pbs_mapped
        assert pbs_mapped["-q"] == "compute"

    def test_format_command_args(self):
        """Test command argument formatting."""
        spec = ClusterResourceSpec(cmd="python test.py", queue="compute", cpu_count=4)

        args = ResourceManager.format_command_args(spec, "slurm")
        assert isinstance(args, list)
        assert "--partition" in args
        assert "compute" in args


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

        assert isinstance(spec, ClusterResourceSpec)
        assert spec.cmd == "python analysis.py"
        assert spec.cpu_count == 8
        assert spec.memory == "16GB"
        assert spec.time_limit == "4h"
        assert spec.job_name == "analysis"

    def test_create_gpu_job(self):
        """Test GPU job creation."""
        spec = create_gpu_job(
            cmd="python gpu_train.py",
            gpu_count=2,
            cpu_count=16,
            memory="32GB",
            time_limit="12h",
            gpu_type="v100",
        )

        assert isinstance(spec, ClusterResourceSpec)
        assert spec.cmd == "python gpu_train.py"
        assert spec.gpu_count == 2
        assert spec.gpu_type == "v100"
        assert spec.cpu_count == 16

    def test_create_array_job(self):
        """Test array job creation."""
        spec = create_array_job(
            cmd="python batch.py",
            array_spec="1-100",
            cpu_count=2,
            memory="4GB",
            time_limit="1h",
        )

        assert isinstance(spec, ClusterResourceSpec)
        assert spec.cmd == "python batch.py"
        assert spec.array_spec == "1-100"
        assert spec.cpu_count == 2

    def test_create_high_memory_job(self):
        """Test high memory job creation."""
        spec = create_high_memory_job(
            cmd="python big_analysis.py", memory="128GB", cpu_count=32, time_limit="24h"
        )

        assert isinstance(spec, ClusterResourceSpec)
        assert spec.cmd == "python big_analysis.py"
        assert spec.memory == "128GB"
        assert spec.exclusive_node is True


class TestIntegrationScenarios:
    """Test realistic usage scenarios."""

    def test_machine_learning_job(self):
        """Test ML training job specification."""
        spec = ClusterResourceSpec(
            cmd="python train_bert.py",
            queue="gpu",
            gpu_count=4,
            gpu_type="a100",
            cpu_count=32,
            memory="128GB",
            time_limit="3d",
            job_name="bert_training",
            email="researcher@university.edu",
            email_events=[EmailEvent.END, EmailEvent.FAIL],
            output_file="training_%j.out",
            error_file="training_%j.err",
            account="ml_project",
        )

        # Test SLURM mapping
        slurm_mapped = ResourceManager.map_to_scheduler(spec, "slurm")
        assert slurm_mapped["--partition"] == "gpu"
        assert slurm_mapped["--gres"] == "gpu:a100:4"
        assert slurm_mapped["--ntasks"] == "32"
        assert slurm_mapped["--time"] == "3-00:00:00"

    def test_bioinformatics_pipeline(self):
        """Test bioinformatics pipeline job."""
        spec = ClusterResourceSpec(
            cmd="bash genome_pipeline.sh",
            queue="compute",
            cpu_count=64,
            memory="256GB",
            time_limit="7d",
            job_name="genome_assembly",
            exclusive_node=True,
            constraints=["intel", "infiniband"],
            licenses=["bioinformatics:1"],
            priority=PriorityLevel.HIGH,
        )

        # Test constraint and license handling
        slurm_mapped = ResourceManager.map_to_scheduler(spec, "slurm")
        assert slurm_mapped["--constraint"] == "intel&infiniband"
        assert slurm_mapped["--licenses"] == "bioinformatics:1"
        assert slurm_mapped["--priority"] == "750"  # HIGH priority

    def test_parameter_sweep_array(self):
        """Test parameter sweep array job."""
        spec = ClusterResourceSpec(
            cmd="python sweep.py --param $SLURM_ARRAY_TASK_ID",
            queue="compute",
            cpu_count=4,
            memory="8GB",
            time_limit="2h",
            array_spec="1-1000:10",  # 100 tasks, step size 10
            job_name="param_sweep",
            output_file="sweep_%A_%a.out",
            error_file="sweep_%A_%a.err",
        )

        # Test array job handling
        slurm_mapped = ResourceManager.map_to_scheduler(spec, "slurm")
        assert slurm_mapped["--array"] == "1-1000:10"
        assert slurm_mapped["--output"] == "sweep_%A_%a.out"
