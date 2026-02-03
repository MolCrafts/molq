# pragma: no cover
"""SLURM submitter using the refactored architecture."""

from pathlib import Path

from molq.adapters import SlurmAdapter
from molq.lifecycle import JobLifecycleManager
from molq.resources import JobSpec, ExecutionSpec
from molq.submitor.base import BaseSubmitor
from molq.tempfiles import TempFileManager


class SlurmSubmitor(BaseSubmitor):
    """Submit jobs to a SLURM workload manager.
    
    This is a thin wrapper around BaseSubmitor that uses the SlurmAdapter.
    """

    def __init__(self, cluster_name: str, cluster_config: dict = {}):
        """Initialize the SLURM submitter.
        
        Args:
            cluster_name: Name of the cluster
            cluster_config: Optional cluster configuration
        """
        adapter = SlurmAdapter()
        lifecycle_manager = JobLifecycleManager()
        temp_manager = TempFileManager()
        
        super().__init__(
            cluster_name=cluster_name,
            adapter=adapter,
            lifecycle_manager=lifecycle_manager,
            temp_manager=temp_manager,
            cluster_config=cluster_config,
        )
    
    def gen_script(self, script_path: str | Path, cmd: list[str], **kwargs) -> Path:
        """Generate a SLURM script.
        
        Args:
            script_path: Path where script should be written
            cmd: Command to execute
            **kwargs: Additional SLURM parameters
            
        Returns:
            Path to generated script
        """
        # Create a minimal JobSpec for script generation
        spec = JobSpec(execution=ExecutionSpec(cmd=cmd))
        script_path = Path(script_path)
        return self.adapter.script_generator.generate(spec, script_path)
