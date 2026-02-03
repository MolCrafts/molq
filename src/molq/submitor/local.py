# pragma: no cover
"""Local submitter using the refactored architecture."""

from pathlib import Path

from molq.adapters import LocalAdapter
from molq.lifecycle import JobLifecycleManager
from molq.resources import JobSpec, ExecutionSpec
from molq.submitor.base import BaseSubmitor
from molq.tempfiles import TempFileManager


class LocalSubmitor(BaseSubmitor):
    """Execute jobs on the current machine using bash.
    
    This is a thin wrapper around BaseSubmitor that uses the LocalAdapter.
    """

    def __init__(self, cluster_name: str, cluster_config: dict = {}):
        """Initialize the local submitter.
        
        Args:
            cluster_name: Name of the cluster/section
            cluster_config: Optional cluster configuration
        """
        adapter = LocalAdapter()
        lifecycle_manager = JobLifecycleManager()
        temp_manager = TempFileManager()
        
        super().__init__(
            cluster_name=cluster_name,
            adapter=adapter,
            lifecycle_manager=lifecycle_manager,
            temp_manager=temp_manager,
            cluster_config=cluster_config,
        )
    
    def gen_script(self, script_path: str | Path, cmd: list[str], conda_env: str | None = None) -> Path:
        """Generate a bash script.
        
        Args:
            script_path: Path where script should be written
            cmd: Command to execute
            conda_env: Optional conda environment to activate
            
        Returns:
            Path to generated script
        """
        # Create a JobSpec with conda_env in extra
        extra = {"conda_env": conda_env} if conda_env else {}
        spec = JobSpec(execution=ExecutionSpec(cmd=cmd, extra=extra))
        script_path = Path(script_path)
        return self.adapter.script_generator.generate(spec, script_path)
