"""Script generation for job submission.

This module provides script generators for different schedulers, separating
the concern of script generation from job submission logic.
"""

from pathlib import Path
from typing import Protocol

from molq.resources import JobSpec, ResourceManager


class ScriptGenerator(Protocol):
    """Protocol for generating submission scripts.
    
    Script generators are responsible for creating scheduler-specific
    submission scripts from JobSpec objects.
    """

    def generate(self, spec: JobSpec, output_path: Path) -> Path:
        """Generate a submission script.
        
        Args:
            spec: Job specification
            output_path: Path where the script should be written
            
        Returns:
            Path to the generated script
        """
        ...


class SlurmScriptGenerator:
    """Generates SLURM batch submission scripts."""

    def generate(self, spec: JobSpec, output_path: Path) -> Path:
        """Generate a SLURM batch script.
        
        Args:
            spec: Job specification
            output_path: Path where the script should be written
            
        Returns:
            Path to the generated script
        """
        # Map JobSpec to SLURM parameters
        mapped_params = ResourceManager.map_to_scheduler(spec, "slurm")
        
        # Extract command
        cmd = spec.execution.cmd
        if isinstance(cmd, str):
            cmd = [cmd]
        
        # Write script
        with open(output_path, "w") as f:
            f.write("#!/bin/bash\n")
            
            # Write SBATCH directives
            for key, value in mapped_params.items():
                key_clean = key.lstrip("-")
                if value == "":  # Flag without value (e.g., --exclusive)
                    f.write(f"#SBATCH --{key_clean}\n")
                else:
                    f.write(f"#SBATCH --{key_clean}={value}\n")
            
            f.write("\n")
            f.write("\n".join(cmd))
        
        return output_path


class BashScriptGenerator:
    """Generates simple bash scripts for local execution."""

    def generate(self, spec: JobSpec, output_path: Path) -> Path:
        """Generate a bash script.
        
        Args:
            spec: Job specification
            output_path: Path where the script should be written
            
        Returns:
            Path to the generated script
        """
        # Extract command
        cmd = spec.execution.cmd
        if isinstance(cmd, str):
            cmd = [cmd]
        
        # Check for conda environment
        conda_env = spec.execution.extra.get("conda_env")
        
        # Write script
        with open(output_path, "w") as f:
            f.write("#!/bin/bash\n")
            
            if conda_env:
                f.write("source $(conda info --base)/etc/profile.d/conda.sh\n")
                f.write(f"conda activate {conda_env}\n")
            
            f.write("\n")
            f.write(" ".join(cmd))
        
        return output_path
