"""Scheduler adapter interfaces for molq.

This module defines the SchedulerAdapter protocol that all scheduler implementations
must follow, providing a minimal interface for job submission, querying, and cancellation.
"""

import getpass
import subprocess
from pathlib import Path
from typing import Protocol

from molq.resources import JobSpec
from molq.scripts import SlurmScriptGenerator, BashScriptGenerator
from molq.jobstatus import JobStatus


class SchedulerAdapter(Protocol):
    """Protocol for scheduler-specific job submission adapters.
    
    This protocol defines the minimal interface that all scheduler adapters
    must implement. Adapters handle the translation from JobSpec to
    scheduler-specific commands and manage interaction with the scheduler.
    """

    def submit(self, spec: JobSpec) -> int:
        """Submit a job to the scheduler.
        
        Args:
            spec: Job specification containing all job parameters
            
        Returns:
            Scheduler-assigned job ID
            
        Raises:
            RuntimeError: If submission fails
        """
        ...

    def query(self, job_id: int | None = None) -> dict[int, JobStatus]:
        """Query job status from the scheduler.
        
        Args:
            job_id: Specific job ID to query, or None for all jobs
            
        Returns:
            Mapping of job IDs to their current status
        """
        ...

    def cancel(self, job_id: int) -> None:
        """Cancel a running job.
        
        Args:
            job_id: Job ID to cancel
        """
        ...


class SlurmAdapter:
    """Adapter for SLURM workload manager."""

    def __init__(self):
        """Initialize the SLURM adapter."""
        self.script_generator = SlurmScriptGenerator()

    def submit(self, spec: JobSpec) -> int:
        """Submit a job to SLURM using sbatch.
        
        Args:
            spec: Job specification
            
        Returns:
            SLURM job ID
            
        Raises:
            RuntimeError: If submission fails
        """
        work_dir = Path(spec.execution.workdir) if spec.execution.workdir else Path.cwd()
        script_name = spec.execution.extra.get("script_name", "run_slurm.sh")
        test_only = spec.execution.extra.get("test_only", False)
        
        # Generate script
        script_path = work_dir / script_name
        self.script_generator.generate(spec, script_path)
        
        # Build sbatch command
        submit_cmd = ["sbatch", str(script_path.absolute()), "--parsable"]
        if test_only:
            submit_cmd.append("--test-only")
        
        try:
            proc = subprocess.run(submit_cmd, capture_output=True, check=True, text=True)
            
            # Parse job ID
            if test_only:
                try:
                    job_id = int(proc.stderr.split()[2])
                except (IndexError, ValueError):
                    raise ValueError(f"Could not parse job ID from sbatch --test-only: {proc.stderr}")
            else:
                try:
                    job_id = int(proc.stdout.strip())
                except ValueError:
                    raise ValueError(f"Could not parse job ID from sbatch output: {proc.stdout}")
            
            return job_id
            
        except subprocess.CalledProcessError as e:
            script_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"SLURM submission failed.\nStdout: {e.stdout}\nStderr: {e.stderr}"
            ) from e

    def query(self, job_id: int | None = None) -> dict[int, JobStatus]:
        """Query job status using squeue.
        
        Args:
            job_id: Specific job ID or None for all user jobs
            
        Returns:
            Mapping of job IDs to status
        """
        user = getpass.getuser()
        cmd_list = ["squeue", "-u", user, "-h", "-o", "%i %t %j %P %u %T %M %N %S"]
        
        if job_id:
            cmd_list = ["squeue", "-j", str(job_id), "-h", "-o", "%i %t %j %P %u %T %M %N %S"]
        
        try:
            proc = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
            out = proc.stdout.strip()
            
            result = {}
            if not out:
                return {}
            
            squeue_fields = ["JOBID", "ST", "NAME", "PARTITION", "USER", "STATE", "TIME", "NODES", "NODELIST", "START_TIME"]
            
            for line in out.split("\n"):
                if not line.strip():
                    continue
                
                parts = line.split(maxsplit=len(squeue_fields) - 1)
                status_data = dict(zip(squeue_fields, parts))
                
                jid = int(status_data["JOBID"])
                st = status_data["ST"]
                
                status_map = {
                    "R": JobStatus.Status.RUNNING,
                    "PD": JobStatus.Status.PENDING,
                    "CD": JobStatus.Status.COMPLETED,
                    "CG": JobStatus.Status.RUNNING,
                    "CA": JobStatus.Status.FAILED,
                    "F": JobStatus.Status.FAILED,
                }
                enum_status = status_map.get(st, JobStatus.Status.FAILED)
                
                result[jid] = JobStatus(
                    job_id=jid,
                    status=enum_status,
                    name=status_data.get("NAME", ""),
                )
            
            return result
            
        except Exception:
            return {}

    def cancel(self, job_id: int) -> None:
        """Cancel a SLURM job using scancel.
        
        Args:
            job_id: Job ID to cancel
        """
        subprocess.run(["scancel", str(job_id)], capture_output=True)


class LocalAdapter:
    """Adapter for local execution using bash."""

    def __init__(self):
        """Initialize the local adapter."""
        self.script_generator = BashScriptGenerator()

    def submit(self, spec: JobSpec) -> int:
        """Submit a job locally using bash.
        
        Args:
            spec: Job specification
            
        Returns:
            Process ID
            
        Raises:
            RuntimeError: If submission fails
        """
        cwd = spec.execution.workdir
        script_name = spec.execution.extra.get("script_name", "run_local.sh")
        quiet = spec.execution.extra.get("quiet", False)
        
        if cwd is None:
            script_path = Path(script_name)
        else:
            cwd = Path(cwd)
            cwd.mkdir(parents=True, exist_ok=True)
            script_path = cwd / script_name
        
        # Generate script
        self.script_generator.generate(spec, script_path)
        
        # Build bash command
        submit_cmd = ["bash", str(script_path.absolute())]
        spparams = {}
        
        if quiet:
            spparams |= {
                "stdin": subprocess.DEVNULL,
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
        
        # Start process
        proc = subprocess.Popen(submit_cmd, cwd=cwd, **spparams)
        job_id = int(proc.pid)
        
        # Record job ID for tracking
        self._record_local_job_id(job_id)
        
        return job_id

    @staticmethod
    def _record_local_job_id(job_id: int) -> None:
        """Record job_id to ~/.molq/local_jobs.json."""
        import json
        
        molq_dir = Path.home() / ".molq"
        molq_dir.mkdir(exist_ok=True)
        job_file = molq_dir / "local_jobs.json"
        
        jobs = []
        if job_file.exists():
            try:
                with open(job_file, "r") as f:
                    jobs = json.load(f)
            except Exception:
                pass
        
        if job_id not in jobs:
            jobs.append(job_id)
        
        with open(job_file, "w") as f:
            json.dump(jobs, f)

    def query(self, job_id: int | None = None) -> dict[int, JobStatus]:
        """Query job status using ps.
        
        Args:
            job_id: Specific job ID or None
            
        Returns:
            Mapping of job IDs to status
        """
        cmd = ["ps", "--no-headers"]
        
        if job_id:
            cmd.extend(["-p", str(job_id)])
        
        cmd.extend(["-o", "pid,user,stat"])
        
        try:
            proc = subprocess.run(cmd, capture_output=True, check=False)
            
            if proc.stderr:
                return {}
            
            out = proc.stdout.decode().strip()
            status = {}
            
            if out:
                lines = [line.split() for line in out.split("\n")]
                status_map = {
                    "S": JobStatus.Status.RUNNING,
                    "R": JobStatus.Status.RUNNING,
                    "D": JobStatus.Status.PENDING,
                    "Z": JobStatus.Status.COMPLETED,
                }
                
                status = {
                    int(line[0]): JobStatus(
                        int(line[0]),
                        status_map.get(line[2][0], JobStatus.Status.RUNNING),
                    )
                    for line in lines
                }
            
            return status
            
        except Exception:
            return {}

    def cancel(self, job_id: int) -> None:
        """Cancel a local job using kill.
        
        Args:
            job_id: Process ID to kill
        """
        subprocess.run(["kill", str(job_id)], capture_output=True)

