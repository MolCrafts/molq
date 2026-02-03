"""Refactored submitter base classes using orthogonalized components.

This module provides the BaseSubmitor class that coordinates lifecycle management,
scheduler adapters, and temporary file management, along with the JobStatus class.
"""

import time
from pathlib import Path

from molq.adapters import SchedulerAdapter
from molq.jobstatus import JobStatus  # Re-export for convenience
from molq.lifecycle import JobLifecycleManager
from molq.resources import JobSpec, ExecutionSpec, ResourceSpec, ClusterSpec
from molq.tempfiles import TempFileManager

__all__ = ["JobStatus", "BaseSubmitor"]


class BaseSubmitor:
    """Coordinating submitter that delegates to specialized components.
    
    This class provides a unified interface for job submission while delegating
    to specialized components for lifecycle management, scheduler interaction,
    and temporary file cleanup.
    """

    def __init__(
        self,
        cluster_name: str,
        adapter: SchedulerAdapter,
        lifecycle_manager: JobLifecycleManager | None = None,
        temp_manager: TempFileManager | None = None,
        cluster_config: dict | None = None,
    ):
        """Initialize the submitter.
        
        Args:
            cluster_name: Name of the cluster/section for job tracking
            adapter: Scheduler adapter for job submission
            lifecycle_manager: Job lifecycle manager (creates default if None)
            temp_manager: Temporary file manager (creates default if None)
            cluster_config: Optional cluster configuration
        """
        self.cluster_name = cluster_name
        self.adapter = adapter
        self.lifecycle_manager = lifecycle_manager or JobLifecycleManager()
        self.temp_manager = temp_manager or TempFileManager()
        self.cluster_config = cluster_config or {}

    def __repr__(self):
        """Return a concise textual representation."""
        return f"<{self.cluster_name} {self.__class__.__name__}>"

    def submit(self, config: dict | JobSpec) -> int:
        """Submit a job.
        
        Args:
            config: Job configuration as dict or JobSpec
            
        Returns:
            Job ID assigned by the scheduler
        """
        # Convert dict to JobSpec if needed
        if isinstance(config, dict):
            # Handle legacy flat dict format
            if "execution" not in config:
                spec = self._convert_legacy_config(config)
            else:
                spec = JobSpec(**config)
        else:
            spec = config
        
        # Submit to scheduler
        job_id = self.adapter.submit(spec)
        
        # Register with lifecycle manager
        self.lifecycle_manager.register_job(
            section=self.cluster_name,
            job_id=job_id,
            spec=spec,
            submit_time=time.time(),
            status=JobStatus.Status.PENDING,
        )
        
        # Track script file if it was created and exists
        work_dir = spec.execution.workdir or "."
        script_name = spec.execution.extra.get("script_name", "run.sh")
        script_path = Path(work_dir) / script_name
        if script_path.exists() and script_path.is_file():
            self.temp_manager.track(job_id, script_path)
        
        # Handle blocking and cleanup
        return self._handle_post_submit(job_id, spec)

    def _convert_legacy_config(self, config: dict) -> JobSpec:
        """Convert legacy flat dict format to JobSpec.
        
        Args:
            config: Legacy configuration dictionary
            
        Returns:
            JobSpec instance
        """
        execution_kwargs = {}
        resource_kwargs = {}
        cluster_kwargs = {}
        extra_kwargs = {}
        
        core_execution_fields = {"cmd", "workdir", "cwd", "env", "block", "job_name", "cleanup_temp_files", "output_file", "error_file"}
        resource_fields = {"cpu_count", "memory", "gpu_count", "gpu_type", "time_limit"}
        cluster_fields = set(ClusterSpec.model_fields.keys()) | {"partition"}
        
        for k, v in config.items():
            if k in core_execution_fields:
                execution_kwargs[k] = v
            elif k in resource_fields:
                resource_kwargs[k] = v
            elif k in cluster_fields:
                cluster_kwargs[k] = v
            else:
                extra_kwargs[k] = v
        
        # Default block to True if not specified
        execution_kwargs.setdefault("block", True)
        
        return JobSpec(
            execution=ExecutionSpec(**execution_kwargs, extra=extra_kwargs),
            resources=ResourceSpec(**resource_kwargs),
            cluster=ClusterSpec(**cluster_kwargs)
        )

    def _handle_post_submit(self, job_id: int, spec: JobSpec) -> int:
        """Handle post-submission tasks like blocking and cleanup.
        
        Args:
            job_id: Job ID
            spec: Job specification
            
        Returns:
            Job ID
        """
        block = spec.execution.block
        cleanup_enabled = spec.execution.cleanup_temp_files
        
        if block:
            self.block_one_until_complete(job_id)
            if cleanup_enabled:
                self.temp_manager.cleanup(job_id)
        
        return job_id

    def query(self, job_id: int | None = None) -> dict[int, JobStatus]:
        """Query job status from the scheduler.
        
        Args:
            job_id: Specific job ID or None for all jobs
            
        Returns:
            Mapping of job IDs to status
        """
        return self.adapter.query(job_id)

    def cancel(self, job_id: int) -> None:
        """Cancel a running job.
        
        Args:
            job_id: Job ID to cancel
        """
        self.adapter.cancel(job_id)
        self.temp_manager.cleanup(job_id)

    def block_one_until_complete(
        self, job_id: int, interval: int = 2, verbose: bool = True
    ) -> None:
        """Block until a job reaches a terminal state.
        
        Args:
            job_id: Job ID to wait for
            interval: Polling interval in seconds
            verbose: Whether to print status updates
        """
        while True:
            statuses = self.query(job_id)
            jobstatus = statuses.get(job_id)
            
            if jobstatus is None or jobstatus.is_finish:
                break
            
            time.sleep(interval)

    def block_all_until_complete(self, interval: int = 2, verbose: bool = True) -> None:
        """Block until all tracked jobs finish.
        
        Args:
            interval: Polling interval in seconds
            verbose: Whether to print status updates
        """
        jobs = self.lifecycle_manager.get_jobs(self.cluster_name)
        
        while jobs:
            for job_id in list(jobs.keys()):
                statuses = self.query(job_id)
                if job_id in statuses and statuses[job_id].is_finish:
                    del jobs[job_id]
            
            if jobs:
                time.sleep(interval)

    @staticmethod
    def print_jobs(jobs: list[dict], verbose: bool = False) -> None:
        """Pretty print a list of job dicts."""
        if verbose:
            if not jobs:
                print("No jobs found.")
                return
            for job in jobs:
                print("-" * 40)
                for k, v in job.items():
                    print(f"{k}: {v}")
        else:
            try:
                from rich.console import Console
                from rich.table import Table

                table = Table(title="Molq Jobs")
                table.add_column("SECTION", style="cyan", no_wrap=True)
                table.add_column("JOB_ID", style="magenta")
                table.add_column("NAME", style="green")
                table.add_column("STATUS", style="yellow")
                table.add_column("SUBMIT_TIME", style="white")
                table.add_column("END_TIME", style="white")
                
                for job in jobs:
                    table.add_row(
                        str(job["section"]),
                        str(job["job_id"]),
                        str(job["name"]),
                        str(job["status"]),
                        str(job["submit_time"]) if job["submit_time"] else "",
                        str(job["end_time"]) if job["end_time"] else "",
                    )
                
                console = Console()
                if not jobs:
                    console.print("[bold red]No jobs found.[/bold red]")
                else:
                    console.print(table)
            except ImportError:
                if not jobs:
                    print("No jobs found.")
                    return
                print(f"{'SECTION':<15} {'JOB_ID':<10} {'NAME':<20} {'STATUS':<10}")
                print("-" * 60)
                for job in jobs:
                    print(
                        f"{job['section']:<15} {job['job_id']:<10} {job['name']:<20} {job['status']:<10}"
                    )
