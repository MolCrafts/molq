# pragma: no cover - CLI interaction is hard to test fully
import subprocess
import time
from pathlib import Path

from .base import BaseSubmitor, JobStatus


class LocalSubmitor(BaseSubmitor):
    """Execute jobs on the current machine using ``bash``."""

    def local_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd: str | Path | None = None,
        script_name: str | Path = "run_local.sh",
        conda_env: str | None = None,
        quiet: bool = False,
        block: bool = False,
        cleanup_temp_files: bool = True,
        # Unified ResourceSpec parameters (mostly ignored for local execution)
        cpu_count: int | None = None,
        memory: str | None = None,
        time_limit: str | None = None,
        queue: str | None = None,
        gpu_count: int | None = None,
        gpu_type: str | None = None,
        email: str | None = None,
        email_events: list | None = None,
        priority: str | None = None,
        **kwargs,
    ) -> int:
        """Run ``cmd`` locally by generating and executing a shell script."""

        if isinstance(cmd, str):
            cmd = [cmd]

        if cwd is None:
            script_path = Path(script_name)
        else:
            cwd = Path(cwd)
            if not cwd.exists():
                cwd.mkdir(parents=True, exist_ok=True)
            script_path = cwd / script_name
        script_path = self._gen_script(script_path, cmd, conda_env, **kwargs)

        submit_cmd = ["bash", str(script_path.absolute())]
        spparams = {}
        if quiet:
            spparams["stdin"] = subprocess.DEVNULL
            spparams["stdout"] = subprocess.DEVNULL
            spparams["stderr"] = subprocess.DEVNULL

        proc = subprocess.Popen(
            submit_cmd,
            cwd=cwd,
            **spparams,
        )  # non-blocking

        job_id = int(proc.pid)
        self._track_temp_file(job_id, str(script_path))
        self._record_local_job_id(job_id)
        # Register to sqlite job database with new signature
        self.register_job(
            "local",
            job_id,
            job_name,
            JobStatus.Status.RUNNING,
            command=" ".join(cmd) if isinstance(cmd, list) else str(cmd),
            work_dir=str(cwd) if cwd is not None else str(Path.cwd()),
            submit_time=time.time(),
            extra_info={"cmd": cmd},
        )

        if block:
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(
                    f"Job {job_name} failed with return code {proc.returncode}"
                )

            # Don't clean up here - let BaseSubmitor.after_submit handle it
            # This ensures consistent cleanup behavior across all submitters

        return job_id

    @staticmethod
    def _record_local_job_id(job_id: int):
        """Record job_id to ~/.molq/local_jobs.json"""
        import json

        molq_dir = Path.home() / ".molq"
        molq_dir.mkdir(exist_ok=True)
        job_file = molq_dir / "local_jobs.json"
        if job_file.exists():
            try:
                with open(job_file, "r") as f:
                    jobs = json.load(f)
            except Exception:
                jobs = []
        else:
            jobs = []
        if job_id not in jobs:
            jobs.append(job_id)
        with open(job_file, "w") as f:
            json.dump(jobs, f)

    @staticmethod
    def _get_recorded_job_ids():
        """Read job_id list from ~/.molq/local_jobs.json"""
        import json

        job_file = Path.home() / ".molq" / "local_jobs.json"
        if job_file.exists():
            try:
                with open(job_file, "r") as f:
                    jobs = json.load(f)
                return [int(j) for j in jobs]
            except Exception:
                return []
        return []

    def remote_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd: str | Path | None = None,
        block: bool = False,
        cleanup_temp_files: bool = True,
        **resource_kwargs,
    ) -> int:
        """Local submitter doesn't support remote submission."""
        raise NotImplementedError("Local submitter doesn't support remote submission")

    def _gen_script(self, script_path, cmd: list[str], conda_env, **args) -> Path:
        """Generate a temporary shell script file for job execution.

        Creates a bash script that optionally activates a conda environment
        before executing the specified command.

        Args:
            script_path (Path): Path where the script file will be created
            cmd (list[str]): Command to be executed in the script
            conda_env (str): Name of conda environment to activate (optional)
            **args: Additional arguments (currently unused)

        Returns:
            Path: Path to the generated script file
        """
        with open(script_path, mode="w") as f:
            f.write("#!/bin/bash\n")

            if conda_env:
                f.write(f"source $(conda info --base)/etc/profile.d/conda.sh\n")
                f.write(f"conda activate {conda_env}\n")

            f.write("\n")
            f.write(" ".join(cmd))

        return script_path

    def query(
        self,
        job_id: int | None = None,
        job_ids: list[int] | None = None,
        auto_update: bool = True,
    ) -> dict[int, JobStatus]:  # pragma: no cover
        """Return a mapping of job IDs to statuses using ``ps``. If auto_update, update DB for finished jobs."""

        if job_ids is not None:
            results = {}
            for jid in job_ids:
                try:
                    res = self.query(job_id=jid, auto_update=auto_update)
                    results.update(res)
                except Exception:
                    pass
            return results

        cmd = [
            "ps",
            "--no-headers",
        ]
        if job_id:
            cmd.extend(["-p", str(job_id)])
        query_status = {
            "job_id": "pid",
            "user": "user",
            "status": "stat",
        }
        query_str = ",".join(query_status.values())
        cmd.extend(["-o", query_str])
        proc = subprocess.run(cmd, capture_output=True)
        if proc.stderr:
            raise RuntimeError(proc.stderr.decode())

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
                    int(line[0]), status_map.get(line[2][0], JobStatus.Status.RUNNING)
                )
                for line in lines
            }

        # If job_id was requested but not found, mark as COMPLETED in DB
        if auto_update and job_id and not status:
            try:
                self.update_job(
                    "local",
                    job_id,
                    status=JobStatus.Status.COMPLETED,
                    end_time=time.time(),
                )
            except Exception:
                pass

        return status

    def validate_config(self, config: dict) -> dict:
        """Fill in defaults for missing configuration values."""
        if "job_name" not in config:
            config["job_name"] = "local_job"
        return config

    def cancel(self, job_id: int) -> None:
        """Terminate a running process."""
        cmd = ["kill", str(job_id)]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to cancel job {job_id}: {proc.stderr.decode()}")

        # Clean up temp files when job is cancelled
        self._cleanup_temp_files_for_job(job_id)

    def refresh_job_status(self, job_id: int) -> JobStatus | None:
        """Refresh the status of a local job by checking if the process is still running."""
        import os
        import signal

        try:
            # Check if process exists and is running
            # Using os.kill with signal 0 to check if process exists without actually sending a signal
            os.kill(job_id, 0)
            # If we get here, process exists - return RUNNING status
            return JobStatus(job_id, JobStatus.Status.RUNNING, name=f"Job {job_id}")
        except OSError:
            # Process doesn't exist or we don't have permission - mark as COMPLETED
            return JobStatus(
                job_id,
                JobStatus.Status.COMPLETED,
                name=f"Job {job_id}",
                end_time=str(time.time()),
            )
