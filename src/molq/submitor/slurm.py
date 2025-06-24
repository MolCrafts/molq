# pragma: no cover - heavy interaction with SLURM
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .base import BaseSubmitor, JobStatus


class SlurmSubmitor(BaseSubmitor):
    """Submit jobs to a SLURM workload manager."""

    def local_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd: str | Path | None = None,
        block: bool = False,
        cleanup_temp_files: bool = True,
        # Unified resource specification parameters
        cpu_count: int | None = None,
        memory: str | None = None,
        time_limit: str | None = None,
        queue: str | None = None,
        account: str | None = None,
        gpu_count: int | None = None,
        gpu_type: str | None = None,
        email: str | None = None,
        email_events: List[str] | None = None,
        priority: str | None = None,
        exclusive_node: bool | None = None,
        script_name: str | Path = "run_slurm.sh",
        work_dir: Path | None = None,
        test_only: bool = False,
        **slurm_kwargs,
    ) -> int:
        """Create a SLURM script and submit it with ``sbatch``."""
        # Convert unified parameters to SLURM configuration
        submit_config = self._prepare_slurm_config(
            job_name=job_name,
            cpu_count=cpu_count,
            memory=memory,
            time_limit=time_limit,
            queue=queue,
            account=account,
            gpu_count=gpu_count,
            gpu_type=gpu_type,
            email=email,
            email_events=email_events,
            priority=priority,
            exclusive_node=exclusive_node,
            **slurm_kwargs,
        )

        # Set working directory
        if work_dir is None:
            work_dir = Path(cwd) if cwd else Path.cwd()

        # Ensure cmd is a list
        if isinstance(cmd, str):
            cmd = [cmd]

        script_path = self._gen_script(
            Path(work_dir) / script_name, cmd, **submit_config
        )

        submit_cmd = ["sbatch", str(script_path.absolute()), "--parsable"]

        if test_only:
            submit_cmd.append("--test-only")

        # The following line seems to be a bug, as script_name is already part of submit_cmd via script_path
        # submit_cmd.append(str(script_name))

        try:
            # Register the job with the database before submitting
            # This ensures the job is tracked even if submission is very fast or fails
            # A placeholder job_id (e.g., -1 or a temporary UUID) might be needed if
            # the actual job_id is only available after successful submission.
            # For now, we'll assume sbatch returns quickly enough.

            proc = subprocess.run(
                submit_cmd, capture_output=True, check=True, text=True
            )
            if cleanup_temp_files:
                script_path.unlink(missing_ok=True)

            # Get job ID first
            if test_only:
                # example output:
                # sbatch: Job 3676091 to start at 2024-04-26T20:02:12 using 256 processors on nodes nid001000 in partition main
                # Need to parse stderr for job ID in test_only mode
                try:
                    job_id_str = proc.stderr.split()[2]
                    job_id = int(job_id_str)
                except (IndexError, ValueError) as e:
                    raise ValueError(
                        f"Could not parse job ID from sbatch --test-only output: {proc.stderr}"
                    ) from e
            else:
                try:
                    job_id = int(proc.stdout.strip())
                except ValueError as e:
                    raise ValueError(
                        f"Could not parse job ID from sbatch output: {proc.stdout}"
                    ) from e

            # Track script file for cleanup
            self._track_temp_file(job_id, str(script_path))

            # Register job in the database
            self.register_job(
                job_id=job_id,
                name=job_name,
                status=JobStatus.Status.PENDING,  # Initial status
                command=" ".join(cmd) if isinstance(cmd, list) else cmd,
                work_dir=str(work_dir.resolve()),
                submit_time=time.time(),
                section=self.cluster_name,  # Use cluster_name as section
            )

        except subprocess.CalledProcessError as e:
            # If submission fails, clean up script immediately
            try:
                script_path.unlink()
            except:
                pass
            # Construct a more informative error message
            error_message = f"SLURM submission failed.\\n"
            error_message += f"Command: {' '.join(submit_cmd)}\\n"
            error_message += f"Return code: {e.returncode}\\n"
            error_message += f"Stdout: {e.stdout}\\n"
            error_message += f"Stderr: {e.stderr}"
            raise RuntimeError(error_message) from e
        except Exception as e:  # Catch other potential errors during parsing etc.
            try:
                script_path.unlink()
            except:
                pass
            raise e

        return job_id

    def remote_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd: str | Path | None = None,
        block: bool = False,
        cleanup_temp_files: bool = True,
        **resource_kwargs,
    ) -> int:
        """Submit a job to a remote SLURM cluster (not implemented yet)."""
        raise NotImplementedError("Remote SLURM submission not implemented yet")

    # public helper for tests
    def gen_script(self, script_path: str | Path, cmd: list[str], **kwargs) -> Path:
        """Public helper used in tests to generate a SLURM script."""
        return self._gen_script(Path(script_path), cmd, **kwargs)

    def _gen_script(self, script_path: Path, cmd: list[str], **kwargs) -> Path:
        """Write a SLURM submission script and return its path."""
        assert script_path.parent.exists(), f"{script_path.parent} does not exist"
        with open(script_path, "w") as f:
            f.write("#!/bin/bash\n")
            for key, value in kwargs.items():
                # Remove leading dashes for SBATCH key
                key_clean = key.lstrip("-")
                f.write(f"#SBATCH --{key_clean}={value}\n")
            f.write("\n")
            f.write("\n".join(cmd))
        return script_path

    def query(self, job_id: int | None = None) -> dict[int, JobStatus]:
        """Query the scheduler for job status using ``squeue``."""
        if job_id is None:
            # Query all jobs for the current user by default for efficiency
            # You might need to get the current username, e.g., using `getpass.getuser()`
            import getpass

            user = getpass.getuser()
            cmd_list = ["squeue", "-u", user, "-h", "-o", "%i %t %j %P %u %T %M %N %S"]
        else:
            cmd_list = [
                "squeue",
                "-j",
                str(job_id),
                "-h",
                "-o",
                "%i %t %j %P %u %T %M %N %S",
            ]

        try:
            proc = subprocess.run(
                cmd_list, capture_output=True, text=True, check=False
            )  # check=False to handle no jobs found
            out = proc.stdout.strip()

            if (
                proc.returncode != 0
                and "Invalid job id specified" not in proc.stderr
                and "slurm_load_jobs error: Invalid job id specified" not in proc.stderr
            ):
                # Handle other squeue errors, but allow "Invalid job id" as it means the job is not found (e.g. completed and purged)
                if (
                    job_id
                ):  # Only raise if a specific job_id was queried and not found due to other errors
                    pass  # Will be handled by the logic below returning no status or specific status
                # If querying all jobs and squeue fails for other reasons, it's an issue.
                # For now, we'll let it return an empty dict, but logging a warning might be good.

            result = {}
            if not out:  # No output means no jobs matching query
                if (
                    job_id
                ):  # If a specific job was queried and not found, assume it's completed or failed and purged.
                    # This is a heuristic. A more robust way would be to check sacct if available.
                    # For now, if squeue doesn't find it, we can't determine its final state easily without more info.
                    # We could mark it as UNKNOWN or try sacct.
                    # Let's assume for now it's not found, so refresh_job_status will handle it.
                    return {}  # Return empty, refresh_job_status will see no update.
                return {}

            # Expected squeue output format: job_id state name partition user status time nodes nodelist(reason) start_time
            # Example: 70420 PD myjob main myuser PENDING 0:00 1 (Resources) N/A
            # Example: 70421 R  another main myuser RUNNING 0:05 1 cnode01 N/A
            squeue_fields = [
                "JOBID",
                "ST",
                "NAME",
                "PARTITION",
                "USER",
                "STATE",
                "TIME",
                "NODES",
                "NODELIST(REASON)",
                "START_TIME",
            ]

            for line in out.split("\n"):
                if not line.strip():
                    continue

                parts = line.split(
                    maxsplit=len(squeue_fields) - 1
                )  # Maxsplit to handle spaces in NAME or NODELIST
                status_data = dict(zip(squeue_fields, parts))

                try:
                    current_job_id = int(status_data["JOBID"])
                    slurm_state = status_data["ST"]  # Short state code
                except (KeyError, ValueError) as e:
                    # Log a warning or handle malformed line
                    print(f"Warning: Could not parse squeue line: {line}. Error: {e}")
                    continue

                if slurm_state in ("R", "RUNNING"):
                    enum_status = JobStatus.Status.RUNNING
                elif slurm_state in ("PD", "PENDING"):
                    enum_status = JobStatus.Status.PENDING
                elif slurm_state in ("CD", "COMPLETED"):
                    enum_status = JobStatus.Status.COMPLETED
                elif slurm_state in (
                    "CG",
                    "COMPLETING",
                ):  # Completing is also a running state for our purpose
                    enum_status = JobStatus.Status.RUNNING
                elif slurm_state in ("CA", "CANCELLED"):
                    enum_status = JobStatus.Status.FAILED  # Treat cancelled as FAILED
                elif slurm_state in ("F", "FAILED"):
                    enum_status = JobStatus.Status.FAILED
                elif slurm_state in ("TO", "TIMEOUT"):
                    enum_status = JobStatus.Status.FAILED
                elif slurm_state in ("NF", "NODE_FAIL"):
                    enum_status = JobStatus.Status.FAILED
                elif slurm_state in ("PR", "PREEMPTED"):
                    enum_status = (
                        JobStatus.Status.FAILED
                    )  # Or PENDING if it will be requeued
                elif slurm_state in ("S", "SUSPENDED"):
                    enum_status = (
                        JobStatus.Status.PENDING
                    )  # Or a new SUSPENDED state if needed
                else:
                    enum_status = (
                        JobStatus.Status.FAILED
                    )  # Default to FAILED for unknown states

                job_status = JobStatus(
                    job_id=current_job_id,
                    status=enum_status,
                    name=status_data.get("NAME", ""),
                    # Add other relevant fields from squeue if needed
                    partition=status_data.get("PARTITION", ""),
                    user=status_data.get("USER", ""),
                    time_used=status_data.get("TIME", ""),  # squeue TIME is time used
                    nodes_alloc=status_data.get("NODES", "0"),
                    node_list=status_data.get("NODELIST(REASON)", ""),
                    start_time_actual=status_data.get("START_TIME", ""),
                )
                result[job_status.job_id] = job_status

            return result

        except subprocess.CalledProcessError as e:
            # This might happen if squeue command itself fails for reasons other than "Invalid job id"
            # which is now handled by check=False and checking proc.returncode
            error_message = f"squeue command failed.\\n"
            error_message += f"Command: {' '.join(cmd_list)}\\n"
            error_message += f"Return code: {e.returncode}\\n"
            error_message += f"Stdout: {e.stdout}\\n"
            error_message += f"Stderr: {e.stderr}"
            # Depending on the error, might return {} or raise
            print(f"Warning: {error_message}")  # Log warning
            return {}  # Return empty on squeue failure
        except Exception as e:
            # Catch any other unexpected errors during parsing
            if job_id:
                print(
                    f"Warning: Error parsing squeue output for job {job_id}: {e}. Output: {out}"
                )
            else:
                print(f"Warning: Error parsing squeue output: {e}. Output: {out}")
            return {}

    def refresh_job_status(self, job_id: int) -> Optional[JobStatus]:
        """Refresh the status of a single SLURM job."""
        statuses = self.query(job_id=job_id)
        if job_id in statuses:
            return statuses[job_id]
        else:
            # If squeue -j <job_id> returns nothing, the job is likely completed (and purged from active queue) or failed.
            # We need a way to get historical job data, typically `sacct`.
            # For now, if not in squeue, we can't update its status further from here.
            # The job will remain in its last known state in the DB unless sacct logic is added.
            # One option: if not in squeue, check DB. If it was RUNNING or PENDING, it might be COMPLETED.
            # This is a heuristic. A more robust solution involves `sacct`.

            # Attempt to use sacct for more definitive status of completed/failed jobs
            try:
                cmd_sacct = [
                    "sacct",
                    "-j",
                    str(job_id),
                    "--parsable2",
                    "--noheader",
                    "-o",
                    "JobID,State",
                ]
                proc_sacct = subprocess.run(
                    cmd_sacct, capture_output=True, text=True, check=True
                )
                out_sacct = proc_sacct.stdout.strip()

                if out_sacct:
                    lines = out_sacct.split("\\n")
                    # sacct can return multiple lines for a job (e.g., job steps)
                    # We are interested in the main job's final state.
                    # The first line usually refers to the batch job itself.
                    # Example: 12345|COMPLETED
                    # Example: 12345.batch|COMPLETED
                    # Example: 12345.extern|COMPLETED

                    final_state_line = ""
                    for line in lines:
                        if "|" in line:
                            parts = line.split("|")
                            sacct_job_id_full, sacct_state_str = parts[0], parts[1]
                            # We want the state of the base job ID, not sub-steps like .batch or .extern if possible
                            if str(job_id) == sacct_job_id_full.split(".")[0]:
                                final_state_line = line  # Take the first relevant line
                                break

                    if final_state_line:
                        parts = final_state_line.split("|")
                        sacct_job_id_full, sacct_state_str = parts[0], parts[1]

                        # Ensure we're looking at the status of the exact job_id or its primary record
                        if (
                            str(job_id) == sacct_job_id_full.split(".")[0]
                        ):  # Compare base job ID
                            sacct_state_str = sacct_state_str.strip()
                            # Map sacct states to JobStatus.Status
                            # Common sacct states: PENDING, RUNNING, SUSPENDED, COMPLETING, COMPLETED, FAILED, CANCELLED, TIMEOUT, NODE_FAIL, PREEMPTED, BOOT_FAIL, DEADLINE
                            if sacct_state_str == "COMPLETED":
                                return JobStatus(
                                    job_id=job_id,
                                    status=JobStatus.Status.COMPLETED,
                                    name=f"Job {job_id}",
                                )
                            elif sacct_state_str in (
                                "FAILED",
                                "CANCELLED",
                                "TIMEOUT",
                                "NODE_FAIL",
                                "PREEMPTED",
                                "BOOT_FAIL",
                                "DEADLINE",
                            ):
                                return JobStatus(
                                    job_id=job_id,
                                    status=JobStatus.Status.FAILED,
                                    name=f"Job {job_id}",
                                )
                            elif (
                                sacct_state_str == "RUNNING"
                            ):  # Should have been caught by squeue, but as a fallback
                                return JobStatus(
                                    job_id=job_id,
                                    status=JobStatus.Status.RUNNING,
                                    name=f"Job {job_id}",
                                )
                            elif (
                                sacct_state_str == "PENDING"
                            ):  # Should have been caught by squeue
                                return JobStatus(
                                    job_id=job_id,
                                    status=JobStatus.Status.PENDING,
                                    name=f"Job {job_id}",
                                )
                            # Add more mappings if necessary
                            # If state is still inconclusive from sacct (e.g. RUNNING but not in squeue - rare), then no update.
            except subprocess.CalledProcessError as e:
                # sacct command failed or job not found in sacct (e.g., very old job, or sacct disabled)
                print(
                    f"sacct query for job {job_id} failed or job not found: {e.stderr}"
                )
            except Exception as e:
                # Other errors during sacct processing
                print(f"Error processing sacct output for job {job_id}: {e}")

            # If job not in squeue and sacct didn't give a definitive terminal state, return None.
            # This means we can't determine its current status from Slurm.
            # The job's status in the DB will remain unchanged by this refresh attempt.
            return None

    def validate_config(self, config: dict) -> dict:
        """Validate the configuration before submission."""
        return super().validate_config(config)

    def cancel(self, job_id: int) -> None:
        """Cancel a submitted SLURM job."""
        cmd = ["scancel", str(job_id)]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to cancel job {job_id}: {proc.stderr.decode()}")

        # Clean up temp files when job is cancelled
        self._cleanup_temp_files_for_job(job_id)

    def _prepare_slurm_config(
        self,
        job_name: str,
        cpu_count: int | None = None,
        memory: str | None = None,
        time_limit: str | None = None,
        queue: str | None = None,
        account: str | None = None,
        gpu_count: int | None = None,
        gpu_type: str | None = None,
        email: str | None = None,
        email_events: List[str] | None = None,
        priority: str | None = None,
        exclusive_node: bool | None = None,
        **slurm_kwargs,
    ) -> Dict[str, Any]:
        """Convert unified resource parameters to SLURM configuration."""
        submit_config = slurm_kwargs.copy()
        submit_config["--job-name"] = job_name

        # Set CPU count
        if cpu_count:
            submit_config["--ntasks"] = cpu_count

        # Handle memory conversion
        if memory:
            submit_config["--mem"] = self._convert_memory_format(memory)

        # Handle time conversion
        if time_limit:
            submit_config["--time"] = self._convert_time_format(time_limit)

        # Set other parameters
        if queue:
            submit_config["--partition"] = queue
        if account:
            submit_config["--account"] = account

        # GPU resources
        if gpu_count:
            if gpu_type:
                submit_config["--gres"] = f"gpu:{gpu_type}:{gpu_count}"
            else:
                submit_config["--gres"] = f"gpu:{gpu_count}"

        # Email notifications
        if email:
            submit_config["--mail-user"] = email
        if email_events:
            # Convert email events to SLURM format
            mail_type = self._convert_email_events(email_events)
            if mail_type:
                submit_config["--mail-type"] = mail_type

        # Priority
        if priority:
            submit_config["--priority"] = self._convert_priority(priority)

        # Exclusive node
        if exclusive_node:
            submit_config["--exclusive"] = ""

        return submit_config

    def _convert_memory_format(self, memory: str) -> str:
        """Convert human-readable memory format to SLURM format."""
        try:
            from molq.resources import MemoryParser

            return MemoryParser.to_slurm_format(memory)
        except ImportError:
            # Fallback: assume it's already in correct format
            return memory

    def _convert_time_format(self, time_str: str) -> str:
        """Convert human-readable time format to SLURM format."""
        try:
            from molq.resources import TimeParser

            return TimeParser.to_slurm_format(time_str)
        except ImportError:
            # Fallback: assume it's already in correct format
            return time_str

    def _convert_email_events(self, events: List[str]) -> str:
        """Convert email events to SLURM mail-type format."""
        event_map = {"start": "BEGIN", "end": "END", "fail": "FAIL", "all": "ALL"}
        slurm_events = [event_map.get(event.lower(), event.upper()) for event in events]
        return ",".join(slurm_events)

    def _convert_priority(self, priority: str) -> str:
        """Convert priority level to SLURM numeric priority."""
        priority_map = {"low": "100", "normal": "500", "high": "1000"}
        return priority_map.get(priority.lower(), priority)
