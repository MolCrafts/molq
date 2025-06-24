"""Submitter base classes used by ``molq.submit`` decorators."""

import enum
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


class JobStatus:
    """Lightweight representation of a job's state."""

    class Status(enum.Enum):
        PENDING = 1
        RUNNING = 2
        COMPLETED = 3
        FAILED = 4
        FINISHED = 5

    def __init__(self, job_id: int, status: Status, name: str = "", **others: str):
        """Create a :class:`JobStatus` instance.

        Parameters
        ----------
        job_id:
            Identifier returned by the scheduler.
        status:
            Initial job state.
        name:
            Optional human readable job name.
        others:
            Additional status fields.
        """
        self.name: str = name
        self.job_id: int = job_id
        self.status: JobStatus.Status = status

        self.others: dict[str, str] = others

    def __repr__(self):
        return f"<Job {self.name}({self.job_id}): {self.status}>"

    @property
    def is_finish(self) -> bool:
        return self.status in [
            JobStatus.Status.COMPLETED,
            JobStatus.Status.FAILED,
            JobStatus.Status.FINISHED,
        ]


class BaseSubmitor(ABC):
    """Abstract interface for cluster-specific submitters."""

    GLOBAL_JOB_POOL: dict[int, JobStatus] = dict()

    def __init__(self, cluster_name: str, cluster_config: dict = {}):
        """Initialize a submitter.

        Parameters
        ----------
        cluster_name:
            Name identifying the cluster.
        cluster_config:
            Arbitrary configuration passed to the concrete implementation.
        """
        self.cluster_name = cluster_name
        self.cluster_config = cluster_config
        # Track temporary files per job for cleanup
        self._temp_files_by_job: dict[int, list[str]] = {}

    def __repr__(self):
        """Return a concise textual representation."""
        return f"<{self.cluster_name} {self.__class__.__name__}>"

    def _track_temp_file(self, job_id: int, filepath: str) -> None:
        """Track a temporary file for a specific job."""
        if job_id not in self._temp_files_by_job:
            self._temp_files_by_job[job_id] = []
        if filepath not in self._temp_files_by_job[job_id]:
            self._temp_files_by_job[job_id].append(filepath)

    def _cleanup_temp_files_for_job(self, job_id: int) -> None:
        """Clean up temporary files for a specific job."""
        if job_id not in self._temp_files_by_job:
            return

        import os

        for filepath in self._temp_files_by_job[job_id]:
            try:
                if os.path.exists(filepath):
                    os.unlink(filepath)
                    print(f"Cleaned up temporary file: {filepath}")
            except Exception as e:
                print(f"Warning: Could not clean up {filepath}: {e}")

        # Remove job from tracking
        del self._temp_files_by_job[job_id]

    def _should_cleanup_temp_files(self, config: dict) -> bool:
        """Check if temporary files should be cleaned up based on config."""
        return config.get("cleanup_temp_files", True)

    def submit(self, config: dict):
        """Submit a job described by ``config``.

        The configuration dictionary is first validated, then dispatched to the
        local or remote submission routines.
        """
        config = self.validate_config(config)
        block = config.get("block", False)
        remote = config.get("remote", False)
        cleanup_enabled = self._should_cleanup_temp_files(config)

        if remote:
            job_id = self.remote_submit(**config)
        else:
            job_id = self.local_submit(**config)
        return self.after_submit(job_id, block, cleanup_enabled)

    def after_submit(self, job_id: int, block: bool, cleanup_enabled: bool = True):
        """Handle a newly submitted job."""
        self.query(job_id=job_id)
        if block:
            self.block_one_until_complete(job_id)
            # Clean up temp files after blocking job completes
            if cleanup_enabled:
                self._cleanup_temp_files_for_job(job_id)
        return job_id

    @abstractmethod
    def local_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd: str | Path | None = None,
        block: bool = False,
        **resource_kwargs,
    ) -> int:
        """Submit a job to the local execution environment.

        Args:
            job_name: Name for the job
            cmd: Command to execute
            cwd: Working directory (optional)
            block: Whether to wait for completion
            **resource_kwargs: Additional resource specifications

        Returns:
            Job identifier
        """
        pass  # pragma: no cover

    @abstractmethod
    def remote_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd: str | Path | None = None,
        block: bool = False,
        **resource_kwargs,
    ) -> int:
        """Submit a job to a remote cluster.

        Args:
            job_name: Name for the job
            cmd: Command to execute
            cwd: Working directory (optional)
            block: Whether to wait for completion
            **resource_kwargs: Additional resource specifications

        Returns:
            Job identifier
        """
        pass  # pragma: no cover

    @abstractmethod
    def query(self, job_id: int | None = None) -> dict[int, JobStatus]:
        """Query the status of ``job_id`` or all jobs."""
        pass  # pragma: no cover

    @abstractmethod
    def cancel(self, job_id: int) -> None:
        """Cancel a running job."""
        pass

    def validate_config(self, config: dict) -> dict:
        """Validate and normalize the user provided configuration."""
        # Provide default validation that can be overridden
        required_fields = ["job_name", "cmd"]
        for field in required_fields:
            if field not in config:
                raise ValueError(
                    f"Required field '{field}' missing from job configuration"
                )

        # Normalize cmd to list format
        if isinstance(config["cmd"], str):
            config["cmd"] = [config["cmd"]]

        return config

    def prepare_resource_spec(self, config: dict) -> dict:
        """Prepare and validate resource specifications.

        This method can be overridden by subclasses to handle
        scheduler-specific resource parameter conversion.
        """
        # Extract common ResourceSpec parameters
        resource_params = {}

        # Resource parameters that might be in config
        resource_keys = [
            "cpu_count",
            "memory",
            "time_limit",
            "queue",
            "partition",
            "gpu_count",
            "gpu_type",
            "email",
            "email_events",
            "priority",
            "exclusive_node",
            "node_count",
            "cpu_per_node",
            "memory_per_cpu",
            "account",
            "constraints",
            "licenses",
            "array_spec",
            "workdir",
        ]

        for key in resource_keys:
            if key in config:
                resource_params[key] = config[key]

        return resource_params

    def convert_unified_to_scheduler_params(self, config: dict) -> dict:
        """Convert unified ResourceSpec parameters to scheduler-specific format.

        This is a default implementation that can be overridden by subclasses
        to handle scheduler-specific parameter conversion.

        Args:
            config: Job configuration with potential unified parameters

        Returns:
            Configuration with scheduler-specific parameters
        """
        # Default implementation just passes through
        # Subclasses should override this for specific conversion logic
        return config

    def extract_core_params(self, config: dict) -> tuple[str, str | list[str], dict]:
        """Extract core job parameters from configuration.

        Returns:
            Tuple of (job_name, cmd, resource_params)
        """
        job_name = config.get("job_name", "unnamed_job")
        cmd = config.get("cmd", [])

        # Extract resource-related parameters
        resource_params = self.prepare_resource_spec(config)

        return job_name, cmd, resource_params

    def modify_node(self, node: Callable[..., Any]) -> Callable[..., Any]:
        """Allow submitters to adapt Hamilton nodes if needed."""
        return node

    @property
    def job_id_list(self):
        """List of tracked job identifiers."""
        return list(self.GLOBAL_JOB_POOL.keys())

    @property
    def jobs(self):
        """Snapshot of the job pool."""
        return self.GLOBAL_JOB_POOL.copy()

    def get_status(self, job_id: int) -> JobStatus | None:
        """Return the :class:`JobStatus` associated with ``job_id``."""
        return self.GLOBAL_JOB_POOL.get(job_id, None)

    def update_status(self, status: dict[int, JobStatus], verbose: bool = False):
        """Replace the internal job pool and optionally print it."""
        self.GLOBAL_JOB_POOL = status
        # self.GLOBAL_JOB_POOL = {k: v for k, v in self.GLOBAL_JOB_POOL.items() if not v.is_finish}
        if verbose:
            self.print_status()

    def monitor_all(
        self, interval: int = 60, verbose: bool = True, callback: Callable | None = None
    ):
        """Poll all jobs until completion."""
        while self.GLOBAL_JOB_POOL:
            self.refresh_status()
            time.sleep(interval)
            if callback:
                callback()

    def block_all_until_complete(self, interval: int = 2, verbose: bool = True):
        """Block until every tracked job finishes."""
        while self.GLOBAL_JOB_POOL:
            self.refresh_status()
            time.sleep(interval)

    def block_one_until_complete(
        self, job_id: int, interval: int = 2, verbose: bool = True
    ):
        """Block until ``job_id`` reaches a terminal state."""
        while True:
            self.refresh_status(verbose=False)
            jobstatus = self.get_status(job_id)
            if jobstatus is None or jobstatus.is_finish:
                break
            time.sleep(interval)

    def get_status_by_name(self, name: str):
        """Return the first job whose name has ``name`` as prefix."""
        for status in self.jobs.values():
            if name.startswith(status.name):
                return status
        return None

    def refresh_status(self, verbose: bool = True):
        """Refresh internal status cache from the scheduler."""
        status = self.query()
        self.update_status(status, verbose=verbose)

    def print_status(self):
        """print job status in a nice table

        Args:
            pool (dict[int, JobStatus]): job pool to be printed
        """
        for i, status in enumerate(self.jobs.values(), 1):
            print(f"{status} | {i}/{len(self.GLOBAL_JOB_POOL)} \r", flush=True)

    @staticmethod
    def print_jobs(jobs, verbose=False):
        """Pretty print a list of job dicts. If verbose, print all fields. Else, use rich table if available."""
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

    @staticmethod
    def _init_job_db():
        """Initialize the sqlite job database and create the jobs table if it does not exist."""
        import sqlite3
        from pathlib import Path

        molq_dir = Path.home() / ".molq"
        molq_dir.mkdir(exist_ok=True)
        db_path = molq_dir / "jobs.db"
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        # Add command and work_dir columns
        c.execute(
            """CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section TEXT,
            job_id INTEGER,
            name TEXT,
            status TEXT,
            command TEXT,
            work_dir TEXT,
            submit_time REAL,
            end_time REAL,
            extra_info TEXT
        )"""
        )
        # Check if columns exist before trying to add them to avoid errors if already present
        c.execute("PRAGMA table_info(jobs)")
        columns = [column[1] for column in c.fetchall()]
        if "command" not in columns:
            c.execute("ALTER TABLE jobs ADD COLUMN command TEXT")
        if "work_dir" not in columns:
            c.execute("ALTER TABLE jobs ADD COLUMN work_dir TEXT")
        conn.commit()
        conn.close()

    @staticmethod
    def _get_db_conn():
        """Get a connection to the sqlite job database."""
        import sqlite3
        from pathlib import Path

        db_path = Path.home() / ".molq" / "jobs.db"
        return sqlite3.connect(db_path)

    def register_job(
        self,
        section: str,
        job_id: int,
        name: str,
        status: JobStatus.Status,
        command: str,
        work_dir: str,
        submit_time: float,
        extra_info: Optional[dict] = None,
    ):
        """Register a new job in the database."""
        import json
        import time

        BaseSubmitor._init_job_db()  # Ensures table is up-to-date
        conn = BaseSubmitor._get_db_conn()
        c = conn.cursor()
        c.execute(
            "INSERT INTO jobs (section, job_id, name, status, command, work_dir, submit_time, end_time, extra_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                section,
                job_id,
                name,
                status.name,
                command,
                work_dir,
                submit_time,
                None,
                json.dumps(extra_info or {}),
            ),
        )
        conn.commit()
        conn.close()

    def update_job(
        self,
        section: str,
        job_id: int,
        status: Optional[JobStatus.Status] = None,
        end_time: Optional[float] = None,
        extra_info: Optional[dict] = None,
    ):
        """Update the status or information of a job in the database."""
        import json
        import time

        BaseSubmitor._init_job_db()  # Ensures table is up-to-date
        conn = BaseSubmitor._get_db_conn()
        c = conn.cursor()
        sql = "UPDATE jobs SET "
        fields = []
        values = []
        if status is not None:
            fields.append("status = ?")
            values.append(status.name)  # Store status as string (enum member name)
        if end_time is not None:
            fields.append("end_time = ?")
            values.append(end_time)
        if extra_info is not None:
            # Ensure existing extra_info is loaded, updated, then dumped
            # This requires fetching the current extra_info first if we want to merge
            # For simplicity now, this will overwrite extra_info.
            # A more robust update would fetch, merge, then save.
            fields.append("extra_info = ?")
            values.append(json.dumps(extra_info))

        if not fields:  # Nothing to update
            conn.close()
            return

        sql += ", ".join(fields) + " WHERE section = ? AND job_id = ?"
        values.extend([section, job_id])
        c.execute(sql, tuple(values))
        conn.commit()
        conn.close()

    def remove_job(self, section, job_id):
        """Remove a job from the database."""
        BaseSubmitor._init_job_db()
        conn = BaseSubmitor._get_db_conn()
        c = conn.cursor()
        c.execute(
            "DELETE FROM jobs WHERE section = ? AND job_id = ?", (section, job_id)
        )
        conn.commit()
        conn.close()

    @staticmethod
    def list_jobs(section=None, all_history=False):
        """Query jobs from the DB and return a list of dicts."""
        import json
        import sqlite3
        from pathlib import Path

        db_path = Path.home() / ".molq" / "jobs.db"
        if not db_path.exists():
            BaseSubmitor._init_job_db()

        # Re-check after potential init
        if not db_path.exists():  # Should not happen if _init_job_db worked
            return []

        conn = BaseSubmitor._get_db_conn()
        conn.row_factory = sqlite3.Row  # Access columns by name
        c = conn.cursor()

        # Ensure table structure is checked/updated by calling _init_job_db
        # This is slightly inefficient to call _init_job_db on every list_jobs,
        # but ensures schema migrations are handled gracefully if the table exists but is old.
        # A better approach would be a dedicated migration path.
        # For now, creating a dummy instance to call _init_job_db.
        # Consider moving _init_job_db to be a static method or a module-level function.
        # However, _init_job_db is an instance method.
        # Let's assume _init_job_db has been called appropriately elsewhere (e.g. Submitor init or CLI)
        BaseSubmitor._init_job_db()

        query = "SELECT section, job_id, name, status, command, work_dir, submit_time, end_time, extra_info FROM jobs"
        where_clauses = []
        params = []
        if section:
            where_clauses.append("section = ?")
            params.append(section)
        if not all_history:
            where_clauses.append("status != ?")
            params.append(
                JobStatus.Status.COMPLETED.name
            )  # Exclude completed jobs by default

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        c.execute(query, tuple(params))
        rows = c.fetchall()
        conn.close()

        # Convert rows to list of dicts
        jobs = []
        for row in rows:
            job = {key: row[key] for key in row.keys()}
            job["submit_time"] = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(job["submit_time"])
            )
            if job["end_time"] is not None:
                job["end_time"] = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(job["end_time"])
                )
            job["extra_info"] = json.loads(job["extra_info"] or "{}")
            jobs.append(job)

        return jobs

    def refresh_all_jobs(self, section: str):
        """Refresh status of all unfinished jobs in DB for this section."""
        BaseSubmitor._init_job_db()

        unfinished_jobs_from_db = self.list_jobs(section=section, all_history=False)

        for job_dict in unfinished_jobs_from_db:
            job_id_to_refresh = job_dict["job_id"]
            current_db_status_str = job_dict["status"]
            updated_job_status_obj = self.refresh_job_status(job_id_to_refresh)

            if updated_job_status_obj:
                new_status_enum = updated_job_status_obj.status
                new_status_str = new_status_enum.name

                if new_status_str != current_db_status_str:
                    import time

                    end_time = None
                    if new_status_enum in [
                        JobStatus.Status.COMPLETED,
                        JobStatus.Status.FAILED,
                        JobStatus.Status.FINISHED,
                    ]:
                        # Use end_time from others if present, else use current time
                        end_time_str = updated_job_status_obj.others.get("end_time")
                        if end_time_str:
                            try:
                                end_time = float(end_time_str)
                            except ValueError:
                                end_time = time.time()
                        else:
                            end_time = time.time()
                    extra_info_update = updated_job_status_obj.others
                    self.update_job(
                        section=section,
                        job_id=job_id_to_refresh,
                        status=new_status_enum,
                        end_time=end_time,
                        extra_info=extra_info_update,
                    )
