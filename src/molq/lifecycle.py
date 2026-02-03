"""Job lifecycle management for molq.

This module provides the JobLifecycleManager class for handling job persistence
and status tracking independently of scheduler-specific submission logic.
"""

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from molq.jobstatus import JobStatus
from molq.resources import JobSpec


class JobLifecycleManager:
    """Manages job persistence and lifecycle tracking.
    
    This class handles all database operations for job tracking, including
    registration, status updates, and querying. It is independent of
    scheduler-specific logic and can be used with any scheduler adapter.
    """

    def __init__(self, db_path: Path | None = None):
        """Initialize the lifecycle manager.
        
        Args:
            db_path: Path to SQLite database. Defaults to ~/.molq/jobs.db
        """
        if db_path is None:
            molq_dir = Path.home() / ".molq"
            molq_dir.mkdir(exist_ok=True)
            db_path = molq_dir / "jobs.db"
        
        self.db_path = Path(db_path)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the SQLite job database schema."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
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
        
        # Ensure all columns exist (migration support)
        c.execute("PRAGMA table_info(jobs)")
        columns = [column[1] for column in c.fetchall()]
        if "command" not in columns:
            c.execute("ALTER TABLE jobs ADD COLUMN command TEXT")
        if "work_dir" not in columns:
            c.execute("ALTER TABLE jobs ADD COLUMN work_dir TEXT")
        
        conn.commit()
        conn.close()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a database connection."""
        return sqlite3.connect(self.db_path)

    def register_job(
        self,
        section: str,
        job_id: int,
        spec: JobSpec,
        submit_time: float,
        status: JobStatus.Status = JobStatus.Status.PENDING,
    ) -> None:
        """Register a new job in the database.
        
        Args:
            section: Section/cluster name for the job
            job_id: Scheduler-assigned job ID
            spec: Job specification
            submit_time: Unix timestamp of submission
            status: Initial job status
        """
        conn = self._get_conn()
        c = conn.cursor()
        
        # Extract command from spec
        cmd = spec.execution.cmd
        if isinstance(cmd, list):
            cmd = " ".join(cmd)
        
        # Extract work directory
        work_dir = spec.execution.workdir or str(Path.cwd())
        
        # Job name
        name = spec.execution.job_name or f"job_{job_id}"
        
        c.execute(
            """INSERT INTO jobs 
            (section, job_id, name, status, command, work_dir, submit_time, end_time, extra_info) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                section,
                job_id,
                name,
                status.name,
                cmd,
                work_dir,
                submit_time,
                None,
                json.dumps({}),
            ),
        )
        conn.commit()
        conn.close()

    def update_status(
        self,
        section: str,
        job_id: int,
        status: JobStatus.Status | None = None,
        end_time: float | None = None,
        extra_info: dict[str, Any] | None = None,
    ) -> None:
        """Update job status and metadata.
        
        Args:
            section: Section/cluster name
            job_id: Job ID to update
            status: New status (if changing)
            end_time: End timestamp (if job finished)
            extra_info: Additional metadata to store
        """
        conn = self._get_conn()
        c = conn.cursor()
        
        fields = []
        values = []
        
        if status is not None:
            fields.append("status = ?")
            values.append(status.name)
        
        if end_time is not None:
            fields.append("end_time = ?")
            values.append(end_time)
        
        if extra_info is not None:
            fields.append("extra_info = ?")
            values.append(json.dumps(extra_info))
        
        if not fields:
            conn.close()
            return
        
        sql = "UPDATE jobs SET " + ", ".join(fields) + " WHERE section = ? AND job_id = ?"
        values.extend([section, job_id])
        
        c.execute(sql, tuple(values))
        conn.commit()
        conn.close()

    def get_job(self, section: str, job_id: int) -> dict[str, Any] | None:
        """Get a single job record.
        
        Args:
            section: Section/cluster name
            job_id: Job ID to retrieve
            
        Returns:
            Job record as dict, or None if not found
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute(
            """SELECT section, job_id, name, status, command, work_dir, 
            submit_time, end_time, extra_info 
            FROM jobs WHERE section = ? AND job_id = ?""",
            (section, job_id),
        )
        
        row = c.fetchone()
        conn.close()
        
        if not row:
            return None
        
        job = {key: row[key] for key in row.keys()}
        job["extra_info"] = json.loads(job["extra_info"] or "{}")
        
        return job

    def list_jobs(
        self, section: str | None = None, include_completed: bool = False
    ) -> list[dict[str, Any]]:
        """List jobs from the database.
        
        Args:
            section: Filter by section (None for all sections)
            include_completed: Include completed jobs
            
        Returns:
            List of job records as dicts
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        query = """SELECT section, job_id, name, status, command, work_dir, 
                   submit_time, end_time, extra_info FROM jobs"""
        where_clauses = []
        params = []
        
        if section:
            where_clauses.append("section = ?")
            params.append(section)
        
        if not include_completed:
            where_clauses.append("status != ?")
            params.append(JobStatus.Status.COMPLETED.name)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        c.execute(query, tuple(params))
        rows = c.fetchall()
        conn.close()
        
        jobs = []
        for row in rows:
            job = {key: row[key] for key in row.keys()}
            # Format timestamps
            if job["submit_time"]:
                job["submit_time"] = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(job["submit_time"])
                )
            if job["end_time"]:
                job["end_time"] = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(job["end_time"])
                )
            job["extra_info"] = json.loads(job["extra_info"] or "{}")
            jobs.append(job)
        
        return jobs

    def remove_job(self, section: str, job_id: int) -> None:
        """Remove a job from the database.
        
        Args:
            section: Section/cluster name
            job_id: Job ID to remove
        """
        conn = self._get_conn()
        c = conn.cursor()
        c.execute("DELETE FROM jobs WHERE section = ? AND job_id = ?", (section, job_id))
        conn.commit()
        conn.close()

    def get_jobs(self, section: str | None = None) -> dict[int, JobStatus]:
        """Get jobs as JobStatus objects (for compatibility).
        
        Args:
            section: Filter by section
            
        Returns:
            Mapping of job_id to JobStatus
        """
        jobs = self.list_jobs(section=section, include_completed=False)
        result = {}
        
        for job in jobs:
            try:
                status_enum = JobStatus.Status[job["status"]]
                result[job["job_id"]] = JobStatus(
                    job_id=job["job_id"],
                    status=status_enum,
                    name=job["name"],
                )
            except (KeyError, ValueError):
                # Skip jobs with invalid status
                continue
        
        return result
