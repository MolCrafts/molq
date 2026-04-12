"""Job persistence layer for molq.

Provides JobStore backed by SQLite with WAL mode, UUID-based job identity,
schema versioning, and automatic v1 migration.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

from molq.errors import JobNotFoundError, StoreError
from molq.models import JobRecord, JobSpec
from molq.status import JobState

_SCHEMA_VERSION = "2"

_CREATE_META = """
CREATE TABLE IF NOT EXISTS molq_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

_CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    cluster_name TEXT NOT NULL,
    scheduler TEXT NOT NULL,
    scheduler_job_id TEXT,
    state TEXT NOT NULL DEFAULT 'created',
    command_type TEXT NOT NULL,
    command_display TEXT NOT NULL,
    cwd TEXT NOT NULL,
    submitted_at REAL,
    started_at REAL,
    finished_at REAL,
    last_polled REAL,
    exit_code INTEGER,
    failure_reason TEXT,
    metadata TEXT DEFAULT '{}',
    UNIQUE(cluster_name, scheduler_job_id)
)
"""

_CREATE_TRANSITIONS = """
CREATE TABLE IF NOT EXISTS status_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    old_state TEXT,
    new_state TEXT NOT NULL,
    timestamp REAL NOT NULL,
    reason TEXT
)
"""

_CREATE_IDX_CLUSTER_STATE = """
CREATE INDEX IF NOT EXISTS idx_jobs_cluster_state
ON jobs(cluster_name, state)
"""

_CREATE_IDX_TRANSITIONS = """
CREATE INDEX IF NOT EXISTS idx_transitions_job
ON status_transitions(job_id)
"""


class JobStore:
    """SQLite-backed job persistence with WAL mode.

    Args:
        db_path: Path to database file. Use ':memory:' for testing.
                 Defaults to ~/.molq/jobs.db.
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        if db_path is None:
            molq_dir = Path.home() / ".molq"
            molq_dir.mkdir(exist_ok=True)
            db_path = molq_dir / "jobs.db"

        self.db_path = Path(db_path) if db_path != ":memory:" else db_path
        self._write_lock = threading.Lock()
        self._conn = self._open_connection()
        self._ensure_schema()

    def _open_connection(self) -> sqlite3.Connection:
        path = str(self.db_path)
        conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_schema(self) -> None:
        """Check schema version and create/migrate as needed."""
        try:
            row = self._conn.execute(
                "SELECT value FROM molq_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row:
                version = row["value"]
                if version == _SCHEMA_VERSION:
                    return
                if version > _SCHEMA_VERSION:
                    raise StoreError(
                        f"Database schema version {version} is newer than "
                        f"supported version {_SCHEMA_VERSION}. "
                        f"Please upgrade molq."
                    )
        except sqlite3.OperationalError:
            # molq_meta table does not exist
            if self._has_old_schema():
                self._migrate_from_v1()
                return

        # Fresh database or needs schema creation
        self._create_schema()

    def _has_old_schema(self) -> bool:
        """Check if this is a v1 database (has 'jobs' table but no 'molq_meta')."""
        try:
            row = self._conn.execute(
                "SELECT name FROM sqlite_master " "WHERE type='table' AND name='jobs'"
            ).fetchone()
            return row is not None
        except sqlite3.OperationalError:
            return False

    def _migrate_from_v1(self) -> None:
        """Back up v1 database and create fresh v2 schema."""
        self._conn.close()

        if isinstance(self.db_path, Path):
            backup_path = self.db_path.with_suffix(".db.v1.bak")
            self.db_path.rename(backup_path)
            print(
                f"molq: migrated database to v2, "
                f"old data backed up to {backup_path}",
                file=sys.stderr,
            )

        self._conn = self._open_connection()
        self._create_schema()

    def _create_schema(self) -> None:
        """Create all tables and indexes for the v2 schema."""
        with self._write_lock:
            self._conn.execute(_CREATE_META)
            self._conn.execute(
                "INSERT OR REPLACE INTO molq_meta (key, value) VALUES (?, ?)",
                ("schema_version", _SCHEMA_VERSION),
            )
            self._conn.execute(_CREATE_JOBS)
            self._conn.execute(_CREATE_TRANSITIONS)
            self._conn.execute(_CREATE_IDX_CLUSTER_STATE)
            self._conn.execute(_CREATE_IDX_TRANSITIONS)
            self._conn.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def insert_job(self, spec: JobSpec) -> None:
        """Insert a new job record from a JobSpec."""
        now = time.time()
        with self._write_lock:
            self._conn.execute(
                """INSERT INTO jobs
                (job_id, cluster_name, scheduler, state,
                 command_type, command_display, cwd,
                 submitted_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    spec.job_id,
                    spec.cluster_name,
                    spec.scheduler,
                    JobState.CREATED.value,
                    spec.command.command_type,
                    spec.command.display,
                    spec.cwd,
                    now,
                    json.dumps(spec.metadata),
                ),
            )
            self._conn.execute(
                """INSERT INTO status_transitions
                (job_id, old_state, new_state, timestamp, reason)
                VALUES (?, ?, ?, ?, ?)""",
                (spec.job_id, None, JobState.CREATED.value, now, "job created"),
            )
            self._conn.commit()

    def update_job(
        self,
        job_id: str,
        *,
        state: JobState | None = None,
        scheduler_job_id: str | None = None,
        submitted_at: float | None = None,
        started_at: float | None = None,
        finished_at: float | None = None,
        last_polled: float | None = None,
        exit_code: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        """Partial update of a job record."""
        fields: list[str] = []
        values: list[object] = []

        updates = {
            "state": state.value if state else None,
            "scheduler_job_id": scheduler_job_id,
            "submitted_at": submitted_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "last_polled": last_polled,
            "exit_code": exit_code,
            "failure_reason": failure_reason,
        }

        for col, val in updates.items():
            if val is not None:
                fields.append(f"{col} = ?")
                values.append(val)

        if not fields:
            return

        values.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ?"

        with self._write_lock:
            self._conn.execute(sql, tuple(values))
            self._conn.commit()

    def record_transition(
        self,
        job_id: str,
        old_state: JobState | None,
        new_state: JobState,
        timestamp: float,
        reason: str | None = None,
    ) -> None:
        """Record a status transition."""
        with self._write_lock:
            self._conn.execute(
                """INSERT INTO status_transitions
                (job_id, old_state, new_state, timestamp, reason)
                VALUES (?, ?, ?, ?, ?)""",
                (
                    job_id,
                    old_state.value if old_state else None,
                    new_state.value,
                    timestamp,
                    reason,
                ),
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_record(self, job_id: str) -> JobRecord | None:
        """Get a single job record by ID."""
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def list_records(
        self,
        cluster_name: str,
        include_terminal: bool = False,
    ) -> list[JobRecord]:
        """List job records for a cluster."""
        if include_terminal:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE cluster_name = ? "
                "ORDER BY submitted_at DESC",
                (cluster_name,),
            ).fetchall()
        else:
            terminal = tuple(s.value for s in JobState if s.is_terminal)
            placeholders = ",".join("?" for _ in terminal)
            rows = self._conn.execute(
                f"SELECT * FROM jobs WHERE cluster_name = ? "
                f"AND state NOT IN ({placeholders}) "
                f"ORDER BY submitted_at DESC",
                (cluster_name, *terminal),
            ).fetchall()

        return [self._row_to_record(row) for row in rows]

    def get_active_records(self, cluster_name: str) -> list[JobRecord]:
        """Get all non-terminal job records for a cluster."""
        return self.list_records(cluster_name, include_terminal=False)

    def list_all_records(
        self,
        include_terminal: bool = False,
        limit: int | None = None,
    ) -> list[JobRecord]:
        """List job records across **all** clusters, ordered by submission time.

        Args:
            include_terminal: When ``False`` (default), terminal states
                (succeeded, failed, cancelled, timed_out, lost) are excluded.
            limit: Cap the result set.  ``None`` returns all matching rows.

        Returns:
            List of :class:`JobRecord`, newest first.
        """
        if include_terminal:
            sql = "SELECT * FROM jobs ORDER BY submitted_at DESC"
            params: tuple = ()
        else:
            terminal = tuple(s.value for s in JobState if s.is_terminal)
            placeholders = ",".join("?" for _ in terminal)
            sql = (
                f"SELECT * FROM jobs WHERE state NOT IN ({placeholders}) "
                f"ORDER BY submitted_at DESC"
            )
            params = terminal

        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_record(row) for row in rows]

    def _row_to_record(self, row: sqlite3.Row) -> JobRecord:
        state_str = row["state"]
        try:
            state = JobState(state_str)
        except ValueError:
            state = JobState.LOST

        return JobRecord(
            job_id=row["job_id"],
            cluster_name=row["cluster_name"],
            scheduler=row["scheduler"],
            state=state,
            scheduler_job_id=row["scheduler_job_id"],
            submitted_at=row["submitted_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            exit_code=row["exit_code"],
            failure_reason=row["failure_reason"],
            cwd=row["cwd"],
            command_type=row["command_type"],
            command_display=row["command_display"],
            metadata=json.loads(row["metadata"] or "{}"),
        )

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
