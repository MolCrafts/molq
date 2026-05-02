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
from collections.abc import Sequence
from pathlib import Path

from molq.errors import StoreError
from molq.models import (
    DependencyPreview,
    DependencyPreviewItem,
    JobDependency,
    JobRecord,
    JobSpec,
    StatusTransition,
)
from molq.status import JobState

_SCHEMA_VERSION = "7"

_CREATE_META = """
CREATE TABLE IF NOT EXISTS molq_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

# v3: dropped UNIQUE(cluster_name, scheduler_job_id).  job_id (UUID) already
# guarantees row identity, and OS-level PID reuse used to make the constraint
# fire spuriously when the LocalScheduler reused a freed PID.
_CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    cluster_name TEXT NOT NULL,
    scheduler TEXT NOT NULL,
    root_job_id TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    previous_attempt_job_id TEXT,
    retry_group_id TEXT,
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
    request_json TEXT DEFAULT '{}',
    profile_name TEXT,
    cleaned_at REAL
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

_CREATE_IDX_ROOT_ATTEMPT = """
CREATE INDEX IF NOT EXISTS idx_jobs_root_attempt
ON jobs(root_job_id, attempt)
"""

_CREATE_IDX_RETRY_GROUP = """
CREATE INDEX IF NOT EXISTS idx_jobs_retry_group
ON jobs(retry_group_id)
"""

_CREATE_IDX_TRANSITIONS = """
CREATE INDEX IF NOT EXISTS idx_transitions_job
ON status_transitions(job_id)
"""

_CREATE_DEPENDENCIES = """
CREATE TABLE IF NOT EXISTS job_dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    dependency_job_id TEXT NOT NULL REFERENCES jobs(job_id),
    dependency_type TEXT NOT NULL,
    scheduler_dependency TEXT NOT NULL
)
"""

_CREATE_IDX_DEPENDENCIES = """
CREATE INDEX IF NOT EXISTS idx_job_dependencies_job
ON job_dependencies(job_id)
"""


class JobStore:
    """SQLite-backed job persistence with WAL mode.

    Args:
        db_path: Path to database file. Use ':memory:' for testing.
                 Defaults to ~/.molq/jobs.db.
    """

    # Always set after __init__; close() flips it to None as an escape hatch
    # so __del__ can be idempotent.  The type annotation captures the
    # normal-operation invariant — calls after close() raise via SQLite's
    # own "Cannot operate on a closed database" error.
    _conn: sqlite3.Connection

    def __init__(self, db_path: Path | str | None = None) -> None:
        if db_path is None:
            molq_dir = Path.home() / ".molq"
            molq_dir.mkdir(exist_ok=True)
            db_path = molq_dir / "jobs.db"

        self.db_path = Path(db_path) if db_path != ":memory:" else db_path
        self._write_lock = threading.RLock()
        self._conn = self._open_connection()
        with self._write_lock:
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
                if version in {"2", "3", "4", "5", "6"}:
                    self._migrate_from_known_version(version)
                    return
                raise StoreError(f"Unknown schema version {version!r}; cannot migrate.")
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
                "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
            ).fetchone()
            return row is not None
        except sqlite3.OperationalError:
            return False

    def _migrate_from_v1(self) -> None:
        """Back up v1 database and create fresh v3 schema."""
        self._conn.close()

        if isinstance(self.db_path, Path):
            backup_path = self.db_path.with_suffix(".db.v1.bak")
            self.db_path.rename(backup_path)
            print(
                f"molq: migrated database to v{_SCHEMA_VERSION}, "
                f"old data backed up to {backup_path}",
                file=sys.stderr,
            )

        self._conn = self._open_connection()
        self._create_schema()

    def _migrate_from_known_version(self, version: str) -> None:
        if version == "2":
            self._migrate_v2_to_v7()
            return
        if version in {"3", "4", "5", "6"}:
            self._migrate_v3_to_v7()
            return
        raise StoreError(f"Unknown schema version {version!r}; cannot migrate.")

    def _migrate_v2_to_v7(self) -> None:
        """Migrate the v2 jobs table directly to the current schema.

        SQLite cannot drop table constraints in place, so we recreate the
        ``jobs`` table without the constraint and copy rows over.  The whole
        operation runs inside a single transaction so concurrent readers
        always observe a consistent snapshot.
        """
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("ALTER TABLE jobs RENAME TO _jobs_v2_old")
                self._conn.execute(_CREATE_META)
                self._conn.execute(_CREATE_JOBS)
                self._conn.execute(_CREATE_TRANSITIONS)
                self._conn.execute(_CREATE_DEPENDENCIES)
                self._conn.execute(
                    "INSERT INTO jobs ("
                    "job_id, cluster_name, scheduler, root_job_id, attempt, "
                    "previous_attempt_job_id, retry_group_id, scheduler_job_id, "
                    "state, command_type, command_display, cwd, "
                    "submitted_at, started_at, finished_at, last_polled, "
                    "exit_code, failure_reason, metadata, request_json, profile_name, cleaned_at) "
                    "SELECT job_id, cluster_name, scheduler, job_id, 1, "
                    "NULL, job_id, scheduler_job_id, "
                    "state, command_type, command_display, cwd, "
                    "submitted_at, started_at, finished_at, last_polled, "
                    "exit_code, failure_reason, metadata, '{}', NULL, NULL "
                    "FROM _jobs_v2_old"
                )
                self._conn.execute("DROP TABLE _jobs_v2_old")
                self._conn.execute(_CREATE_IDX_CLUSTER_STATE)
                self._conn.execute(_CREATE_IDX_TRANSITIONS)
                self._conn.execute(_CREATE_IDX_ROOT_ATTEMPT)
                self._conn.execute(_CREATE_IDX_RETRY_GROUP)
                self._conn.execute(_CREATE_IDX_DEPENDENCIES)
                self._conn.execute(
                    "INSERT OR REPLACE INTO molq_meta (key, value) VALUES (?, ?)",
                    ("schema_version", _SCHEMA_VERSION),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _migrate_v3_to_v7(self) -> None:
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                columns = {
                    row["name"]
                    for row in self._conn.execute("PRAGMA table_info(jobs)").fetchall()
                }
                if "root_job_id" not in columns:
                    self._conn.execute(
                        "ALTER TABLE jobs ADD COLUMN root_job_id TEXT NOT NULL DEFAULT ''"
                    )
                if "attempt" not in columns:
                    self._conn.execute(
                        "ALTER TABLE jobs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1"
                    )
                if "previous_attempt_job_id" not in columns:
                    self._conn.execute(
                        "ALTER TABLE jobs ADD COLUMN previous_attempt_job_id TEXT"
                    )
                if "retry_group_id" not in columns:
                    self._conn.execute(
                        "ALTER TABLE jobs ADD COLUMN retry_group_id TEXT"
                    )
                if "request_json" not in columns:
                    self._conn.execute(
                        "ALTER TABLE jobs ADD COLUMN request_json TEXT DEFAULT '{}'"
                    )
                if "profile_name" not in columns:
                    self._conn.execute("ALTER TABLE jobs ADD COLUMN profile_name TEXT")
                if "cleaned_at" not in columns:
                    self._conn.execute("ALTER TABLE jobs ADD COLUMN cleaned_at REAL")

                self._conn.execute(
                    "UPDATE jobs SET root_job_id = job_id WHERE root_job_id = '' OR root_job_id IS NULL"
                )
                self._conn.execute(
                    "UPDATE jobs SET retry_group_id = root_job_id WHERE retry_group_id IS NULL"
                )
                self._conn.execute(_CREATE_DEPENDENCIES)
                self._conn.execute(_CREATE_IDX_CLUSTER_STATE)
                self._conn.execute(_CREATE_IDX_TRANSITIONS)
                self._conn.execute(_CREATE_IDX_ROOT_ATTEMPT)
                self._conn.execute(_CREATE_IDX_RETRY_GROUP)
                self._conn.execute(_CREATE_IDX_DEPENDENCIES)
                self._conn.execute(
                    "INSERT OR REPLACE INTO molq_meta (key, value) VALUES (?, ?)",
                    ("schema_version", _SCHEMA_VERSION),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _create_schema(self) -> None:
        """Create all tables and indexes for the current schema."""
        with self._write_lock:
            self._conn.execute(_CREATE_META)
            self._conn.execute(
                "INSERT OR REPLACE INTO molq_meta (key, value) VALUES (?, ?)",
                ("schema_version", _SCHEMA_VERSION),
            )
            self._conn.execute(_CREATE_JOBS)
            self._conn.execute(_CREATE_TRANSITIONS)
            self._conn.execute(_CREATE_DEPENDENCIES)
            self._conn.execute(_CREATE_IDX_CLUSTER_STATE)
            self._conn.execute(_CREATE_IDX_TRANSITIONS)
            self._conn.execute(_CREATE_IDX_ROOT_ATTEMPT)
            self._conn.execute(_CREATE_IDX_RETRY_GROUP)
            self._conn.execute(_CREATE_IDX_DEPENDENCIES)
            self._conn.commit()

    def compare_and_update_state(
        self,
        job_id: str,
        expected_state: JobState,
        new_state: JobState,
        *,
        started_at: float | None = None,
        finished_at: float | None = None,
        last_polled: float | None = None,
        exit_code: int | None = None,
        failure_reason: str | None = None,
        cleaned_at: float | None = None,
    ) -> bool:
        """Atomically update state iff current state matches ``expected_state``.

        Returns True if the row was updated, False if the precondition failed.
        """
        fields: list[str] = ["state = ?"]
        values: list[object] = [new_state.value]

        extras = {
            "started_at": started_at,
            "finished_at": finished_at,
            "last_polled": last_polled,
            "exit_code": exit_code,
            "failure_reason": failure_reason,
            "cleaned_at": cleaned_at,
        }
        for col, val in extras.items():
            if val is not None:
                fields.append(f"{col} = ?")
                values.append(val)

        values.extend([job_id, expected_state.value])
        sql = f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ? AND state = ?"

        with self._write_lock:
            cur = self._conn.execute(sql, tuple(values))
            self._conn.commit()
            return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def insert_job(self, spec: JobSpec) -> None:
        """Insert a new job record from a JobSpec."""
        now = time.time()
        with self._write_lock:
            self._conn.execute(
                """INSERT INTO jobs
                (job_id, cluster_name, scheduler, root_job_id, attempt,
                 previous_attempt_job_id, retry_group_id, state,
                 command_type, command_display, cwd,
                 submitted_at, metadata, request_json, profile_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    spec.job_id,
                    spec.cluster_name,
                    spec.scheduler,
                    spec.root_job_id or spec.job_id,
                    spec.attempt,
                    spec.previous_attempt_job_id,
                    spec.retry_group_id or spec.root_job_id or spec.job_id,
                    JobState.CREATED.value,
                    spec.command.command_type,
                    spec.command.display,
                    spec.cwd,
                    now,
                    json.dumps(spec.metadata),
                    spec.request_json,
                    spec.profile_name,
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
        cleaned_at: float | None = None,
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
            "cleaned_at": cleaned_at,
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

    def add_dependencies(
        self,
        job_id: str,
        dependencies: list[JobDependency],
    ) -> None:
        if not dependencies:
            return
        with self._write_lock:
            self._conn.executemany(
                """INSERT INTO job_dependencies
                (job_id, dependency_job_id, dependency_type, scheduler_dependency)
                VALUES (?, ?, ?, ?)""",
                [
                    (
                        dep.job_id,
                        dep.dependency_job_id,
                        dep.dependency_type,
                        dep.scheduler_dependency,
                    )
                    for dep in dependencies
                ],
            )
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
                "SELECT * FROM jobs WHERE cluster_name = ? ORDER BY submitted_at DESC",
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

    def get_transitions(self, job_id: str) -> list[StatusTransition]:
        """Return the persisted transition timeline for a job."""
        rows = self._conn.execute(
            "SELECT job_id, old_state, new_state, timestamp, reason "
            "FROM status_transitions WHERE job_id = ? "
            "ORDER BY timestamp ASC, id ASC",
            (job_id,),
        ).fetchall()
        return [
            StatusTransition(
                job_id=row["job_id"],
                old_state=JobState(row["old_state"]) if row["old_state"] else None,
                new_state=JobState(row["new_state"]),
                timestamp=row["timestamp"],
                reason=row["reason"],
            )
            for row in rows
        ]

    def get_dependencies(self, job_id: str) -> list[JobDependency]:
        rows = self._conn.execute(
            "SELECT job_id, dependency_job_id, dependency_type, scheduler_dependency "
            "FROM job_dependencies WHERE job_id = ? ORDER BY id ASC",
            (job_id,),
        ).fetchall()
        return [
            JobDependency(
                job_id=row["job_id"],
                dependency_job_id=row["dependency_job_id"],
                dependency_type=row["dependency_type"],
                scheduler_dependency=row["scheduler_dependency"],
            )
            for row in rows
        ]

    def get_dependents(self, job_id: str) -> list[JobDependency]:
        rows = self._conn.execute(
            "SELECT job_id, dependency_job_id, dependency_type, scheduler_dependency "
            "FROM job_dependencies WHERE dependency_job_id = ? ORDER BY id ASC",
            (job_id,),
        ).fetchall()
        return [
            JobDependency(
                job_id=row["job_id"],
                dependency_job_id=row["dependency_job_id"],
                dependency_type=row["dependency_type"],
                scheduler_dependency=row["scheduler_dependency"],
            )
            for row in rows
        ]

    def get_dependency_previews(
        self,
        job_ids: Sequence[str],
        *,
        max_items: int = 8,
    ) -> dict[str, DependencyPreview]:
        unique_job_ids = tuple(dict.fromkeys(job_ids))
        if not unique_job_ids:
            return {}

        placeholders = ",".join("?" for _ in unique_job_ids)
        owner_rows = self._conn.execute(
            f"SELECT job_id, state, started_at FROM jobs WHERE job_id IN ({placeholders})",
            unique_job_ids,
        ).fetchall()
        owner_state_map = {
            row["job_id"]: (
                _coerce_job_state(row["state"]),
                row["started_at"],
            )
            for row in owner_rows
        }

        upstream_total = {job_id: 0 for job_id in unique_job_ids}
        upstream_satisfied = {job_id: 0 for job_id in unique_job_ids}
        upstream_items = {job_id: [] for job_id in unique_job_ids}
        downstream_total = {job_id: 0 for job_id in unique_job_ids}
        downstream_items = {job_id: [] for job_id in unique_job_ids}

        upstream_rows = self._conn.execute(
            "SELECT d.job_id, d.dependency_job_id, d.dependency_type, d.scheduler_dependency, "
            "u.state AS related_state, u.started_at AS related_started_at, "
            "u.command_display AS related_command_display "
            f"FROM job_dependencies d JOIN jobs u ON u.job_id = d.dependency_job_id "
            f"WHERE d.job_id IN ({placeholders}) ORDER BY d.id ASC",
            unique_job_ids,
        ).fetchall()

        for row in upstream_rows:
            owner_job_id = row["job_id"]
            related_state = _coerce_job_state(row["related_state"])
            relation_state = dependency_relation_state(
                row["dependency_type"],
                related_state,
                row["related_started_at"],
            )
            upstream_total[owner_job_id] += 1
            if relation_state == "satisfied":
                upstream_satisfied[owner_job_id] += 1
            if len(upstream_items[owner_job_id]) < max_items:
                upstream_items[owner_job_id].append(
                    DependencyPreviewItem(
                        job_id=row["dependency_job_id"],
                        dependency_type=row["dependency_type"],
                        relation_state=relation_state,
                        job_state=related_state,
                        command_display=row["related_command_display"] or "",
                        scheduler_dependency=row["scheduler_dependency"],
                    )
                )

        downstream_rows = self._conn.execute(
            "SELECT d.dependency_job_id, d.job_id AS dependent_job_id, "
            "d.dependency_type, d.scheduler_dependency, "
            "j.state AS related_state, j.command_display AS related_command_display "
            f"FROM job_dependencies d JOIN jobs j ON j.job_id = d.job_id "
            f"WHERE d.dependency_job_id IN ({placeholders}) ORDER BY d.id ASC",
            unique_job_ids,
        ).fetchall()

        for row in downstream_rows:
            owner_job_id = row["dependency_job_id"]
            owner_state, owner_started_at = owner_state_map.get(
                owner_job_id, (JobState.LOST, None)
            )
            relation_state = dependency_relation_state(
                row["dependency_type"],
                owner_state,
                owner_started_at,
            )
            downstream_total[owner_job_id] += 1
            if len(downstream_items[owner_job_id]) < max_items:
                downstream_items[owner_job_id].append(
                    DependencyPreviewItem(
                        job_id=row["dependent_job_id"],
                        dependency_type=row["dependency_type"],
                        relation_state=relation_state,
                        job_state=_coerce_job_state(row["related_state"]),
                        command_display=row["related_command_display"] or "",
                        scheduler_dependency=row["scheduler_dependency"],
                    )
                )

        return {
            job_id: DependencyPreview(
                job_id=job_id,
                upstream_total=upstream_total[job_id],
                upstream_satisfied=upstream_satisfied[job_id],
                upstream=tuple(upstream_items[job_id]),
                downstream_total=downstream_total[job_id],
                downstream=tuple(downstream_items[job_id]),
            )
            for job_id in unique_job_ids
        }

    def get_retry_family(self, job_id: str) -> list[JobRecord]:
        record = self.get_record(job_id)
        if record is None:
            return []
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE root_job_id = ? ORDER BY attempt ASC, submitted_at ASC",
            (record.root_job_id or record.job_id,),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_latest_attempt_record(self, job_id: str) -> JobRecord | None:
        record = self.get_record(job_id)
        if record is None:
            return None
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE root_job_id = ? "
            "ORDER BY attempt DESC, submitted_at DESC LIMIT 1",
            (record.root_job_id or record.job_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def get_request_json(self, job_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT request_json FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        return row["request_json"]

    def list_cleanup_candidates(
        self,
        cluster_name: str,
        *,
        job_dir_cutoff: float,
        record_cutoff: float,
        include_failed_job_dirs: bool,
    ) -> tuple[list[JobRecord], list[JobRecord]]:
        terminal = tuple(s.value for s in JobState if s.is_terminal)
        placeholders = ",".join("?" for _ in terminal)
        rows = self._conn.execute(
            f"SELECT * FROM jobs WHERE cluster_name = ? "
            f"AND state IN ({placeholders}) ORDER BY finished_at ASC",
            (cluster_name, *terminal),
        ).fetchall()
        records = [self._row_to_record(row) for row in rows]
        artifact_candidates: list[JobRecord] = []
        record_candidates: list[JobRecord] = []
        for record in records:
            finished_at = record.finished_at or 0.0
            if finished_at <= 0:
                continue
            if (
                record.cleaned_at is None
                and finished_at <= job_dir_cutoff
                and (
                    include_failed_job_dirs
                    or record.state
                    not in {JobState.FAILED, JobState.TIMED_OUT, JobState.LOST}
                )
            ):
                artifact_candidates.append(record)
            if finished_at <= record_cutoff:
                record_candidates.append(record)
        return artifact_candidates, record_candidates

    def delete_terminal_records(self, job_ids: list[str]) -> None:
        if not job_ids:
            return
        placeholders = ",".join("?" for _ in job_ids)
        with self._write_lock:
            self._conn.execute(
                f"DELETE FROM job_dependencies WHERE job_id IN ({placeholders}) "
                f"OR dependency_job_id IN ({placeholders})",
                tuple(job_ids) + tuple(job_ids),
            )
            self._conn.execute(
                f"DELETE FROM status_transitions WHERE job_id IN ({placeholders})",
                tuple(job_ids),
            )
            self._conn.execute(
                f"DELETE FROM jobs WHERE job_id IN ({placeholders})",
                tuple(job_ids),
            )
            self._conn.commit()

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
            root_job_id=row["root_job_id"] or row["job_id"],
            attempt=row["attempt"] or 1,
            previous_attempt_job_id=row["previous_attempt_job_id"],
            retry_group_id=row["retry_group_id"],
            profile_name=row["profile_name"],
            cleaned_at=row["cleaned_at"],
        )

    def close(self) -> None:
        """Close the database connection.  Idempotent."""
        conn = getattr(self, "_conn", None)
        if conn is None:
            return
        try:
            conn.close()
        finally:
            self._conn = None  # ty: ignore[invalid-assignment]

    def __del__(self) -> None:
        # Finalizer guard: if the user forgot to call close(), at least
        # silence the sqlite3 ResourceWarning instead of leaking the FD.
        # Module-level state may already be torn down at interpreter
        # shutdown, so swallow everything.
        try:
            self.close()
        except Exception:
            pass


def _coerce_job_state(value: str | None) -> JobState:
    try:
        return JobState(value) if value is not None else JobState.LOST
    except ValueError:
        return JobState.LOST


def dependency_relation_state(
    dependency_type: str,
    related_state: JobState,
    related_started_at: float | None,
) -> str:
    """Evaluate whether a single dependency edge is satisfied, pending, or impossible.

    Args:
        dependency_type: One of the canonical ``DependencyCondition`` values
            (``"after_success"``, ``"after_failure"``, ``"after_started"``,
            ``"after"``).
        related_state: Current ``JobState`` of the upstream job.
        related_started_at: Unix timestamp of when the upstream job started
            executing, or ``None`` if it has not started yet.

    Returns:
        ``"satisfied"`` — the condition is already met.
        ``"pending"``   — the upstream job has not reached the required state.
        ``"impossible"`` — the upstream job reached a terminal state that can
            never satisfy the condition (e.g. ``after_success`` on a failed job).

    Raises:
        ValueError: If *dependency_type* is not a recognised condition name.
    """
    if dependency_type == "after_success":
        if related_state == JobState.SUCCEEDED:
            return "satisfied"
        if related_state.is_terminal:
            return "impossible"
        return "pending"

    if dependency_type == "after_failure":
        if related_state in {
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.TIMED_OUT,
            JobState.LOST,
        }:
            return "satisfied"
        if related_state == JobState.SUCCEEDED:
            return "impossible"
        return "pending"

    if dependency_type == "after_started":
        if related_started_at is not None or related_state in {
            JobState.RUNNING,
            JobState.SUCCEEDED,
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.TIMED_OUT,
            JobState.LOST,
        }:
            return "satisfied"
        return "pending"

    if dependency_type == "after":
        return "satisfied" if related_state.is_terminal else "pending"

    raise ValueError(
        f"Unknown dependency condition {dependency_type!r}. "
        "Valid values: 'after_success', 'after_failure', 'after_started', 'after'."
    )
