import os
import signal
import sqlite3
import subprocess
import time

import pytest

from molq.submitor.base import JobStatus
from molq.submitor.local import LocalSubmitor


def test_local_refresh_job_status(tmp_molq_home):
    # Use tmp_molq_home fixture which automatically sets HOME
    s = LocalSubmitor("local")
    s._init_job_db()
    # Start a background sleep process
    proc = subprocess.Popen(["sleep", "2"])
    job_id = proc.pid
    s.register_job(
        "local",
        job_id,
        "test_sleep",
        JobStatus.Status.RUNNING,
        "sleep 2",
        "/tmp",
        time.time(),
        {"cmd": ["sleep", "2"]},
    )
    # Should be running now
    jobs = s.list_jobs(section="local")
    assert jobs[0]["status"] == "RUNNING"
    # Wait for process to finish
    proc.wait()
    # Refresh all jobs
    s.refresh_all_jobs("local")
    jobs2 = s.list_jobs(section="local", all_history=True)
    assert jobs2[0]["status"] == "COMPLETED"
    assert jobs2[0]["end_time"] is not None and jobs2[0]["end_time"] != ""


def test_local_refresh_job_status_zombie(tmp_molq_home):
    # Use tmp_molq_home fixture which automatically sets HOME
    s = LocalSubmitor("local")
    s._init_job_db()
    # Register a fake job id (not running)
    fake_pid = 999999
    s.register_job(
        "local",
        fake_pid,
        "fake_job",
        JobStatus.Status.RUNNING,
        "echo hi",
        "/tmp",
        time.time(),
        {"cmd": ["echo", "hi"]},
    )
    # Refresh all jobs
    s.refresh_all_jobs("local")
    jobs = s.list_jobs(section="local", all_history=True)
    assert jobs[0]["status"] == "COMPLETED"
    assert jobs[0]["end_time"] is not None and jobs[0]["end_time"] != ""
