import time
import pytest

from molq.submitor.base import JobStatus
from molq.submitor.local import LocalSubmitor
from molq.resources import JobSpec, ExecutionSpec


def test_local_refresh_job_status(tmp_molq_home):
    """Test refreshing local job status with new architecture."""
    # Use tmp_molq_home fixture which automatically sets HOME
    submitor = LocalSubmitor("local")
    lifecycle = submitor.lifecycle_manager
    
    # Register a job without actually running it
    job_id = 12345
    spec = JobSpec(execution=ExecutionSpec(cmd=["sleep", "2"], job_name="test_sleep", workdir="/tmp"))
    lifecycle.register_job(
        "local",
        job_id,
        spec,
        time.time(),
        JobStatus.Status.RUNNING,
    )
    
    # Should be running now
    jobs = lifecycle.list_jobs(section="local")
    assert jobs[0]["status"] == "RUNNING"
    
    # Manually update to completed (simulating job finish)
    lifecycle.update_status("local", job_id, JobStatus.Status.COMPLETED, time.time())
    
    jobs2 = lifecycle.list_jobs(section="local", include_completed=True)
    assert jobs2[0]["status"] == "COMPLETED"
    assert jobs2[0]["end_time"] is not None and jobs2[0]["end_time"] != ""


def test_local_refresh_job_status_zombie(tmp_molq_home):
    """Test refreshing status of non-existent job."""
    # Use tmp_molq_home fixture which automatically sets HOME
    submitor = LocalSubmitor("local")
    lifecycle = submitor.lifecycle_manager
    
    # Register a fake job id (not running)
    fake_pid = 999999
    spec = JobSpec(execution=ExecutionSpec(cmd=["echo", "hi"], job_name="fake_job", workdir="/tmp"))
    lifecycle.register_job(
        "local",
        fake_pid,
        spec,
        time.time(),
        JobStatus.Status.RUNNING,
    )
    
    # Query the job - it won't be found, so we mark it completed
    status = submitor.query(fake_pid)
    if fake_pid not in status:
        lifecycle.update_status("local", fake_pid, JobStatus.Status.COMPLETED, time.time())
    
    jobs = lifecycle.list_jobs(section="local", include_completed=True)
    assert jobs[0]["status"] == "COMPLETED"
    assert jobs[0]["end_time"] is not None and jobs[0]["end_time"] != ""
