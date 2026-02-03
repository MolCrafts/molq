from molq.submitor import LocalSubmitor, JobStatus
from molq.lifecycle import JobLifecycleManager
from molq.resources import JobSpec, ExecutionSpec


def test_job_db_isolated(tmp_molq_home):
    """Test job database isolation with new architecture."""
    # Use tmp_molq_home fixture which automatically sets HOME
    submitor = LocalSubmitor("local")
    lifecycle = submitor.lifecycle_manager
    
    # Register jobs using lifecycle manager
    from molq.resources import JobSpec, ExecutionSpec
    import time
    
    spec1 = JobSpec(execution=ExecutionSpec(cmd="echo 1", job_name="job1", workdir="/tmp"))
    spec2 = JobSpec(execution=ExecutionSpec(cmd="echo 2", job_name="job2", workdir="/tmp"))
    
    lifecycle.register_job(
        "local",
        1,
        spec1,
        1234567890,
        JobStatus.Status.RUNNING,
    )
    lifecycle.register_job(
        "slurm@mock",
        2,
        spec2,
        1234567890,
        JobStatus.Status.PENDING,
    )
    
    # List jobs
    jobs_local = lifecycle.list_jobs(section="local")
    jobs_slurm = lifecycle.list_jobs(section="slurm@mock")
    assert len(jobs_local) == 1
    assert jobs_local[0]["name"] == "job1"
    assert jobs_slurm[0]["status"] == "PENDING"
    
    # Update job
    lifecycle.update_status("local", 1, status=JobStatus.Status.COMPLETED, end_time=1234567890)
    jobs_local2 = lifecycle.list_jobs(section="local", include_completed=True)
    assert jobs_local2[0]["status"] == "COMPLETED"
    
    # Remove job
    lifecycle.remove_job("local", 1)
    jobs_local3 = lifecycle.list_jobs(section="local", include_completed=True)
    assert len(jobs_local3) == 0


def test_list_jobs_multi_section(tmp_molq_home):
    """Test listing jobs across multiple sections."""
    # Use tmp_molq_home fixture which automatically sets HOME
    submitor = LocalSubmitor("local")
    lifecycle = submitor.lifecycle_manager
    
    spec1 = JobSpec(execution=ExecutionSpec(cmd="echo 1", job_name="job1", workdir="/tmp"))
    spec2 = JobSpec(execution=ExecutionSpec(cmd="echo 2", job_name="job2", workdir="/tmp"))
    
    lifecycle.register_job(
        "local", 1, spec1, 1234567890, JobStatus.Status.RUNNING
    )
    lifecycle.register_job(
        "slurm@mock", 2, spec2, 1234567890, JobStatus.Status.RUNNING
    )
    
    all_jobs = lifecycle.list_jobs(include_completed=True)
    sections = set(j["section"] for j in all_jobs)
    assert "local" in sections
    assert "slurm@mock" in sections
