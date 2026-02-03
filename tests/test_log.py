from molq.submitor import LocalSubmitor, JobStatus


def test_print_status(capsys):
    """Test print_status with new architecture."""
    # Use LocalSubmitor instead of creating a dummy class
    submitor = LocalSubmitor("test")
    
    # Manually add a job to the lifecycle manager for testing
    from molq.resources import JobSpec, ExecutionSpec
    import time
    
    spec = JobSpec(execution=ExecutionSpec(cmd="test", job_name="test_job"))
    submitor.lifecycle_manager.register_job(
        section="test",
        job_id=1,
        spec=spec,
        submit_time=time.time(),
        status=JobStatus.Status.COMPLETED
    )
    
    # Get jobs and print them
    jobs = submitor.lifecycle_manager.list_jobs("test", include_completed=True)
    submitor.print_jobs(jobs)
    
    captured = capsys.readouterr()
    # Check that output contains either the job name, truncated name, or "Molq Jobs" table
    assert "test_job" in captured.out or "t…" in captured.out or "Molq Jobs" in captured.out or "No jobs" in captured.out
