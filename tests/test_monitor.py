from molq.submitor import LocalSubmitor, JobStatus


def test_monitor_loop():
    """Test monitoring loop with new architecture."""
    submitor = LocalSubmitor("test")
    
    calls = [0]
    original_query = submitor.query
    
    def counting_query(job_id=None):
        calls[0] += 1
        if calls[0] == 1:
            return {1: JobStatus(1, JobStatus.Status.RUNNING)}
        return {}
    
    submitor.query = counting_query
    
    # Set up lifecycle manager to return the job initially
    submitor.lifecycle_manager.get_jobs = lambda section: {1: JobStatus(1, JobStatus.Status.RUNNING)} if calls[0] == 0 else {}
    
    submitor.block_all_until_complete(interval=0, verbose=False)
    
    assert calls[0] >= 2
