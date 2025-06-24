from molq.submitor.base import BaseSubmitor, JobStatus


class DummySubmitor(BaseSubmitor):
    def local_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd=None,
        block: bool = False,
        **kwargs,
    ):
        return 12345

    def remote_submit(
        self,
        job_name: str,
        cmd: str | list[str],
        cwd=None,
        block: bool = False,
        **kwargs,
    ):
        return 54321

    def query(self, *args, **kwargs):
        return {}

    def cancel(self, *args, **kwargs):
        return None

    def refresh_job_status(self, job_id):
        return None


def test_job_db_isolated(tmp_molq_home):
    # Use tmp_molq_home fixture which automatically sets HOME
    s = DummySubmitor("local")
    s._init_job_db()
    # Register job
    s.register_job(
        "local",
        1,
        "job1",
        JobStatus.Status.RUNNING,
        "echo 1",
        "/tmp",
        1234567890,
        {"foo": "bar"},
    )
    s.register_job(
        "slurm@mock",
        2,
        "job2",
        JobStatus.Status.PENDING,
        "echo 2",
        "/tmp",
        1234567890,
        {"bar": "baz"},
    )
    # List jobs
    jobs_local = s.list_jobs(section="local")
    jobs_slurm = s.list_jobs(section="slurm@mock")
    assert len(jobs_local) == 1
    assert jobs_local[0]["name"] == "job1"
    assert jobs_slurm[0]["status"] == "PENDING"
    # Update job
    s.update_job("local", 1, status=JobStatus.Status.COMPLETED, end_time=1234567890)
    jobs_local2 = s.list_jobs(section="local", all_history=True)
    assert jobs_local2[0]["status"] == "COMPLETED"
    # Remove job
    s.remove_job("local", 1)
    jobs_local3 = s.list_jobs(section="local", all_history=True)
    assert len(jobs_local3) == 0


def test_list_jobs_multi_section(tmp_molq_home):
    # Use tmp_molq_home fixture which automatically sets HOME
    s = DummySubmitor("local")
    s._init_job_db()
    s.register_job(
        "local", 1, "job1", JobStatus.Status.RUNNING, "echo 1", "/tmp", 1234567890
    )
    s.register_job(
        "slurm@mock", 2, "job2", JobStatus.Status.RUNNING, "echo 2", "/tmp", 1234567890
    )
    all_jobs = s.list_jobs(all_history=True)
    sections = set(j["section"] for j in all_jobs)
    assert "local" in sections
    assert "slurm@mock" in sections
