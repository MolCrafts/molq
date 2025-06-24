from molq.submitor import BaseSubmitor, JobStatus


class Dummy(BaseSubmitor):
    def local_submit(self, **kw):
        return 1

    def remote_submit(self):
        pass

    def query(self, job_id=None):
        return {1: JobStatus(1, JobStatus.Status.COMPLETED)}

    def cancel(self, job_id):
        return 0

    def validate_config(self, config):
        return config


def test_print_status(capsys):
    d = Dummy("d")
    d.GLOBAL_JOB_POOL = {1: JobStatus(1, JobStatus.Status.COMPLETED)}
    d.print_status()
    captured = capsys.readouterr()
    assert "Job" in captured.out
