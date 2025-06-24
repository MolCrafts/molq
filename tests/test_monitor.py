from molq.submitor import BaseSubmitor, JobStatus


class Dummy(BaseSubmitor):
    def __init__(self):
        super().__init__("d")
        self.calls = 0

    def local_submit(self, **kw):
        return 1

    def remote_submit(self):
        pass

    def query(self, job_id=None):
        self.calls += 1
        if self.calls == 1:
            return {1: JobStatus(1, JobStatus.Status.RUNNING)}
        return {}

    def cancel(self, job_id):
        return 0

    def validate_config(self, config):
        return config


def test_monitor_loop():
    d = Dummy()
    d.GLOBAL_JOB_POOL = {1: JobStatus(1, JobStatus.Status.RUNNING)}
    d.monitor_all(interval=0, verbose=False)
    assert d.calls >= 2
    assert d.GLOBAL_JOB_POOL == {}
