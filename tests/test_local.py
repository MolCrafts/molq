import subprocess

import pytest

from molq.submitor import JobStatus, LocalSubmitor


class DummyPopen:
    def __init__(self, pid=42):
        self.pid = pid
        self.returncode = 0

    def wait(self):
        self.returncode = 0


class DummyRun:
    def __init__(self, stdout=b"", stderr=b""):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = 0


@pytest.fixture
def tmp_script(tmp_path):
    return tmp_path / "run.sh"


def test_local_submit(tmp_script, monkeypatch):
    popen_rec = {}

    def fake_popen(cmd, cwd=None, **kwargs):
        popen_rec["cmd"] = cmd
        popen_rec["cwd"] = cwd
        return DummyPopen(123)

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    submitor = LocalSubmitor("local")
    job_id = submitor.local_submit(
        "job", ["echo", "ok"], script_name=tmp_script, block=True
    )
    assert job_id == 123
    assert popen_rec["cmd"][0] == "bash"
    assert tmp_script.exists()


def test_query_and_cancel(monkeypatch):
    out = b"123 user R\n"

    def fake_run(cmd, capture_output=True):
        if cmd[0] == "kill":
            return DummyRun()
        return DummyRun(stdout=out)

    monkeypatch.setattr(subprocess, "run", fake_run)

    submitor = LocalSubmitor("local")
    status = submitor.query(123)
    assert 123 in status
    assert status[123].status == JobStatus.Status.RUNNING
    # cancel() now returns None instead of return code
    submitor.cancel(123)  # Should not raise exception

    # call query without job_id
    submitor.query(None)


def test_gen_script(tmp_path):
    sub = LocalSubmitor("x")
    path = sub._gen_script(tmp_path / "s.sh", ["echo hi"], conda_env=None)
    with open(path) as f:
        content = f.read()
    assert "echo hi" in content


def test_monitoring(monkeypatch):
    seq = [
        {1: JobStatus(1, JobStatus.Status.RUNNING)},
        {1: JobStatus(1, JobStatus.Status.COMPLETED)},
    ]

    def fake_query(job_id=None):
        return seq.pop(0)

    submitor = LocalSubmitor("local")
    submitor.query = fake_query
    submitor.GLOBAL_JOB_POOL = {1: JobStatus(1, JobStatus.Status.RUNNING)}
    submitor.block_one_until_complete(1, interval=0)
    assert submitor.GLOBAL_JOB_POOL[1].status == JobStatus.Status.COMPLETED
