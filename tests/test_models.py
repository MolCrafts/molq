"""Tests for molq.models — Command, JobSpec, JobRecord, SubmitorDefaults."""

import dataclasses

import pytest

from molq.errors import CommandError
from molq.models import Command, JobRecord, JobSpec, SubmitorDefaults
from molq.status import JobState
from molq.types import JobExecution, JobResources, JobScheduling, Memory, Script


class TestCommand:
    def test_argv(self):
        cmd = Command.from_submit_args(argv=["python", "train.py"])
        assert cmd.argv == ("python", "train.py")
        assert cmd.command is None
        assert cmd.script is None
        assert cmd.command_type == "argv"
        assert cmd.display == "python train.py"

    def test_command(self):
        cmd = Command.from_submit_args(command="python train.py && python eval.py")
        assert cmd.command == "python train.py && python eval.py"
        assert cmd.command_type == "command"

    def test_script_inline(self):
        s = Script.inline("echo hello")
        cmd = Command.from_submit_args(script=s)
        assert cmd.script is s
        assert cmd.command_type == "script"
        assert cmd.display == "script:inline"

    def test_script_path(self, tmp_path):
        f = tmp_path / "run.sh"
        f.write_text("#!/bin/bash")
        s = Script.path(f)
        cmd = Command.from_submit_args(script=s)
        assert "script:" in cmd.display

    def test_zero_provided_raises(self):
        with pytest.raises(CommandError, match="Exactly one"):
            Command.from_submit_args()

    def test_two_provided_raises(self):
        with pytest.raises(CommandError, match="Exactly one"):
            Command.from_submit_args(argv=["echo"], command="echo")

    def test_three_provided_raises(self):
        with pytest.raises(CommandError, match="Exactly one"):
            Command.from_submit_args(
                argv=["echo"],
                command="echo",
                script=Script.inline("echo"),
            )

    def test_command_with_newline_raises(self):
        with pytest.raises(CommandError, match="newline"):
            Command.from_submit_args(command="echo\nhello")


class TestJobSpec:
    def test_construction(self):
        cmd = Command.from_submit_args(argv=["echo", "hi"])
        spec = JobSpec(
            job_id="abc-123",
            cluster_name="dev",
            scheduler="local",
            command=cmd,
        )
        assert spec.job_id == "abc-123"
        assert spec.cluster_name == "dev"
        assert spec.scheduler == "local"
        assert spec.resources == JobResources()
        assert spec.metadata == {}

    def test_new_job_id_is_uuid(self):
        jid = JobSpec.new_job_id()
        assert len(jid) == 36  # UUID format: 8-4-4-4-12
        assert jid.count("-") == 4

    def test_unique_ids(self):
        ids = {JobSpec.new_job_id() for _ in range(100)}
        assert len(ids) == 100


class TestJobRecord:
    def test_construction(self):
        r = JobRecord(
            job_id="abc",
            cluster_name="dev",
            scheduler="local",
            state=JobState.RUNNING,
            scheduler_job_id="12345",
        )
        assert r.job_id == "abc"
        assert r.state == JobState.RUNNING
        assert r.scheduler_job_id == "12345"

    def test_defaults(self):
        r = JobRecord(
            job_id="x",
            cluster_name="c",
            scheduler="local",
            state=JobState.CREATED,
        )
        assert r.scheduler_job_id is None
        assert r.submitted_at is None
        assert r.exit_code is None
        assert r.metadata == {}

    def test_frozen(self):
        r = JobRecord(
            job_id="x",
            cluster_name="c",
            scheduler="local",
            state=JobState.CREATED,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.state = JobState.RUNNING  # type: ignore[misc]


class TestSubmitorDefaults:
    def test_all_none(self):
        d = SubmitorDefaults()
        assert d.resources is None
        assert d.scheduling is None
        assert d.execution is None

    def test_with_values(self):
        d = SubmitorDefaults(
            resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
            scheduling=JobScheduling(queue="normal"),
        )
        assert d.resources.cpu_count == 4
        assert d.scheduling.queue == "normal"
        assert d.execution is None

    def test_frozen(self):
        d = SubmitorDefaults()
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.resources = JobResources()  # type: ignore[misc]
