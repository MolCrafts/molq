"""Breaking-release contracts, including the real no-service path."""

import json
import subprocess
import sys

import pytest

from molq import Execution, ExecutionUnit, JobSpec, Molq, MolqError, Resources
from molq.protocol import contract
from molq.protocol.types import JobsHistoryParams1, JobSnapshot


def test_no_service_submit_wait_restart_and_no_job_tables(tmp_path):
    registry = tmp_path / "registry.db"
    with Molq(registry=registry) as mq:
        mq.clusters.register(name="local", scheduler="shell")
        job = mq.cluster("local").submit(
            JobSpec.argv([sys.executable, "-c", 'print("molq09")'])
        )
        ref = job.ref
    with Molq(registry=registry) as mq:
        result = mq.job(ref).wait(timeout=10, interval=0.05)
        assert result["completion"]["all_confirmed_terminal"]
        assert result["snapshots"][0]["state"] == "succeeded"
        assert "molq09" in mq.job(ref).logs()["text"]
    import sqlite3

    with sqlite3.connect(registry) as db:
        tables = {
            r[0]
            for r in db.execute("select name from sqlite_master where type='table'")
        }
    assert tables == {"schema_meta", "clusters"}


def test_unknown_cluster_is_not_local(tmp_path):
    with Molq(registry=tmp_path / "registry.db") as mq:
        with pytest.raises(MolqError) as e:
            mq.cluster("missing").submit(JobSpec.argv(["true"]))
        assert e.value.kind == "CLUSTER_NOT_FOUND"


def test_single_job_and_multi_unit_schema_are_orthogonal():
    spec = JobSpec(
        execution=Execution(units=(ExecutionUnit.argv("main", ["echo", "ok"]),)),
        resources=Resources(nodes=2, tasks=4),
    )
    wire = spec.to_wire()
    assert wire["resources"]["nodes"] == 2
    assert wire["execution"]["units"][0]["command"]["argv"] == ["echo", "ok"]
    assert "scheduler" not in wire


def test_packaged_contract_loads():
    document = contract()
    assert "jobs.history" in document["methods"]
    assert "name" in JobsHistoryParams1.__annotations__
    assert JobSnapshot.__annotations__["state"]


def test_client_import_has_no_runtime_side_effects(tmp_path):
    code = 'import molq, sys; assert not any(x.startswith("molq.runtime") or x.startswith("molq.scheduler") or x.startswith("molq.registry") for x in sys.modules)'
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
        cwd=tmp_path,
    )
    assert proc.returncode == 0, proc.stderr


def test_cli_is_structured_on_input_error():
    proc = subprocess.run(
        [sys.executable, "-m", "molq", "rpc", "not.a.method"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode != 0
    assert json.loads(proc.stdout)["error"]["data"]["kind"] == "METHOD_NOT_FOUND"


def test_contract_and_runtime_have_no_independent_protocol_version(tmp_path):
    assert "protocol_version" not in contract()
    assert not contract()["$id"].endswith(("/v1", "/v2"))
    with Molq(registry=tmp_path / "registry.db") as mq:
        hello = mq.rpc("molq.hello")
        assert hello["release"] == "0.9.0"
        assert hello["registry_id"]
        assert not {"version", "protocol_version", "schema_version"} & hello.keys()


def test_job_directory_purge_is_a_mutation_with_retention_report(tmp_path):
    assert contract()["methods"]["jobs.purge"]["properties"].keys() == {"cluster"}
    with Molq(registry=tmp_path / "registry.db") as mq:
        mq.clusters.register(name="local", scheduler="shell")
        report = mq.rpc("jobs.purge")
    assert report["retention_days"] == 30
    assert report["partial_failure"] is False
    assert [r["location"] for r in report["results"]] == ["target"]
    assert report["results"][0]["removed"] == []
