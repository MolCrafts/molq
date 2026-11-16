"""Decorator ergonomics over the same RPC handles, without implicit resend."""

import asyncio
import inspect
import os

import pytest

from molq import (
    AsyncMolq,
    Execution,
    ExecutionUnit,
    Job,
    JobSpec,
    Molq,
    MolqError,
    asubmit,
    submit,
)


@pytest.mark.skipif(os.name == "nt", reason="Local Shell execution requires POSIX")
def test_cluster_generator_submits_real_jobs_and_preserves_return(tmp_path):
    mq = Molq(registry=tmp_path / "registry.db")
    try:
        mq.clusters.register(name="local", scheduler="shell")
        local = mq.cluster("local", project_root=tmp_path)

        @local
        def hello(name: str = "molq"):
            """Submit two independently identified allocations."""
            first = yield JobSpec.argv(["echo", name])
            second = yield JobSpec(
                Execution((ExecutionUnit.argv("main", ["echo", "second"]),))
            )
            return first, second

        assert hello.__name__ == "hello"
        assert str(inspect.signature(hello)) == "(name: str = 'molq')"
        first, second = hello("decorated")
        assert isinstance(first, Job)
        assert first.ref != second.ref
        assert mq.jobs([first.ref, second.ref]).wait(timeout=10, interval=0.05)[
            "completion"
        ]["successful"]
        assert first.logs()["text"] == "decorated\n"
        assert second.logs()["text"] == "second\n"
        ref = first.ref
    finally:
        mq.close()
    with Molq(registry=tmp_path / "registry.db") as resumed:
        assert resumed.job(ref).status()["snapshots"][0]["state"] == "succeeded"


@pytest.mark.skipif(os.name == "nt", reason="Local Shell execution requires POSIX")
def test_async_decorator_awaits_submission_through_generated_client(tmp_path):
    async def scenario():
        mq = AsyncMolq(registry=tmp_path / "registry.db")
        try:
            await mq.clusters.register(name="local", scheduler="shell")
            local = mq.cluster("local", project_root=tmp_path)

            @local
            def hello(name):
                job = yield JobSpec.argv(["echo", name])
                return job

            job = await hello("async")
            assert (await job.wait(timeout=10, interval=0.05))["completion"][
                "successful"
            ]
            assert (await job.logs())["text"] == "async\n"

            @local
            def factory():
                job = yield JobSpec.argv(["echo", "factory"])
                return job

            other = await factory()
            assert (await other.wait(timeout=10, interval=0.05))["completion"][
                "successful"
            ]
        finally:
            await mq.close()

    asyncio.run(scenario())


def test_decoration_is_lazy_and_each_generator_call_uses_one_rpc(monkeypatch):
    mq = Molq()
    calls = []

    def rpc(method, **params):
        calls.append((method, params))
        return {"ref": {"native_id": str(len(calls))}}

    monkeypatch.setattr(mq, "rpc", rpc)

    @mq.cluster("registered", project_root="/project")
    def hello(name):
        job = yield JobSpec.argv(["echo", name])
        return job

    assert calls == []
    assert hello("first").ref != hello("second").ref
    assert [method for method, _ in calls] == ["jobs.submit", "jobs.submit"]
    assert calls[0][1]["spec"]["execution"]["units"][0]["command"]["argv"] == [
        "echo",
        "first",
    ]


@pytest.mark.parametrize("cleanup_raises", [False, True])
def test_unknown_outcome_propagates_once_and_closes_generator(
    monkeypatch, cleanup_raises
):
    cluster = Molq().cluster("registered")
    calls, closed = [], []
    failure = MolqError("OUTCOME_UNKNOWN", "disconnected", outcome="unknown")

    def submit(*args, **kwargs):
        calls.append(args)
        raise failure

    monkeypatch.setattr(cluster, "submit", submit)

    @cluster
    def hello():
        try:
            yield JobSpec.argv(["echo", "first"])
            yield JobSpec.argv(["echo", "never"])
        finally:
            closed.append(True)
            if cleanup_raises:
                raise RuntimeError("cleanup failed")

    with pytest.raises(MolqError) as caught:
        hello()
    assert caught.value is failure
    assert len(calls) == 1
    assert closed == [True]


def test_empty_generator_does_not_submit(monkeypatch):
    cluster = Molq().cluster("registered")
    monkeypatch.setattr(cluster, "submit", lambda *a, **k: pytest.fail("submitted"))

    @cluster
    def empty():
        return "empty"
        yield

    assert empty() == "empty"


def test_async_cancellation_closes_builder_without_resubmission(monkeypatch):
    async def scenario():
        cluster = AsyncMolq().cluster("registered")
        entered = asyncio.Event()
        calls, closed = [], []

        async def submit(*args, **kwargs):
            calls.append(args)
            entered.set()
            await asyncio.Future()

        monkeypatch.setattr(cluster, "submit", submit)

        @cluster
        def hello():
            try:
                yield JobSpec.argv(["echo", "first"])
                yield JobSpec.argv(["echo", "never"])
            finally:
                closed.append(True)
                raise RuntimeError("cleanup failed")

        task = asyncio.create_task(hello())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(calls) == 1
        assert closed == [True]

    asyncio.run(scenario())


def test_decorator_rejects_async_builders_before_connecting():
    cluster = Molq().cluster("registered")

    async def coroutine():
        return JobSpec.argv(["echo", "hello"])

    async def generator():
        yield JobSpec.argv(["echo", "hello"])

    for function in (coroutine, generator, None):
        with pytest.raises(TypeError, match="synchronous"):
            cluster(function)


@pytest.mark.parametrize("value", [["echo", "raw"], {"execution": {}}, None])
@pytest.mark.parametrize("method", ["submit", "validate", "preview"])
def test_python_submission_operations_require_job_spec(monkeypatch, method, value):
    mq = Molq()
    monkeypatch.setattr(mq, "rpc", lambda *a, **k: pytest.fail("dispatched"))
    with pytest.raises(MolqError, match="JobSpec") as error:
        getattr(mq.cluster("registered"), method)(value)
    assert error.value.outcome == "not_applied"


@pytest.mark.parametrize("value", [["echo", "raw"], {"execution": {}}, None])
def test_decorator_requires_job_spec_before_dispatch(monkeypatch, value):
    mq = Molq()
    monkeypatch.setattr(mq, "rpc", lambda *a, **k: pytest.fail("dispatched"))

    def builder():
        yield value

    decorated = mq.cluster("registered")(builder)
    with pytest.raises(MolqError, match="JobSpec"):
        decorated()


def test_async_operations_and_decorator_require_job_spec(monkeypatch):
    async def scenario():
        mq = AsyncMolq()

        async def rpc(*args, **kwargs):
            pytest.fail("dispatched")

        monkeypatch.setattr(mq, "rpc", rpc)
        cluster = mq.cluster("registered")
        for method in (cluster.submit, cluster.validate, cluster.preview):
            with pytest.raises(MolqError, match="JobSpec"):
                await method(["echo", "raw"])

        @cluster
        def builder():
            yield {"execution": {}}

        with pytest.raises(MolqError, match="JobSpec"):
            await builder()

    asyncio.run(scenario())


def test_ordinary_function_preserves_old_return_without_rpc(monkeypatch):
    mq = Molq()
    monkeypatch.setattr(mq, "rpc", lambda *a, **k: pytest.fail("dispatched"))

    @mq.cluster("registered")
    def double(value):
        return value * 2

    assert double(3) == 6


@pytest.mark.skipif(os.name == "nt", reason="Local Shell execution requires POSIX")
def test_old_submit_entry_registers_reuses_and_returns_jobs(tmp_path):
    settings = {
        "registry": tmp_path / "registry.db",
        "project_root": tmp_path,
    }
    local = submit("dev", "local", **settings)
    try:

        @local
        def hello(name):
            job = yield JobSpec.argv(["echo", name])
            return job

        job = hello("old style")
        assert job.wait(timeout=10, interval=0.05)["completion"]["successful"]
        assert job.logs()["text"] == "old style\n"
        first_id = job.ref["cluster_id"]
    finally:
        local.close()
    reused = submit("dev", "local", **settings)
    try:
        assert reused.definition()["id"] == first_id
        assert len(reused.molq.clusters.list()) == 1
    finally:
        reused.close()
    selected = submit("dev", registry=settings["registry"])
    try:
        assert (
            selected.molq.job(job.ref).status()["snapshots"][0]["state"] == "succeeded"
        )
    finally:
        selected.close()
    with pytest.raises(MolqError, match="differs"):
        submit("dev", "slurm", registry=settings["registry"])


@pytest.mark.skipif(os.name == "nt", reason="Local Shell execution requires POSIX")
def test_async_submit_entry_has_the_same_generator_contract(tmp_path):
    async def scenario():
        local = await asubmit(
            "dev",
            "local",
            registry=tmp_path / "registry.db",
            project_root=tmp_path,
        )
        try:

            @local
            def hello():
                job = yield JobSpec.argv(["echo", "async entry"])
                return job

            job = await hello()
            assert (await job.wait(timeout=10, interval=0.05))["completion"][
                "successful"
            ]
            assert (await job.logs())["text"] == "async entry\n"

            @local
            def double(value):
                return value * 2

            assert await double(3) == 6
        finally:
            await local.close()

    asyncio.run(scenario())


def test_submit_entry_selects_lazily_and_never_registers_unknown_names(monkeypatch):
    calls, closed = [], []

    def rpc(self, method, **params):
        calls.append(method)
        raise MolqError("CLUSTER_NOT_FOUND", "Unknown Cluster")

    monkeypatch.setattr(Molq, "rpc", rpc)
    monkeypatch.setattr(Molq, "close", lambda self: closed.append(True))
    destination = submit("unknown", project_root="/project")
    assert calls == []
    try:
        with pytest.raises(MolqError, match="Unknown Cluster"):
            destination.submit(JobSpec.argv(["echo", "hello"]))
        assert calls == ["jobs.submit"]
    finally:
        destination.close()
    assert closed == [True]


def test_submit_registration_unknown_outcome_is_not_resent(monkeypatch):
    calls, closed = [], []
    failure = MolqError("OUTCOME_UNKNOWN", "disconnected", outcome="unknown")

    def rpc(self, method, **params):
        calls.append(method)
        if method == "clusters.get":
            raise MolqError("CLUSTER_NOT_FOUND", "Unknown Cluster")
        raise failure

    monkeypatch.setattr(Molq, "rpc", rpc)
    monkeypatch.setattr(Molq, "close", lambda self: closed.append(True))
    with pytest.raises(MolqError) as caught:
        submit("dev", "local")
    assert caught.value is failure
    assert calls == ["clusters.get", "clusters.register"]
    assert closed == [True]


def test_local_binding_cannot_silently_select_ssh_execution(monkeypatch):
    calls = []

    def rpc(self, method, **params):
        calls.append(method)
        return {"scheduler": "shell", "transport": {"kind": "ssh", "alias": "hpc"}}

    monkeypatch.setattr(Molq, "rpc", rpc)
    with pytest.raises(MolqError, match="differs") as caught:
        submit("dev", "local")
    assert caught.value.context["field"] == "transport"
    assert calls == ["clusters.get"]


def test_new_hpc_binding_registers_without_a_job_root(monkeypatch):
    calls = []

    def rpc(self, method, **params):
        calls.append((method, params))
        if method == "clusters.get":
            raise MolqError("CLUSTER_NOT_FOUND", "Unknown Cluster")
        return {"id": "cluster-id"}

    monkeypatch.setattr(Molq, "rpc", rpc)
    hpc = submit("hpc", "slurm", transport={"kind": "ssh", "alias": "hpc"})
    hpc.close()
    assert calls[1] == (
        "clusters.register",
        {
            "name": "hpc",
            "scheduler": "slurm",
            "transport": {"kind": "ssh", "alias": "hpc"},
        },
    )
