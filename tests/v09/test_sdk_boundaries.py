"""Public SDK values preserve errors and reject invalid wait controls locally."""

import asyncio
import json

import pytest

from molq import AsyncMolq, Molq, MolqError, asubmit, submit
from molq.client.binary import executable


def test_runtime_error_context_preserves_reserved_field_names():
    data = {
        "kind": "INVALID_INPUT",
        "message": "Rejected request",
        "outcome": "not_applied",
        "context": {
            "kind": "argv",
            "message": "native diagnostic",
            "outcome": "target value",
        },
    }
    error = MolqError.from_wire(data)
    assert error.to_wire() == data
    assert error.context is not data["context"]


@pytest.mark.parametrize(
    "data",
    [
        [],
        None,
        {"client": []},
        {"client": {"executable": 42}},
        {"client": {"executable": ""}},
    ],
)
def test_invalid_binary_configuration_cannot_fall_back(tmp_path, data):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data))
    with pytest.raises(MolqError) as caught:
        executable(path)
    assert caught.value.kind == "INVALID_INPUT"


@pytest.mark.parametrize(
    "scheduler, fields",
    [("local", {"typo": None}), ("unknown", {}), ("local", {"transport": []})],
)
def test_invalid_binding_options_do_not_query_runtime(monkeypatch, scheduler, fields):
    monkeypatch.setattr(Molq, "rpc", lambda *a, **k: pytest.fail("RPC dispatched"))
    with pytest.raises(MolqError) as caught:
        submit("dev", scheduler, **fields)
    assert caught.value.kind == "INVALID_INPUT"

    async def scenario():
        with pytest.raises(MolqError) as caught:
            await asubmit("dev", scheduler, **fields)
        assert caught.value.kind == "INVALID_INPUT"

    asyncio.run(scenario())


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, 0, True, "1", None])
@pytest.mark.parametrize("parameter", ["timeout", "interval"])
def test_wait_controls_fail_before_observation_in_both_clients(
    monkeypatch, value, parameter
):
    collection = Molq().jobs([])
    monkeypatch.setattr(
        collection, "observe", lambda **kwargs: pytest.fail("RPC dispatched")
    )
    with pytest.raises(MolqError) as error:
        collection.wait(**{parameter: value})
    assert error.value.kind == "INVALID_INPUT"
    assert error.value.outcome == "not_applied"

    async def scenario():
        collection = AsyncMolq().jobs([])

        async def observe(**kwargs):
            pytest.fail("RPC dispatched")

        monkeypatch.setattr(collection, "observe", observe)
        with pytest.raises(MolqError) as error:
            await collection.wait(**{parameter: value})
        assert error.value.kind == "INVALID_INPUT"
        assert error.value.outcome == "not_applied"

    asyncio.run(scenario())


def test_registration_cleanup_preserves_unknown_mutation_outcome(monkeypatch):
    failure = MolqError("OUTCOME_UNKNOWN", "Disconnected", outcome="unknown")
    calls = []

    def rpc(self, method, **params):
        calls.append(method)
        if method == "clusters.get":
            raise MolqError("CLUSTER_NOT_FOUND", "Unknown destination")
        raise failure

    def close(self):
        raise RuntimeError("Cleanup failed")

    monkeypatch.setattr(Molq, "rpc", rpc)
    monkeypatch.setattr(Molq, "close", close)
    with pytest.raises(MolqError) as caught:
        submit("dev", "local")
    assert caught.value is failure
    assert calls == ["clusters.get", "clusters.register"]

    async def scenario():
        calls.clear()

        async def async_rpc(self, method, **params):
            return rpc(self, method, **params)

        async def async_close(self):
            close(self)

        monkeypatch.setattr(AsyncMolq, "rpc", async_rpc)
        monkeypatch.setattr(AsyncMolq, "close", async_close)
        with pytest.raises(MolqError) as caught:
            await asubmit("dev", "local")
        assert caught.value is failure
        assert calls == ["clusters.get", "clusters.register"]

    asyncio.run(scenario())


def test_connection_cleanup_preserves_handshake_failure(monkeypatch):
    failure = MolqError("REGISTRY_MISMATCH", "Unexpected registry")
    mq = Molq()

    def connect():
        raise failure

    def close():
        raise RuntimeError("Cleanup failed")

    monkeypatch.setattr(mq._wire, "connect", connect)
    monkeypatch.setattr(mq, "close", close)
    with pytest.raises(MolqError) as caught:
        mq.__enter__()
    assert caught.value is failure

    async def scenario():
        mq = AsyncMolq()

        async def async_connect():
            connect()

        async def async_close():
            close()

        monkeypatch.setattr(mq._wire, "connect", async_connect)
        monkeypatch.setattr(mq, "close", async_close)
        with pytest.raises(MolqError) as caught:
            await mq.__aenter__()
        assert caught.value is failure

    asyncio.run(scenario())
