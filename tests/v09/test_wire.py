"""RPC session failure isolation against finite native peer processes."""

import asyncio
import json
import subprocess
import sys

import pytest

from molq.client.async_wire import AsyncWire
from molq.client.wire import MAX_FRAME, Wire
from molq.errors import MolqError


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), True, "1", 0, -1])
def test_both_wires_reject_invalid_timeout(timeout):
    for wire_type in (Wire, AsyncWire):
        with pytest.raises(MolqError) as caught:
            wire_type(timeout=timeout)
        assert caught.value.kind == "INVALID_INPUT"


@pytest.mark.parametrize("params", [{"value": object()}, {"value": float("nan")}, []])
def test_invalid_request_is_rejected_before_starting_a_peer(params):
    wire = Wire()
    with pytest.raises(MolqError) as caught:
        wire.call("jobs.submit", params)
    assert caught.value.kind == "INVALID_INPUT"
    assert wire.process is None

    async def scenario():
        wire = AsyncWire()
        with pytest.raises(MolqError) as caught:
            await wire.call("jobs.submit", params)
        assert caught.value.kind == "INVALID_INPUT"
        assert wire.process is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "response",
    [
        "[]",
        "null",
        '{"jsonrpc":"2.0","id":true,"result":{}}',
        '{"jsonrpc":"2.0","id":1,"result":{},"error":{}}',
        '{"jsonrpc":"2.0","id":1,"error":null}',
        '{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"bad","data":[]}}',
        '{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"bad","data":{"kind":"BAD","message":"bad","outcome":"oops","context":{}}}}',
        '{"jsonrpc":"2.0","id":1,"result":NaN}',
    ],
)
def test_malformed_native_mutation_responses_have_unknown_outcome(response):
    peer = f"import sys; sys.stdin.readline(); print({response!r}, flush=True)"
    wire = Wire(timeout=2)
    wire.hello = {"registry_id": "fixture"}
    wire.process = subprocess.Popen(
        [sys.executable, "-u", "-c", peer],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    try:
        with pytest.raises(MolqError) as caught:
            wire.call("jobs.submit", {})
        assert caught.value.kind == "OUTCOME_UNKNOWN"
        assert caught.value.outcome == "unknown"
    finally:
        wire.close()

    async def scenario():
        wire = AsyncWire(timeout=2)
        wire.hello = {"registry_id": "fixture"}
        wire.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            "-c",
            peer,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        try:
            with pytest.raises(MolqError) as caught:
                await wire.call("jobs.submit", {})
            assert caught.value.kind == "OUTCOME_UNKNOWN"
            assert caught.value.outcome == "unknown"
        finally:
            await wire.close()

    asyncio.run(scenario())


def test_sync_stdio_accepts_a_maximum_frame():
    base = json.dumps({"jsonrpc": "2.0", "id": 1, "result": ""})
    peer = f"""
import json, sys
request = json.loads(sys.stdin.readline())
sys.stdout.buffer.write(json.dumps({{"jsonrpc": "2.0", "id": request["id"], "result": "a" * {MAX_FRAME - len(base)}}}).encode() + bytes([10]))
sys.stdout.buffer.flush()
"""
    wire = Wire(timeout=2)
    wire.hello = {"registry_id": "fixture"}
    wire.process = subprocess.Popen(
        [sys.executable, "-u", "-c", peer],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    try:
        assert len(wire.call("clusters.list")) == MAX_FRAME - len(base)
    finally:
        wire.close()


def test_async_stdio_accepts_a_maximum_frame():
    async def scenario():
        base = json.dumps({"jsonrpc": "2.0", "id": 1, "result": ""})
        peer = f"""
import json, sys
request = json.loads(sys.stdin.readline())
sys.stdout.buffer.write(json.dumps({{"jsonrpc": "2.0", "id": request["id"], "result": "a" * {MAX_FRAME - len(base)}}}).encode() + bytes([10]))
sys.stdout.buffer.flush()
"""
        wire = AsyncWire(timeout=2)
        wire.hello = {"registry_id": "fixture"}
        wire.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            "-c",
            peer,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            limit=MAX_FRAME + 1,
        )
        try:
            assert len(await wire.call("clusters.list")) == MAX_FRAME - len(base)
        finally:
            await wire.close()

    asyncio.run(scenario())


def test_utf16_peer_response_cannot_confirm_a_mutation():
    peer = """
import json, sys
request = json.loads(sys.stdin.readline())
response = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": {}})
sys.stdout.buffer.write(response.encode("utf-16") + b"\\n")
sys.stdout.buffer.flush()
"""
    wire = Wire(timeout=2)
    wire.hello = {"registry_id": "fixture"}
    wire.process = subprocess.Popen(
        [sys.executable, "-u", "-c", peer],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    try:
        with pytest.raises(MolqError) as caught:
            wire.call("jobs.submit")
        assert caught.value.outcome == "unknown"
        assert caught.value.kind == "OUTCOME_UNKNOWN"
    finally:
        wire.close()
