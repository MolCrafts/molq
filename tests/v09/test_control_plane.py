"""Portable control plane tests; target POSIX paths need no local Bash."""

import asyncio
import json
import sys

import pytest

from molq import AsyncMolq, Molq, MolqError


def test_native_peer_registry_and_revision_rpc(tmp_path):
    registry = tmp_path / "registry.db"
    with Molq(registry=registry) as mq:
        cluster = mq.clusters.register(
            name="remote",
            scheduler="slurm",
            transport={"kind": "ssh", "alias": "hpc"},
        )
        definition = cluster.definition()
        assert definition["transport"] == {"kind": "ssh", "alias": "hpc"}
        with pytest.raises(MolqError) as error:
            mq.rpc(
                "clusters.register",
                name="bad",
                scheduler="slurm",
                capability={},
            )
        assert error.value.kind == "INVALID_INPUT"
    with Molq(registry=registry) as mq:
        assert mq.cluster("remote").definition()["id"] == definition["id"]


def test_async_native_peer_without_local_scheduler(tmp_path):
    async def scenario():
        async with AsyncMolq(registry=tmp_path / "registry.db") as mq:
            await mq.clusters.register(
                name="remote",
                scheduler="pbs",
                transport={"kind": "ssh", "alias": "hpc"},
                pbs_dialect="openpbs",
            )
            assert len(await mq.clusters.list()) == 1

    asyncio.run(scenario())


def test_stdio_subscription_pushes_registry_reset(tmp_path):
    async def scenario():
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "molq",
            "serve",
            "--stdio",
            "--registry",
            str(tmp_path / "registry.db"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )

        async def read():
            return json.loads(await asyncio.wait_for(process.stdout.readline(), 5))

        async def send(identity, method, params):
            process.stdin.write(
                (
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": identity,
                            "method": method,
                            "params": params,
                        }
                    )
                    + "\n"
                ).encode()
            )
            await process.stdin.drain()

        try:
            await send(1, "events.subscribe", {})
            assert (await read())["result"]["resync"]
            await send(
                2,
                "clusters.register",
                {
                    "name": "remote",
                    "scheduler": "slurm",
                    "transport": {"kind": "ssh", "alias": "hpc"},
                },
            )
            # The push and the response share stdout; either may arrive first.
            frames = [await read(), await read()]
            (response,) = [frame for frame in frames if frame.get("id") == 2]
            (pushed,) = [frame for frame in frames if frame.get("method") == "events"]
            assert response["result"]["id"]
            assert pushed["params"]["resync"]
        finally:
            process.stdin.close()
            await asyncio.wait_for(process.wait(), 10)

    asyncio.run(scenario())


def test_native_startup_schema_error_retains_kind(tmp_path):
    import sqlite3

    path = tmp_path / "old.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE jobs (id TEXT)")
    with pytest.raises(MolqError) as error:
        with Molq(registry=path):
            pass
    assert error.value.kind == "STATE_SCHEMA_INCOMPATIBLE"


def test_cli_partial_cached_batch_has_exit_eight(tmp_path):
    import subprocess

    registry = tmp_path / "registry.db"
    with Molq(registry=registry) as mq:
        destination = mq.clusters.register(
            name="remote",
            scheduler="slurm",
            transport={"kind": "ssh", "alias": "hpc"},
        )
        ref = {
            "registry_id": mq.rpc("molq.hello")["registry_id"],
            "cluster_id": destination.definition()["id"],
            "native_id": "42",
            "incarnation": "original",
        }
    response = subprocess.run(
        [
            sys.executable,
            "-m",
            "molq",
            "--registry",
            str(registry),
            "rpc",
            "jobs.get_many",
            "--params",
            json.dumps({"refs": [ref], "consistency": "cached"}),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert response.returncode == 8
    result = json.loads(response.stdout)["result"]
    assert result["coverage"]["errors"][0]["kind"] == "CACHE_MISS"
    assert not result["completion"]["all_confirmed_terminal"]
