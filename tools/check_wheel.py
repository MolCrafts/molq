"""Exercise the installed artifact; run outside the repository checkout."""

import asyncio
import importlib
import importlib.resources
import importlib.util
import json
import os
import pkgutil
import subprocess
import sys
import tempfile
from pathlib import Path

import molq
from molq import JobSpec, Molq, asubmit, submit

source = Path(__file__).resolve().parents[1] / "src"
assert not Path(molq.__file__).resolve().is_relative_to(source)
assert molq.__version__ == "0.9.0"
for item in pkgutil.walk_packages(molq.__path__, prefix="molq."):
    importlib.import_module(item.name)
for name in molq.__all__:
    getattr(molq, name)
for obsolete in ("submitor", "store", "retry", "plugins", "dashboard", "web"):
    assert importlib.util.find_spec("molq." + obsolete) is None, obsolete
assert importlib.resources.files("molq.protocol").joinpath("contract.json").is_file()
assert not importlib.resources.files("molq.protocol").joinpath("v1.json").is_file()


with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    # Job directories live under the target home; keep them out of the real one.
    os.environ["HOME"] = str(root / "home")
    for args in (["--version"], ["--help"], ["job", "--help"]):
        command = subprocess.run(
            [sys.executable, "-m", "molq", *args],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        if args == ["--version"]:
            envelope = json.loads(command.stdout)
            assert (
                envelope["jsonrpc"] == "2.0"
                and envelope["result"]["version"] == "0.9.0"
            )
        else:
            assert "molq job" in command.stdout
            assert "jsonrpc" not in command.stdout
    with Molq(registry=root / "registry.db", config=root / "config.json") as mq:
        if os.name != "nt":
            mq.clusters.register(name="local", scheduler="shell")
            job = mq.cluster("local").submit(JobSpec.argv(["echo", "installed-wheel"]))
            assert job.wait(timeout=10, interval=0.05)["completion"]["successful"]
            assert "installed-wheel" in job.logs()["text"]
        else:
            mq.clusters.register(
                name="hpc",
                scheduler="slurm",
                transport={"kind": "ssh", "alias": "hpc"},
            )
        assert len(mq.clusters.list()) == 1
    if os.name != "nt":
        local = submit("local", registry=root / "registry.db")

        @local
        def hello():
            job = yield JobSpec.argv(["echo", "installed-decorator"])
            return job

        try:
            job = hello()
            assert job.wait(timeout=10, interval=0.05)["completion"]["successful"]
            assert job.logs()["text"] == "installed-decorator\n"
        finally:
            local.close()

        async def check_async():
            local = await asubmit("local", "local", registry=root / "registry.db")

            @local
            def hello():
                job = yield JobSpec.argv(["echo", "installed-async-decorator"])
                return job

            try:
                job = await hello()
                assert (await job.wait(timeout=10, interval=0.05))["completion"][
                    "successful"
                ]
                assert (await job.logs())["text"] == "installed-async-decorator\n"
            finally:
                await local.close()

        asyncio.run(check_async())
print(
    json.dumps(
        {
            "result": {
                "installed_wheel": "passed",
                "cli_json": "passed",
                "sdk_execution": "passed",
            }
        }
    )
)
