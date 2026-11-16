"""Submit and wait without a long-lived Runtime."""

import json
import tempfile
from pathlib import Path

from molq import JobSpec, submit

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    local = submit("local", "local", registry=root / "registry.db", project_root=root)

    @local
    def hello(name):
        job = yield JobSpec.argv(["echo", name])
        return job

    try:
        job = hello("molq 0.9.0")
        print(json.dumps(job.wait(timeout=10), ensure_ascii=False))
        print(json.dumps(job.logs(), ensure_ascii=False))
    finally:
        local.close()
