"""Public examples emit JSON records instead of reprs or raw logs."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "name",
    [
        "01_basic.py",
        "02_command_types.py",
        "03_resources.py",
        "04_scheduling.py",
        "05_execution.py",
    ],
)
def test_examples_stdout_is_json(name, tmp_path):
    if name == "01_basic.py" and os.name == "nt":
        pytest.skip("Local Shell requires POSIX")
    script = Path(__file__).resolve().parents[2] / "examples" / name
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert records
    assert all(isinstance(record, dict) for record in records)
    if name == "01_basic.py":
        assert records[0]["completion"]["successful"] is True
        assert records[1]["text"] == "molq 0.9.0\n"
