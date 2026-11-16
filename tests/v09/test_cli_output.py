"""Console startup failures preserve the machine-readable output channel."""

import json

import pytest

from molq import __main__ as cli
from molq.errors import MolqError


@pytest.mark.parametrize(
    "failure",
    [
        MolqError("RUNTIME_UNAVAILABLE", 'missing "binary"'),
        OSError('cannot execute "binary"'),
    ],
)
def test_startup_failure_is_json(monkeypatch, capsys, failure):
    monkeypatch.setattr(cli, "executable", lambda: "/missing/molq")

    def fail_exec(*args):
        raise failure

    monkeypatch.setattr(cli.os, "execv", fail_exec)
    with pytest.raises(SystemExit) as stopped:
        cli.main()
    assert stopped.value.code == 4
    output = capsys.readouterr()
    assert not output.err
    envelope = json.loads(output.out)
    assert envelope["jsonrpc"] == "2.0"
    assert envelope["id"] is None
    assert envelope["error"]["data"]["kind"] == "RUNTIME_UNAVAILABLE"
    assert envelope["error"]["data"]["outcome"] == "not_applied"
