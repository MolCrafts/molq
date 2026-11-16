"""Client tests talk to the molq binary. They do not import the service."""

import pytest


@pytest.fixture(autouse=True)
def isolated_home(tmp_path_factory, monkeypatch):
    """Keep job directories, which live under the target home, out of the real one."""
    monkeypatch.setenv("HOME", str(tmp_path_factory.mktemp("home")))
