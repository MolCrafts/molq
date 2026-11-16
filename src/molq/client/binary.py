"""Locate the serve executable. Configuration selects it; the environment does not."""

from __future__ import annotations

import json
import sys
from importlib.resources import files
from pathlib import Path

from molq.errors import MolqError


def executable(
    config: Path | str | None = None, override: Path | str | None = None
) -> str:
    """Return the molq binary path.

    Args:
        config: Optional configuration file whose client.executable field wins.
        override: Explicit constructor path. It wins over the configuration file.
    """
    if override:
        return str(override)
    if config:
        path = Path(config).expanduser()
        if path.is_file():
            try:
                data = json.loads(path.read_text())
            except OSError as error:
                raise MolqError(
                    "INVALID_INPUT", "Cannot read client configuration"
                ) from error
            except ValueError:
                data = {}
            if not isinstance(data, dict) or not isinstance(
                data.get("client", {}), dict
            ):
                raise MolqError(
                    "INVALID_INPUT", "Client configuration must be an object"
                )
            client = data.get("client", {})
            chosen = client.get("executable")
            if "executable" in client and (not isinstance(chosen, str) or not chosen):
                raise MolqError(
                    "INVALID_INPUT", "Client executable must be a nonempty path"
                )
            if chosen is not None:
                return str(Path(chosen).expanduser())
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "go.mod").is_file() and (parent / "bin" / "molq").is_file():
            return str(parent / "bin" / "molq")
    packaged = files("molq").joinpath(
        "_bin", "molq.exe" if sys.platform == "win32" else "molq"
    )
    if packaged.is_file():
        return str(packaged)
    raise MolqError("RUNTIME_UNAVAILABLE", "molq serve executable is not built")
