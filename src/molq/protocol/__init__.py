"""Packaged protocol document. Validation runs in the serve process."""

from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import files
from typing import Any


@lru_cache(maxsize=1)
def contract() -> dict[str, Any]:
    """Load the packaged language-independent contract."""
    return json.loads(files(__package__).joinpath("contract.json").read_text())
