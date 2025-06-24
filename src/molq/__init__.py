"""Public API for the submitter package."""

from .base import cmdline
from .submit import submit
from .submitor import BaseSubmitor, LocalSubmitor, SlurmSubmitor

# Convenience submitter used by the ``cmdline`` decorator
local = submit("_local_cmdline", "local")

__all__ = [
    "submit",
    "cmdline",
    "local",
    "BaseSubmitor",
    "LocalSubmitor",
    "SlurmSubmitor",
]
