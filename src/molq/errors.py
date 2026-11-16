"""Stable language-independent errors."""

from __future__ import annotations

from typing import Any


class MolqError(Exception):
    """An RPC failure with stable kind and mutation outcome.

    Args:
        kind: Public machine-readable classification.
        message: Human-readable description.
        outcome: not_applied, applied or unknown.
        context: Bounded diagnostic information.
    """

    def __init__(
        self, kind: str, message: str, *, outcome: str = "not_applied", **context: Any
    ):
        super().__init__(message)
        self.kind = kind
        self.outcome = outcome
        self.context = context

    def to_wire(self) -> dict[str, Any]:
        """Return the shared RPC ErrorData representation."""
        return {
            "kind": self.kind,
            "message": str(self),
            "outcome": self.outcome,
            "context": self.context,
        }

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> MolqError:
        """Construct a language façade error from runtime ErrorData."""
        error = cls(
            data["kind"],
            data["message"],
            outcome=data.get("outcome", "unknown"),
        )
        error.context = dict(data.get("context", {}))
        return error
