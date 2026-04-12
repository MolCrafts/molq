"""Public value types for molq.

Provides Memory, Duration, Script, DependencyRef, JobResources, JobScheduling,
and JobExecution. All types are frozen (immutable) dataclasses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# Canonical set of dependency conditions.  Used as the type for
# DependencyRef.condition and as a shared reference for all condition-dispatch
# tables in submitor.py and store.py.
DependencyCondition = Literal[
    "after_started", "after", "after_success", "after_failure"
]

# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------

_MEMORY_UNITS: dict[str, int] = {
    "b": 1,
    "kb": 1024,
    "mb": 1024**2,
    "gb": 1024**3,
    "tb": 1024**4,
}

_MEMORY_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*$")


@dataclass(frozen=True, order=True)
class Memory:
    """Immutable memory quantity stored as bytes."""

    bytes: int

    # -- factory methods --

    @classmethod
    def kb(cls, n: int | float) -> Memory:
        return cls(bytes=int(n * 1024))

    @classmethod
    def mb(cls, n: int | float) -> Memory:
        return cls(bytes=int(n * 1024**2))

    @classmethod
    def gb(cls, n: int | float) -> Memory:
        return cls(bytes=int(n * 1024**3))

    @classmethod
    def tb(cls, n: int | float) -> Memory:
        return cls(bytes=int(n * 1024**4))

    @classmethod
    def parse(cls, s: str) -> Memory:
        """Parse a human-readable string like '8GB', '512MB', '1024'."""
        if not s:
            raise ValueError("Empty memory string")
        m = _MEMORY_PATTERN.match(s)
        if not m:
            raise ValueError(f"Cannot parse memory format: {s!r}")
        value_str, unit_str = m.groups()
        value = float(value_str)
        unit = unit_str.lower() if unit_str else "b"
        if unit and not unit.endswith("b"):
            unit += "b"
        multiplier = _MEMORY_UNITS.get(unit)
        if multiplier is None:
            raise ValueError(f"Unknown memory unit: {unit_str!r}")
        return cls(bytes=int(value * multiplier))

    # -- scheduler format methods --

    def to_slurm(self) -> str:
        """Format for SLURM --mem (e.g. '8G', '512M')."""
        if self.bytes >= _MEMORY_UNITS["tb"] and self.bytes % _MEMORY_UNITS["tb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['tb']}T"
        if self.bytes >= _MEMORY_UNITS["gb"] and self.bytes % _MEMORY_UNITS["gb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['gb']}G"
        if self.bytes >= _MEMORY_UNITS["mb"] and self.bytes % _MEMORY_UNITS["mb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['mb']}M"
        if self.bytes >= _MEMORY_UNITS["kb"] and self.bytes % _MEMORY_UNITS["kb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['kb']}K"
        return str(self.bytes)

    def to_pbs(self) -> str:
        """Format for PBS -l mem (e.g. '8gb', '512mb')."""
        if self.bytes >= _MEMORY_UNITS["tb"] and self.bytes % _MEMORY_UNITS["tb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['tb']}tb"
        if self.bytes >= _MEMORY_UNITS["gb"] and self.bytes % _MEMORY_UNITS["gb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['gb']}gb"
        if self.bytes >= _MEMORY_UNITS["mb"] and self.bytes % _MEMORY_UNITS["mb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['mb']}mb"
        if self.bytes >= _MEMORY_UNITS["kb"] and self.bytes % _MEMORY_UNITS["kb"] == 0:
            return f"{self.bytes // _MEMORY_UNITS['kb']}kb"
        return f"{self.bytes}b"

    def to_lsf_kb(self) -> int:
        """Format for LSF -M (KB integer)."""
        return self.bytes // 1024


# ---------------------------------------------------------------------------
# Duration
# ---------------------------------------------------------------------------

_TIME_UNITS: dict[str, int] = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

_DURATION_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*([smhdw])", re.IGNORECASE)


@dataclass(frozen=True, order=True)
class Duration:
    """Immutable time duration stored as seconds."""

    seconds: int

    # -- factory methods --

    @classmethod
    def minutes(cls, n: int) -> Duration:
        return cls(seconds=n * 60)

    @classmethod
    def hours(cls, n: int) -> Duration:
        return cls(seconds=n * 3600)

    @classmethod
    def parse(cls, s: str) -> Duration:
        """Parse '2h30m', '01:30:00', '90m', or plain seconds."""
        if not s:
            raise ValueError("Empty duration string")

        # HH:MM:SS or MM:SS
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 3:
                h, m, sec = (int(p) for p in parts)
                return cls(seconds=h * 3600 + m * 60 + sec)
            if len(parts) == 2:
                m, sec = (int(p) for p in parts)
                return cls(seconds=m * 60 + sec)
            raise ValueError(f"Cannot parse duration: {s!r}")

        # Human-readable: 2h30m, 90m, etc.
        matches = _DURATION_PATTERN.findall(s)
        if matches:
            total = 0.0
            for value_str, unit in matches:
                total += float(value_str) * _TIME_UNITS[unit.lower()]
            return cls(seconds=int(total))

        # Plain number (seconds)
        try:
            return cls(seconds=int(float(s)))
        except ValueError:
            raise ValueError(f"Cannot parse duration: {s!r}")

    # -- scheduler format methods --

    def to_slurm(self) -> str:
        """Format for SLURM --time (HH:MM:SS or D-HH:MM:SS)."""
        total = self.seconds
        hours = total // 3600
        mins = (total % 3600) // 60
        secs = total % 60
        if hours >= 24:
            days = hours // 24
            hours = hours % 24
            return f"{days}-{hours:02d}:{mins:02d}:{secs:02d}"
        return f"{hours:02d}:{mins:02d}:{secs:02d}"

    def to_pbs(self) -> str:
        """Format for PBS -l walltime (HH:MM:SS)."""
        total = self.seconds
        hours = total // 3600
        mins = (total % 3600) // 60
        secs = total % 60
        return f"{hours:02d}:{mins:02d}:{secs:02d}"

    def to_lsf_minutes(self) -> int:
        """Format for LSF -W (integer minutes, rounded up)."""
        return (self.seconds + 59) // 60


# ---------------------------------------------------------------------------
# Script
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Script:
    """Immutable script reference — either inline text or a file path."""

    _variant: str
    _text: str | None
    _path: Path | None

    @classmethod
    def inline(cls, text: str) -> Script:
        """Create a script from inline text content."""
        return cls(_variant="inline", _text=text, _path=None)

    @classmethod
    def path(cls, path: str | Path) -> Script:
        """Create a script from an existing file path."""
        resolved = Path(path).resolve()
        return cls(_variant="path", _text=None, _path=resolved)

    @property
    def variant(self) -> str:
        return self._variant

    @property
    def text(self) -> str | None:
        return self._text

    @property
    def file_path(self) -> Path | None:
        return self._path


# ---------------------------------------------------------------------------
# Resource / Scheduling / Execution specs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DependencyRef:
    """Logical dependency on another molq job."""

    job_id: str
    condition: DependencyCondition = "after_success"


@dataclass(frozen=True)
class JobResources:
    """Hardware requirements for job execution."""

    cpu_count: int | None = None
    memory: Memory | None = None
    gpu_count: int | None = None
    gpu_type: str | None = None
    time_limit: Duration | None = None


@dataclass(frozen=True)
class JobScheduling:
    """Scheduler-level parameters."""

    queue: str | None = None
    account: str | None = None
    priority: str | None = None
    dependency: str | None = None
    dependencies: tuple[DependencyRef, ...] = ()
    node_count: int | None = None
    exclusive_node: bool = False
    array_spec: str | None = None
    email: str | None = None
    qos: str | None = None
    reservation: str | None = None

    def __post_init__(self) -> None:
        if self.dependency is not None and self.dependencies:
            raise ValueError(
                "JobScheduling: 'dependency' (raw scheduler string) and 'dependencies'"
                " (logical refs) are mutually exclusive — pick one approach"
            )


@dataclass(frozen=True)
class JobExecution:
    """Execution environment parameters."""

    env: dict[str, str] | None = None
    cwd: str | Path | None = None
    job_name: str | None = None
    output_file: str | None = None
    error_file: str | None = None
