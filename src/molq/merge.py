"""Defaults merge logic for molq.

Pure function that merges per-submit parameters with Submitor defaults.
"""

from __future__ import annotations

import dataclasses

from molq.models import SubmitorDefaults
from molq.types import JobExecution, JobResources, JobScheduling


def _merge_one(default: object | None, override: object | None, cls: type) -> object:
    """Merge a single defaults/override pair using field-level shallow override.

    For each field in the override instance, if the field value is the type's
    default (typically None or False), fall back to the default instance's value.
    """
    if default is None and override is None:
        return cls()
    if default is None:
        return override  # type: ignore[return-value]
    if override is None:
        return default

    merged_fields: dict[str, object] = {}
    for f in dataclasses.fields(cls):  # type: ignore[arg-type]
        override_val = getattr(override, f.name)
        default_val = getattr(default, f.name)
        # Use override value if it's not the field's default
        field_default = f.default if f.default is not dataclasses.MISSING else None
        if override_val != field_default:
            merged_fields[f.name] = override_val
        else:
            merged_fields[f.name] = default_val

    return cls(**merged_fields)  # type: ignore[call-arg]


def merge_defaults(
    defaults: SubmitorDefaults | None,
    *,
    resources: JobResources | None = None,
    scheduling: JobScheduling | None = None,
    execution: JobExecution | None = None,
) -> tuple[JobResources, JobScheduling, JobExecution]:
    """Merge per-submit parameters with Submitor-level defaults.

    Returns:
        Tuple of (merged_resources, merged_scheduling, merged_execution).
    """
    d_resources = defaults.resources if defaults else None
    d_scheduling = defaults.scheduling if defaults else None
    d_execution = defaults.execution if defaults else None

    return (
        _merge_one(d_resources, resources, JobResources),  # type: ignore[return-value]
        _merge_one(d_scheduling, scheduling, JobScheduling),  # type: ignore[return-value]
        _merge_one(d_execution, execution, JobExecution),  # type: ignore[return-value]
    )
