"""Profile and config loading for molq."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from molq.errors import ConfigError
from molq.models import RetentionPolicy, RetryPolicy, SubmitorDefaults
from molq.options import (
    OPTIONS_TYPE_MAP,
    LocalSchedulerOptions,
    LSFSchedulerOptions,
    PBSSchedulerOptions,
    SchedulerOptions,
    SlurmSchedulerOptions,
)
from molq.serde import (
    deserialize_execution,
    deserialize_resources,
    deserialize_retention_policy,
    deserialize_retry_policy,
    deserialize_scheduling,
)


@dataclass(frozen=True)
class MolqProfile:
    """Named profile loaded from ``~/.molq/config.toml``."""

    name: str
    scheduler: str
    cluster_name: str
    defaults: SubmitorDefaults = field(default_factory=SubmitorDefaults)
    scheduler_options: SchedulerOptions | None = None
    retry: RetryPolicy | None = None
    retention: RetentionPolicy = field(default_factory=RetentionPolicy)
    jobs_dir: str | None = None


@dataclass(frozen=True)
class MolqConfig:
    """Loaded molq configuration."""

    profiles: dict[str, MolqProfile]


def default_config_path() -> Path:
    return Path.home() / ".molq" / "config.toml"


def load_config(path: str | Path | None = None) -> MolqConfig:
    config_path = Path(path).expanduser() if path is not None else default_config_path()
    if not config_path.exists():
        return MolqConfig(profiles={})

    data = tomllib.loads(config_path.read_text())
    raw_profiles = data.get("profiles", {})
    profiles: dict[str, MolqProfile] = {}
    for name, section in raw_profiles.items():
        profiles[name] = _parse_profile(name, section)
    return MolqConfig(profiles=profiles)


def load_profile(name: str, path: str | Path | None = None) -> MolqProfile:
    config = load_config(path)
    profile = config.profiles.get(name)
    if profile is None:
        raise ConfigError(f"Profile {name!r} not found", profile=name)
    return profile


def _parse_profile(name: str, data: dict[str, Any]) -> MolqProfile:
    scheduler = data.get("scheduler")
    cluster_name = data.get("cluster_name")
    if not scheduler or not cluster_name:
        raise ConfigError(
            f"Profile {name!r} must define scheduler and cluster_name",
            profile=name,
        )
    if scheduler not in OPTIONS_TYPE_MAP:
        raise ConfigError(
            f"Profile {name!r} references unknown scheduler {scheduler!r}",
            profile=name,
            scheduler=scheduler,
        )

    defaults = data.get("defaults", {})
    scheduler_options = _parse_scheduler_options(
        scheduler, data.get("scheduler_options")
    )
    return MolqProfile(
        name=name,
        scheduler=scheduler,
        cluster_name=cluster_name,
        defaults=SubmitorDefaults(
            resources=deserialize_resources(defaults.get("resources", {}))
            if defaults.get("resources")
            else None,
            scheduling=deserialize_scheduling(defaults.get("scheduling", {}))
            if defaults.get("scheduling")
            else None,
            execution=deserialize_execution(defaults.get("execution", {}))
            if defaults.get("execution")
            else None,
        ),
        scheduler_options=scheduler_options,
        retry=deserialize_retry_policy(data.get("retry")),
        retention=deserialize_retention_policy(data.get("retention")),
        jobs_dir=data.get("jobs_dir"),
    )


def _parse_scheduler_options(
    scheduler: str,
    data: dict[str, Any] | None,
) -> SchedulerOptions | None:
    if not data:
        return None
    if scheduler == "slurm":
        return SlurmSchedulerOptions(**data)
    if scheduler == "pbs":
        return PBSSchedulerOptions(**data)
    if scheduler == "lsf":
        return LSFSchedulerOptions(**data)
    return LocalSchedulerOptions(**data)
