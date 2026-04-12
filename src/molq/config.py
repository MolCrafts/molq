"""Profile and config loading for molq."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from molcfg import ConfigLoader, OneOf, TomlFileSource
from molcfg import ValidationError as CfgValidationError
from molcfg import validate as cfg_validate

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


# Validation schema for a single profile section
class _ProfileSchema:
    scheduler: str
    cluster_name: str
    jobs_dir: str | None = None
    __constraints__: dict = {"scheduler": [OneOf(*OPTIONS_TYPE_MAP.keys())]}


def default_config_path() -> Path:
    return Path.home() / ".molq" / "config.toml"


def load_config(path: str | Path | None = None) -> MolqConfig:
    config_path = Path(path).expanduser() if path is not None else default_config_path()
    if not config_path.exists():
        return MolqConfig(profiles={})

    cfg = ConfigLoader([TomlFileSource(config_path)]).load()
    raw_profiles = cfg.get("profiles")
    if not raw_profiles:
        return MolqConfig(profiles={})

    profiles: dict[str, MolqProfile] = {}
    for name, section in raw_profiles.items():
        data: dict[str, Any] = (
            section.to_dict() if hasattr(section, "to_dict") else section
        )
        profiles[name] = _parse_profile(name, data)
    return MolqConfig(profiles=profiles)


def load_profile(name: str, path: str | Path | None = None) -> MolqProfile:
    config = load_config(path)
    profile = config.profiles.get(name)
    if profile is None:
        raise ConfigError(f"Profile {name!r} not found", profile=name)
    return profile


def _parse_profile(name: str, data: dict[str, Any]) -> MolqProfile:
    try:
        cfg_validate(data, _ProfileSchema, allow_extra=True)
    except CfgValidationError as exc:
        raise ConfigError(
            f"Profile {name!r} is invalid: {'; '.join(exc.errors)}",
            profile=name,
        ) from exc

    scheduler: str = data["scheduler"]
    cluster_name: str = data["cluster_name"]

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
