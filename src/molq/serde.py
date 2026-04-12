"""Serialization helpers for stored requests and config-driven values."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from molq.models import RetentionPolicy, RetryBackoff, RetryPolicy
from molq.status import JobState
from molq.types import (
    DependencyRef,
    Duration,
    JobExecution,
    JobResources,
    JobScheduling,
    Memory,
    Script,
)


def serialize_memory(value: Memory | None) -> str | None:
    return None if value is None else str(value.bytes)


def deserialize_memory(value: str | None) -> Memory | None:
    return None if value is None else Memory(bytes=int(value))


def serialize_duration(value: Duration | None) -> int | None:
    return None if value is None else value.seconds


def deserialize_duration(value: int | float | None) -> Duration | None:
    return None if value is None else Duration(seconds=int(value))


def serialize_resources(resources: JobResources) -> dict[str, Any]:
    return {
        "cpu_count": resources.cpu_count,
        "memory": serialize_memory(resources.memory),
        "gpu_count": resources.gpu_count,
        "gpu_type": resources.gpu_type,
        "time_limit": serialize_duration(resources.time_limit),
    }


def deserialize_resources(data: dict[str, Any]) -> JobResources:
    return JobResources(
        cpu_count=data.get("cpu_count"),
        memory=deserialize_memory(data.get("memory")),
        gpu_count=data.get("gpu_count"),
        gpu_type=data.get("gpu_type"),
        time_limit=deserialize_duration(data.get("time_limit")),
    )


def serialize_scheduling(scheduling: JobScheduling) -> dict[str, Any]:
    return {
        "queue": scheduling.queue,
        "account": scheduling.account,
        "priority": scheduling.priority,
        "dependency": scheduling.dependency,
        "dependencies": [
            {
                "job_id": dep.job_id,
                "condition": dep.condition,
            }
            for dep in scheduling.dependencies
        ],
        "node_count": scheduling.node_count,
        "exclusive_node": scheduling.exclusive_node,
        "array_spec": scheduling.array_spec,
        "email": scheduling.email,
        "qos": scheduling.qos,
        "reservation": scheduling.reservation,
    }


def deserialize_scheduling(data: dict[str, Any]) -> JobScheduling:
    return JobScheduling(
        queue=data.get("queue"),
        account=data.get("account"),
        priority=data.get("priority"),
        dependency=data.get("dependency"),
        dependencies=tuple(
            DependencyRef(
                job_id=item["job_id"],
                condition=item.get("condition", "after_success"),
            )
            for item in data.get("dependencies", [])
        ),
        node_count=data.get("node_count"),
        exclusive_node=bool(data.get("exclusive_node", False)),
        array_spec=data.get("array_spec"),
        email=data.get("email"),
        qos=data.get("qos"),
        reservation=data.get("reservation"),
    )


def serialize_execution(execution: JobExecution) -> dict[str, Any]:
    return {
        "env": execution.env,
        "cwd": None if execution.cwd is None else str(execution.cwd),
        "job_name": execution.job_name,
        "output_file": execution.output_file,
        "error_file": execution.error_file,
    }


def deserialize_execution(data: dict[str, Any]) -> JobExecution:
    return JobExecution(
        env=data.get("env"),
        cwd=data.get("cwd"),
        job_name=data.get("job_name"),
        output_file=data.get("output_file"),
        error_file=data.get("error_file"),
    )


def serialize_script(script: Script | None) -> dict[str, Any] | None:
    if script is None:
        return None
    return {
        "variant": script.variant,
        "text": script.text,
        "file_path": None if script.file_path is None else str(script.file_path),
    }


def deserialize_script(data: dict[str, Any] | None) -> Script | None:
    if data is None:
        return None
    if data["variant"] == "inline":
        return Script.inline(data["text"] or "")
    return Script.path(Path(data["file_path"]))


def serialize_retry_backoff(backoff: RetryBackoff) -> dict[str, Any]:
    return {
        "mode": backoff.mode,
        "initial_seconds": backoff.initial_seconds,
        "maximum_seconds": backoff.maximum_seconds,
        "factor": backoff.factor,
    }


def deserialize_retry_backoff(data: dict[str, Any]) -> RetryBackoff:
    return RetryBackoff(
        mode=data.get("mode", "exponential"),
        initial_seconds=float(data.get("initial_seconds", 5.0)),
        maximum_seconds=float(data.get("maximum_seconds", 300.0)),
        factor=float(data.get("factor", 2.0)),
    )


def serialize_retry_policy(policy: RetryPolicy | None) -> dict[str, Any] | None:
    if policy is None:
        return None
    return {
        "max_attempts": policy.max_attempts,
        "retry_on_states": [state.value for state in policy.retry_on_states],
        "retry_on_exit_codes": (
            None
            if policy.retry_on_exit_codes is None
            else list(policy.retry_on_exit_codes)
        ),
        "backoff": serialize_retry_backoff(policy.backoff),
    }


def deserialize_retry_policy(data: dict[str, Any] | None) -> RetryPolicy | None:
    if data is None:
        return None
    return RetryPolicy(
        max_attempts=int(data.get("max_attempts", 1)),
        retry_on_states=tuple(
            JobState(state)
            for state in data.get("retry_on_states", ["failed", "timed_out"])
        ),
        retry_on_exit_codes=(
            None
            if data.get("retry_on_exit_codes") is None
            else tuple(int(code) for code in data["retry_on_exit_codes"])
        ),
        backoff=deserialize_retry_backoff(data.get("backoff", {})),
    )


def serialize_retention_policy(policy: RetentionPolicy) -> dict[str, Any]:
    return {
        "keep_job_dirs_for_days": policy.keep_job_dirs_for_days,
        "keep_terminal_records_for_days": policy.keep_terminal_records_for_days,
        "keep_failed_job_dirs": policy.keep_failed_job_dirs,
    }


def deserialize_retention_policy(data: dict[str, Any] | None) -> RetentionPolicy:
    if data is None:
        return RetentionPolicy()
    return RetentionPolicy(
        keep_job_dirs_for_days=int(data.get("keep_job_dirs_for_days", 30)),
        keep_terminal_records_for_days=int(
            data.get("keep_terminal_records_for_days", 90)
        ),
        keep_failed_job_dirs=bool(data.get("keep_failed_job_dirs", True)),
    )


def dump_submit_request(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True)


def load_submit_request(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    return json.loads(text)
