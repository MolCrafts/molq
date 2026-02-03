"""
Resource specification and mapping system for Molq.

This module provides a hierarchical, user-friendly resource specification system
based on Pydantic models that abstracts differences between various job schedulers.
"""

import re
from enum import Enum
from pathlib import Path

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class PriorityLevel(str, Enum):
    """Standardized priority levels."""

    URGENT = "urgent"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"
    IDLE = "idle"


class EmailEvent(str, Enum):
    """Standardized email notification events."""

    START = "start"
    END = "end"
    FAIL = "fail"
    SUCCESS = "success"
    TIMEOUT = "timeout"
    CANCEL = "cancel"
    REQUEUE = "requeue"
    ALL = "all"


class TimeParser:
    """Parse human-readable time formats to scheduler-specific formats."""

    TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    @classmethod
    def parse_duration(cls, time_str: str) -> int:
        """Parse time string to seconds."""
        if not time_str:
            return 0

        # Handle HH:MM:SS format
        if ":" in time_str:
            parts = time_str.split(":")
            if len(parts) == 3:
                h, m, s = map(int, parts)
                return h * 3600 + m * 60 + s
            elif len(parts) == 2:
                m, s = map(int, parts)
                return m * 60 + s

        # Handle human-readable format
        total_seconds = 0
        pattern = r"(\d+(?:\.\d+)?)\s*([smhdw])"
        matches = re.findall(pattern, time_str.lower())

        if matches:
            for value, unit in matches:
                total_seconds += float(value) * cls.TIME_UNITS[unit]
        else:
            try:
                total_seconds = float(time_str)
            except ValueError:
                raise ValueError(f"Cannot parse time format: {time_str}")

        return int(total_seconds)

    @classmethod
    def to_slurm_format(cls, time_str: str) -> str:
        """Convert to SLURM time format (HH:MM:SS)."""
        seconds = cls.parse_duration(time_str)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60

        if hours >= 24:
            days = hours // 24
            hours = hours % 24
            return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    @classmethod
    def to_pbs_format(cls, time_str: str) -> str:
        """Convert to PBS time format (HH:MM:SS)."""
        seconds = cls.parse_duration(time_str)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class MemoryParser:
    """Parse memory specifications to scheduler-specific formats."""

    MEMORY_UNITS = {
        "b": 1,
        "kb": 1024,
        "mb": 1024**2,
        "gb": 1024**3,
        "tb": 1024**4,
        "pb": 1024**5,
    }

    @classmethod
    def parse_memory(cls, memory_str: str) -> int:
        """Parse memory string to bytes."""
        if not memory_str:
            return 0

        try:
            return int(memory_str)
        except ValueError:
            pass

        pattern = r"(\d+(?:\.\d+)?)\s*([kmgtpb]*b?)"
        match = re.match(pattern, memory_str.lower())

        if match:
            value, unit = match.groups()
            unit = unit or "b"
            if unit not in cls.MEMORY_UNITS:
                unit += "b" if not unit.endswith("b") else ""
            return int(float(value) * cls.MEMORY_UNITS.get(unit, 1))

        raise ValueError(f"Cannot parse memory format: {memory_str}")

    @classmethod
    def to_slurm_format(cls, memory_str: str) -> str:
        """Convert to SLURM memory format (e.g., 8G, 512M)."""
        bytes_value = cls.parse_memory(memory_str)

        if bytes_value >= cls.MEMORY_UNITS["tb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['tb']}T"
        elif bytes_value >= cls.MEMORY_UNITS["gb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['gb']}G"
        elif bytes_value >= cls.MEMORY_UNITS["mb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['mb']}M"
        elif bytes_value >= cls.MEMORY_UNITS["kb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['kb']}K"
        else:
            return str(bytes_value)

    @classmethod
    def to_pbs_format(cls, memory_str: str) -> str:
        """Convert to PBS memory format (e.g., 8gb, 512mb)."""
        bytes_value = cls.parse_memory(memory_str)

        if bytes_value >= cls.MEMORY_UNITS["tb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['tb']}tb"
        elif bytes_value >= cls.MEMORY_UNITS["gb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['gb']}gb"
        elif bytes_value >= cls.MEMORY_UNITS["mb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['mb']}mb"
        elif bytes_value >= cls.MEMORY_UNITS["kb"]:
            return f"{bytes_value // cls.MEMORY_UNITS['kb']}kb"
        else:
            return f"{bytes_value}b"


class ResourceSpec(BaseModel):
    """Hardware requirements for job execution."""

    model_config = ConfigDict(validate_assignment=True)

    cpu_count: int | None = Field(None, description="Number of CPU cores required", gt=0)
    memory: str | None = Field(None, description="Total memory requirement (e.g., '8GB')")
    gpu_count: int | None = Field(None, description="Number of GPUs required", ge=0)
    gpu_type: str | None = Field(None, description="Specific GPU type (e.g., 'v100')")
    time_limit: str | None = Field(None, description="Maximum runtime (e.g., '2h30m')")
    extra: dict[str, Any] = Field(default_factory=dict, description="Implementation-specific extra parameters")

    @field_validator("memory")
    @classmethod
    def validate_memory(cls, v: str | None) -> str | None:
        if v: MemoryParser.parse_memory(v)
        return v

    @field_validator("time_limit")
    @classmethod
    def validate_time_limit(cls, v: str | None) -> str | None:
        if v: TimeParser.parse_duration(v)
        return v


class ExecutionSpec(BaseModel):
    """Execution environment and command parameters."""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    cmd: str | list[str] = Field(..., description="Command to execute")
    workdir: str | Path | None = Field(
        None, description="Working directory", alias="cwd"
    )
    env: dict[str, str] | None = Field(None, description="Environment variables")
    block: bool = Field(True, description="Wait for job completion")
    job_name: str | None = Field(None, description="Human-readable job name")
    cleanup_temp_files: bool = Field(True, description="Clean up temporary files")
    output_file: str | None = Field(None, description="STDOUT file path")
    error_file: str | None = Field(None, description="STDERR file path")
    extra: dict[str, Any] = Field(default_factory=dict, description="Implementation-specific extra parameters")

    @field_validator("workdir")
    @classmethod
    def validate_workdir(cls, v: str | Path | None) -> str | None:
        return str(v) if isinstance(v, Path) else v


class ClusterSpec(BaseModel):
    """Scheduler-specific settings for cluster jobs."""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    queue: str | None = Field(None, description="Target queue/partition", alias="partition")
    account: str | None = Field(None, description="Billing account")
    qos: str | None = Field(None, description="Quality of Service level")
    priority: PriorityLevel | str | None = Field(PriorityLevel.NORMAL, description="Job priority")
    exclusive_node: bool = Field(False, description="Exclusive node access")
    node_count: int | None = Field(None, description="Number of compute nodes", gt=0)
    cpu_per_node: int | None = Field(None, description="CPU cores per node", gt=0)
    memory_per_cpu: str | None = Field(None, description="Memory per CPU core")
    dependency: str | list[str] | None = Field(None, description="Job dependencies")
    begin_time: str | None = Field(None, description="Earliest start time")
    array_spec: str | None = Field(None, description="Array job specification")
    email: str | None = Field(None, description="Email for notifications")
    email_events: list[EmailEvent | str] | None = Field(None, description="Notification events")
    constraints: str | list[str] | None = Field(None, description="Node feature constraints")
    licenses: str | list[str] | None = Field(None, description="Software licenses required")
    reservation: str | None = Field(None, description="Specific reservation")
    comment: str | None = Field(None, description="Job comment")
    extra: dict[str, Any] = Field(default_factory=dict, description="Implementation-specific extra parameters")

    @field_validator("memory_per_cpu")
    @classmethod
    def validate_memory_per_cpu(cls, v: str | None) -> str | None:
        if v: MemoryParser.parse_memory(v)
        return v


class JobSpec(BaseModel):
    """Unified job specification composing orthogonal specs."""

    execution: ExecutionSpec
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    cluster: ClusterSpec = Field(default_factory=ClusterSpec)

    @model_validator(mode="after")
    def validate_consistency(self) -> "JobSpec":
        if self.resources.gpu_type and not self.resources.gpu_count:
            raise ValueError("gpu_type specified but gpu_count is not set")
        
        if self.cluster.cpu_per_node and self.cluster.node_count and self.resources.cpu_count:
            expected_total = self.cluster.cpu_per_node * self.cluster.node_count
            if self.resources.cpu_count != expected_total:
                raise ValueError(
                    f"cpu_count ({self.resources.cpu_count}) doesn't match "
                    f"cpu_per_node * node_count = {expected_total}"
                )
        return self


class SchedulerMapper:
    """Base class for scheduler-specific parameter mapping."""

    def map_resources(self, spec: JobSpec) -> dict[str, str]:
        raise NotImplementedError

    def format_command_args(self, mapped_params: dict[str, str]) -> list[str]:
        raise NotImplementedError


class SlurmMapper(SchedulerMapper):
    """Map unified resources to SLURM parameters."""

    PARAMETER_MAPPING = {
        "cluster.queue": "--partition",
        "resources.cpu_count": "--ntasks",
        "cluster.cpu_per_node": "--ntasks-per-node",
        "cluster.node_count": "--nodes",
        "resources.memory": "--mem",
        "cluster.memory_per_cpu": "--mem-per-cpu",
        "resources.time_limit": "--time",
        "execution.job_name": "--job-name",
        "execution.output_file": "--output",
        "execution.error_file": "--error",
        "execution.workdir": "--chdir",
        "cluster.email": "--mail-user",
        "cluster.email_events": "--mail-type",
        "cluster.account": "--account",
        "cluster.priority": "--priority",
        "cluster.exclusive_node": "--exclusive",
        "cluster.array_spec": "--array",
        "cluster.constraints": "--constraint",
        "cluster.licenses": "--licenses",
        "cluster.reservation": "--reservation",
        "cluster.qos": "--qos",
        "cluster.dependency": "--dependency",
        "cluster.begin_time": "--begin",
        "cluster.comment": "--comment",
    }

    PRIORITY_MAPPING = {
        PriorityLevel.URGENT: "1000",
        PriorityLevel.HIGH: "750",
        PriorityLevel.NORMAL: "500",
        PriorityLevel.LOW: "250",
        PriorityLevel.IDLE: "100",
    }

    EMAIL_EVENT_MAPPING = {
        EmailEvent.START: "BEGIN",
        EmailEvent.END: "END",
        EmailEvent.FAIL: "FAIL",
        EmailEvent.SUCCESS: "END",
        EmailEvent.TIMEOUT: "TIME_LIMIT",
        EmailEvent.CANCEL: "FAIL",
        EmailEvent.REQUEUE: "REQUEUE",
        EmailEvent.ALL: "ALL",
    }

    def map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped = {}

        if spec.resources.gpu_count:
            gres_value = f"gpu:{spec.resources.gpu_count}"
            if spec.resources.gpu_type:
                gres_value = f"gpu:{spec.resources.gpu_type}:{spec.resources.gpu_count}"
            mapped["--gres"] = gres_value

        for path, slurm_param in self.PARAMETER_MAPPING.items():
            parts = path.split(".")
            value = getattr(getattr(spec, parts[0]), parts[1], None)
            
            if value is None:
                continue

            if parts[1] == "time_limit":
                mapped[slurm_param] = TimeParser.to_slurm_format(value)
            elif parts[1] == "memory" or parts[1] == "memory_per_cpu":
                mapped[slurm_param] = MemoryParser.to_slurm_format(value)
            elif parts[1] == "exclusive_node":
                if value: mapped[slurm_param] = ""
            elif parts[1] == "priority":
                if isinstance(value, PriorityLevel):
                    mapped[slurm_param] = self.PRIORITY_MAPPING[value]
                else:
                    mapped[slurm_param] = str(value)
            elif parts[1] == "email_events" and isinstance(value, list):
                events = [self.EMAIL_EVENT_MAPPING.get(e, str(e).upper()) for e in value]
                mapped[slurm_param] = ",".join(events)
            elif isinstance(value, list):
                mapped[slurm_param] = ",".join(map(str, value))
            else:
                mapped[slurm_param] = str(value)

        return mapped

    def format_command_args(self, mapped_params: dict[str, str]) -> list[str]:
        args = []
        for param, value in mapped_params.items():
            if value == "": args.append(param)
            else: args.extend([param, value])
        return args


class PbsMapper(SchedulerMapper):
    """Map unified resources to PBS/Torque parameters."""

    def map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped = {}
        
        # Resources line: -l nodes=X:ppn=Y,mem=Z,walltime=T
        resource_list = []
        
        node_count = spec.cluster.node_count or 1
        ppn = spec.cluster.cpu_per_node or (spec.resources.cpu_count if spec.cluster.node_count is None else None)
        
        if ppn:
            resource_list.append(f"nodes={node_count}:ppn={ppn}")
        else:
            resource_list.append(f"nodes={node_count}")
            
        if spec.resources.memory:
            resource_list.append(f"mem={MemoryParser.to_pbs_format(spec.resources.memory)}")
            
        if spec.resources.time_limit:
            resource_list.append(f"walltime={TimeParser.to_pbs_format(spec.resources.time_limit)}")
            
        mapped["-l"] = ",".join(resource_list)
        
        if spec.execution.job_name: mapped["-N"] = spec.execution.job_name
        if spec.execution.output_file: mapped["-o"] = spec.execution.output_file
        if spec.execution.error_file: mapped["-e"] = spec.execution.error_file
        if spec.cluster.queue: mapped["-q"] = spec.cluster.queue
        
        return mapped

    def format_command_args(self, mapped_params: dict[str, str]) -> list[str]:
        args = []
        for param, value in mapped_params.items():
            if param == "-l":
                # PBS -l can be specified multiple times or as a comma-separated list
                for item in value.split(","):
                    args.extend(["-l", item])
            else:
                args.extend([param, value])
        return args


class LsfMapper(SchedulerMapper):
    """Map unified resources to LSF parameters."""

    def map_resources(self, spec: JobSpec) -> dict[str, str]:
        mapped = {}
        
        if spec.cluster.queue: mapped["-q"] = spec.cluster.queue
        if spec.resources.cpu_count: mapped["-n"] = str(spec.resources.cpu_count)
        
        if spec.resources.memory:
            # LSF -M takes memory in KB by default usually, but depends on config. 
            # We'll assume KB for this mapper.
            bytes_val = MemoryParser.parse_memory(spec.resources.memory)
            mapped["-M"] = str(bytes_val // 1024)
            
        if spec.resources.time_limit:
            # LSF -W takes time in minutes
            minutes = TimeParser.parse_duration(spec.resources.time_limit) // 60
            mapped["-W"] = str(minutes)
            
        if spec.execution.job_name:
            name = spec.execution.job_name
            if spec.cluster.array_spec:
                name = f"{name}[{spec.cluster.array_spec}]"
            mapped["-J"] = name
            
        return mapped

    def format_command_args(self, mapped_params: dict[str, str]) -> list[str]:
        args = []
        for param, value in mapped_params.items():
            args.extend([param, value])
        return args


class ResourceManager:
    """Main interface for resource specification and mapping."""

    MAPPER_REGISTRY = {
        "slurm": SlurmMapper,
        "pbs": PbsMapper,
        "lsf": LsfMapper,
    }

    @classmethod
    def get_mapper(cls, scheduler_type: str) -> SchedulerMapper:
        mapper_class = cls.MAPPER_REGISTRY.get(scheduler_type.lower())
        if not mapper_class:
            raise ValueError(f"Unsupported scheduler type: {scheduler_type}")
        return mapper_class()

    @classmethod
    def map_to_scheduler(cls, spec: JobSpec, scheduler_type: str) -> dict[str, str]:
        return cls.get_mapper(scheduler_type).map_resources(spec)

    @classmethod
    def format_command_args(cls, spec: JobSpec, scheduler_type: str) -> list[str]:
        mapper = cls.get_mapper(scheduler_type)
        mapped = mapper.map_resources(spec)
        return mapper.format_command_args(mapped)


def create_compute_job(cmd: str | list[str], cpu_count: int | None = None, memory: str | None = None, time_limit: str | None = None, job_name: str | None = None, **kwargs) -> JobSpec:
    return JobSpec(
        execution=ExecutionSpec(cmd=cmd, job_name=job_name, **kwargs),
        resources=ResourceSpec(cpu_count=cpu_count, memory=memory, time_limit=time_limit)
    )

def create_gpu_job(cmd: str | list[str], gpu_count: int = 1, gpu_type: str | None = None, cpu_count: int | None = None, memory: str | None = None, time_limit: str | None = None, **kwargs) -> JobSpec:
    return JobSpec(
        execution=ExecutionSpec(cmd=cmd, **kwargs),
        resources=ResourceSpec(gpu_count=gpu_count, gpu_type=gpu_type, cpu_count=cpu_count, memory=memory, time_limit=time_limit)
    )

def create_array_job(cmd: str | list[str], array_spec: str, **kwargs) -> JobSpec:
    spec = create_compute_job(cmd, **kwargs)
    spec.cluster.array_spec = array_spec
    return spec

def create_high_memory_job(cmd: str | list[str], memory: str, **kwargs) -> JobSpec:
    spec = create_compute_job(cmd, memory=memory, **kwargs)
    spec.cluster.exclusive_node = True
    return spec
