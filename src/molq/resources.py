"""
Resource specification and mapping system for Molq.

This module provides a hierarchical, user-friendly resource specification system
based on Pydantic models that abstracts differences between various job schedulers.
"""

import re
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Type, Union

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


# Time and Memory parsing utilities
class TimeParser:
    """Parse human-readable time formats to scheduler-specific formats."""

    TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    @classmethod
    def parse_duration(cls, time_str: str) -> int:
        """
        Parse time string to seconds.

        Supports formats like:
        - "02:30:00" (HH:MM:SS)
        - "2h30m" (human readable)
        - "90m" (minutes)
        - "1.5h" (decimal hours)
        - "1 day 2h" (mixed format)
        """
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

        # Pattern to match number + unit
        pattern = r"(\d+(?:\.\d+)?)\s*([smhdw])"
        matches = re.findall(pattern, time_str.lower())

        if matches:
            for value, unit in matches:
                total_seconds += float(value) * cls.TIME_UNITS[unit]
        else:
            # Try to parse as plain number (assume seconds)
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

        # SLURM supports days as well: D-HH:MM:SS
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
        """
        Parse memory string to bytes.

        Supports formats like:
        - "8GB", "512MB", "2TB"
        - "1073741824" (plain bytes)
        - "4.5GB" (decimal values)
        """
        if not memory_str:
            return 0

        # Try to parse as plain number (bytes)
        try:
            return int(memory_str)
        except ValueError:
            pass

        # Parse with units
        pattern = r"(\d+(?:\.\d+)?)\s*([kmgtpb]*b?)"
        match = re.match(pattern, memory_str.lower())

        if match:
            value, unit = match.groups()
            unit = unit or "b"  # Default to bytes
            if unit not in cls.MEMORY_UNITS:
                unit += "b" if not unit.endswith("b") else ""

            return int(float(value) * cls.MEMORY_UNITS.get(unit, 1))

        raise ValueError(f"Cannot parse memory format: {memory_str}")

    @classmethod
    def to_slurm_format(cls, memory_str: str) -> str:
        """Convert to SLURM memory format (e.g., 8G, 512M)."""
        bytes_value = cls.parse_memory(memory_str)

        # Convert to appropriate unit
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

        # Convert to appropriate unit (PBS typically uses lowercase)
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


# Base resource specification using Pydantic
class BaseResourceSpec(BaseModel):
    """
    Base resource specification for all job types.

    This provides the minimal set of parameters needed for any job execution,
    suitable for local execution and as a foundation for more complex specs.
    """

    model_config = ConfigDict(
        extra="allow",  # Allow additional fields for scheduler-specific params
        populate_by_name=True,  # Allow aliases
        use_enum_values=True,  # Use enum values instead of enum objects
        validate_assignment=True,  # Validate on assignment
    )

    # Core execution parameters
    cmd: Union[str, List[str]] = Field(
        ..., description="Command to execute (string or list of arguments)"
    )

    workdir: Optional[Union[str, Path]] = Field(
        None,
        description="Working directory for job execution",
        alias="cwd",  # Support both workdir and cwd
    )

    # Environment and execution control
    env: Optional[Dict[str, str]] = Field(
        None, description="Environment variables to set"
    )

    block: bool = Field(True, description="Whether to wait for job completion")

    # Job identification
    job_name: Optional[str] = Field(None, description="Human-readable job name")

    # Temporary file management
    cleanup_temp_files: bool = Field(
        True,
        description="Whether to clean up temporary script files after job completion",
    )

    # Output redirection
    output_file: Optional[str] = Field(None, description="Path to standard output file")

    error_file: Optional[str] = Field(None, description="Path to standard error file")

    @field_validator("workdir")
    @classmethod
    def validate_workdir(cls, v: Optional[Union[str, Path]]) -> Optional[str]:
        """Convert workdir to string if it's a Path."""
        if isinstance(v, Path):
            return str(v)
        return v

    @staticmethod
    def validate_time_format(v: Optional[str]) -> Optional[str]:
        """Validate and normalize time format."""
        if v is None:
            return v
        if isinstance(v, str):
            try:
                TimeParser.parse_duration(v)
                return v
            except ValueError as e:
                raise ValueError(f"Invalid time format: {e}")
        return v

    @staticmethod
    def validate_memory_format(v: Optional[str]) -> Optional[str]:
        """Validate and normalize memory format."""
        if v is None:
            return v
        if isinstance(v, str):
            try:
                MemoryParser.parse_memory(v)
                return v
            except ValueError as e:
                raise ValueError(f"Invalid memory format: {e}")
        return v


class ComputeResourceSpec(BaseResourceSpec):
    """
    Resource specification for compute jobs.

    Extends BaseResourceSpec with compute-specific parameters like
    CPU count, memory requirements, and time limits.
    """

    # Compute resources
    cpu_count: Optional[int] = Field(
        None, description="Number of CPU cores required", gt=0
    )

    memory: Optional[str] = Field(
        None, description="Total memory requirement (e.g., '8GB', '512MB')"
    )

    # Time management
    time_limit: Optional[str] = Field(
        None, description="Maximum runtime (e.g., '2h30m', '02:30:00', '1d')"
    )

    @field_validator("memory")
    @classmethod
    def validate_memory(cls, v: Optional[str]) -> Optional[str]:
        """Validate memory format."""
        return BaseResourceSpec.validate_memory_format(v)

    @field_validator("time_limit")
    @classmethod
    def validate_time(cls, v: Optional[str]) -> Optional[str]:
        """Validate time format."""
        return BaseResourceSpec.validate_time_format(v)


class ClusterResourceSpec(ComputeResourceSpec):
    """
    Resource specification for cluster/HPC jobs.

    Extends ComputeResourceSpec with cluster-specific parameters like
    queues, nodes, priorities, and advanced scheduling options.
    """

    # Cluster-specific resources
    queue: Optional[str] = Field(
        None, description="Target queue/partition for job execution", alias="partition"
    )

    node_count: Optional[int] = Field(
        None, description="Number of compute nodes required", gt=0
    )

    cpu_per_node: Optional[int] = Field(
        None, description="CPU cores per compute node", gt=0
    )

    memory_per_cpu: Optional[str] = Field(
        None, description="Memory per CPU core (e.g., '2GB', '4GB')"
    )

    # GPU resources
    gpu_count: Optional[int] = Field(None, description="Number of GPUs required", ge=0)

    gpu_type: Optional[str] = Field(
        None, description="Specific GPU type (e.g., 'v100', 'a100', 'rtx3090')"
    )

    # Scheduling and priority
    priority: Optional[Union[PriorityLevel, str]] = Field(
        PriorityLevel.NORMAL, description="Job priority level"
    )

    exclusive_node: bool = Field(False, description="Request exclusive node access")

    # Time and dependencies
    dependency: Optional[Union[str, List[str]]] = Field(
        None,
        description="Job dependencies (e.g., 'ok:12345', ['after:12345', 'ok:12346'])",
    )

    begin_time: Optional[str] = Field(None, description="Earliest start time")

    # Array jobs
    array_spec: Optional[str] = Field(
        None, description="Array job specification (e.g., '1-100', '1-100:5')"
    )

    # Notifications
    email: Optional[str] = Field(None, description="Email address for notifications")

    email_events: Optional[List[Union[EmailEvent, str]]] = Field(
        None, description="Events that trigger email notifications"
    )

    # Accounting and QoS
    account: Optional[str] = Field(None, description="Billing account")

    qos: Optional[str] = Field(None, description="Quality of Service level")

    comment: Optional[str] = Field(None, description="Job description/comment")

    # Advanced options
    constraints: Optional[Union[str, List[str]]] = Field(
        None, description="Node feature constraints"
    )

    licenses: Optional[Union[str, List[str]]] = Field(
        None, description="Software licenses required"
    )

    reservation: Optional[str] = Field(None, description="Use specific reservation")

    @field_validator("memory_per_cpu")
    @classmethod
    def validate_memory_per_cpu(cls, v: Optional[str]) -> Optional[str]:
        """Validate memory per CPU format."""
        return BaseResourceSpec.validate_memory_format(v)

    @model_validator(mode="after")
    def validate_gpu_consistency(self):
        """Validate GPU-related parameters."""
        if self.gpu_type and not self.gpu_count:
            raise ValueError("gpu_type specified but gpu_count is not set")
        return self

    @model_validator(mode="after")
    def validate_cpu_consistency(self):
        """Validate CPU-related parameters."""
        if self.cpu_per_node and self.node_count and self.cpu_count:
            expected_total = self.cpu_per_node * self.node_count
            if self.cpu_count != expected_total:
                raise ValueError(
                    f"cpu_count ({self.cpu_count}) doesn't match "
                    f"cpu_per_node ({self.cpu_per_node}) * node_count ({self.node_count}) = {expected_total}"
                )
        return self


# Type alias for convenience
ResourceSpec = Union[BaseResourceSpec, ComputeResourceSpec, ClusterResourceSpec]


class SchedulerMapper:
    """Base class for scheduler-specific parameter mapping."""

    def map_resources(self, spec: ResourceSpec) -> Dict[str, str]:
        """Map unified resource spec to scheduler-specific parameters."""
        raise NotImplementedError

    def format_command_args(self, mapped_params: Dict[str, str]) -> List[str]:
        """Format mapped parameters as command line arguments."""
        raise NotImplementedError


class SlurmMapper(SchedulerMapper):
    """Map unified resources to SLURM parameters."""

    PARAMETER_MAPPING = {
        "queue": "--partition",
        "cpu_count": "--ntasks",
        "cpu_per_node": "--ntasks-per-node",
        "node_count": "--nodes",
        "memory": "--mem",
        "memory_per_cpu": "--mem-per-cpu",
        "time_limit": "--time",
        "job_name": "--job-name",
        "output_file": "--output",
        "error_file": "--error",
        "working_dir": "--chdir",
        "email": "--mail-user",
        "email_events": "--mail-type",
        "account": "--account",
        "priority": "--priority",
        "exclusive_node": "--exclusive",
        "array_spec": "--array",
        "constraints": "--constraint",
        "licenses": "--licenses",
        "reservation": "--reservation",
        "qos": "--qos",
        "dependency": "--dependency",
        "begin_time": "--begin",
        "comment": "--comment",
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

    def map_resources(self, spec: ResourceSpec) -> Dict[str, str]:
        """Map unified resource spec to SLURM parameters."""
        mapped = {}

        # Handle GPU resources separately since they need special processing
        if spec.gpu_count:
            gres_value = f"gpu:{spec.gpu_count}"
            if spec.gpu_type:
                gres_value = f"gpu:{spec.gpu_type}:{spec.gpu_count}"
            mapped["--gres"] = gres_value

        for attr, slurm_param in self.PARAMETER_MAPPING.items():
            if attr in ["gpu_count", "gpu_type"]:
                continue  # Already handled above

            value = getattr(spec, attr, None)
            if value is None:
                continue

            # Special handling for different parameter types
            if attr == "time_limit":
                mapped[slurm_param] = TimeParser.to_slurm_format(value)
            elif attr == "memory":
                mapped[slurm_param] = MemoryParser.to_slurm_format(value)
            elif attr == "memory_per_cpu":
                mapped[slurm_param] = MemoryParser.to_slurm_format(value)
            elif attr == "exclusive_node":
                if value:
                    mapped[slurm_param] = ""  # Flag parameter
            elif attr == "priority":
                if isinstance(value, PriorityLevel):
                    mapped[slurm_param] = self.PRIORITY_MAPPING[value]
                elif isinstance(value, str) and value in [
                    p.value for p in PriorityLevel
                ]:
                    priority_level = PriorityLevel(value)
                    mapped[slurm_param] = self.PRIORITY_MAPPING[priority_level]
                else:
                    mapped[slurm_param] = str(value)
            elif attr == "email_events":
                if isinstance(value, list):
                    events = []
                    for event in value:
                        if isinstance(event, EmailEvent):
                            events.append(self.EMAIL_EVENT_MAPPING[event])
                        elif isinstance(event, str):
                            event_enum = EmailEvent(event)
                            events.append(self.EMAIL_EVENT_MAPPING[event_enum])
                    mapped[slurm_param] = ",".join(events)
            elif attr == "constraints":
                if isinstance(value, list):
                    mapped[slurm_param] = "&".join(value)
                else:
                    mapped[slurm_param] = str(value)
            elif attr == "licenses":
                if isinstance(value, list):
                    mapped[slurm_param] = ",".join(value)
                else:
                    mapped[slurm_param] = str(value)
            elif attr == "dependency":
                if isinstance(value, list):
                    mapped[slurm_param] = ",".join(value)
                else:
                    mapped[slurm_param] = str(value)
            else:
                mapped[slurm_param] = str(value)

        return mapped

    def format_command_args(self, mapped_params: Dict[str, str]) -> List[str]:
        """Format mapped parameters as SLURM sbatch arguments."""
        args = []
        for param, value in mapped_params.items():
            if value == "":  # Flag parameters
                args.append(param)
            else:
                args.extend([param, value])
        return args


class PbsMapper(SchedulerMapper):
    """Map unified resources to PBS/Torque parameters."""

    PARAMETER_MAPPING = {
        "queue": "-q",
        "node_count": "-l nodes",
        "cpu_per_node": "ppn",  # Combined with nodes
        "memory": "-l mem",
        "time_limit": "-l walltime",
        "job_name": "-N",
        "output_file": "-o",
        "error_file": "-e",
        "working_dir": "-d",
        "email": "-M",
        "email_events": "-m",
        "account": "-A",
        "priority": "-p",
        "array_spec": "-t",
        "dependency": "-W depend",
    }

    EMAIL_EVENT_MAPPING = {
        EmailEvent.START: "b",
        EmailEvent.END: "e",
        EmailEvent.FAIL: "a",
        EmailEvent.ALL: "abe",
    }

    def map_resources(self, spec: ResourceSpec) -> Dict[str, str]:
        """Map unified resource spec to PBS parameters."""
        mapped = {}

        # Handle nodes and ppn combination
        if spec.node_count and spec.cpu_per_node:
            mapped["-l nodes"] = f"{spec.node_count}:ppn={spec.cpu_per_node}"
        elif spec.node_count:
            mapped["-l nodes"] = str(spec.node_count)
        elif spec.cpu_count:
            # Assume single node if only cpu_count specified
            mapped["-l nodes"] = f"1:ppn={spec.cpu_count}"

        for attr, pbs_param in self.PARAMETER_MAPPING.items():
            if attr in ["node_count", "cpu_per_node"]:
                continue  # Already handled above

            value = getattr(spec, attr, None)
            if value is None:
                continue

            if attr == "time_limit":
                mapped[pbs_param] = TimeParser.to_pbs_format(value)
            elif attr == "memory":
                mapped[pbs_param] = MemoryParser.to_pbs_format(value)
            elif attr == "email_events":
                if isinstance(value, list):
                    events = []
                    for event in value:
                        if isinstance(event, EmailEvent):
                            events.append(self.EMAIL_EVENT_MAPPING.get(event, ""))
                        elif isinstance(event, str):
                            event_enum = EmailEvent(event)
                            events.append(self.EMAIL_EVENT_MAPPING.get(event_enum, ""))
                    mapped[pbs_param] = "".join(events)
            else:
                mapped[pbs_param] = str(value)

        return mapped

    def format_command_args(self, mapped_params: Dict[str, str]) -> List[str]:
        """Format mapped parameters as PBS qsub arguments."""
        args = []
        for param, value in mapped_params.items():
            args.extend([param, value])
        return args


class LsfMapper(SchedulerMapper):
    """Map unified resources to LSF parameters."""

    PARAMETER_MAPPING = {
        "queue": "-q",
        "cpu_count": "-n",
        "memory": "-M",
        "time_limit": "-W",
        "job_name": "-J",
        "output_file": "-o",
        "error_file": "-e",
        "working_dir": "-cwd",
        "email": "-u",
        "account": "-P",
        "exclusive_node": "-x",
        "dependency": "-w",
    }

    def map_resources(self, spec: ResourceSpec) -> Dict[str, str]:
        """Map unified resource spec to LSF parameters."""
        mapped = {}

        # Handle array jobs specially
        array_handled = False
        if spec.array_spec:
            job_name = spec.job_name or "job"
            mapped["-J"] = f"{job_name}[{spec.array_spec}]"
            array_handled = True

        for attr, lsf_param in self.PARAMETER_MAPPING.items():
            if attr == "array_spec":
                continue  # Already handled above
            if attr == "job_name" and array_handled:
                continue  # Already handled in array spec

            value = getattr(spec, attr, None)
            if value is None:
                continue

            if attr == "time_limit":
                # LSF uses minutes
                seconds = TimeParser.parse_duration(value)
                mapped[lsf_param] = str(seconds // 60)
            elif attr == "memory":
                # LSF uses KB
                bytes_value = MemoryParser.parse_memory(value)
                mapped[lsf_param] = str(bytes_value // 1024)
            elif attr == "exclusive_node":
                if value:
                    mapped[lsf_param] = ""  # Flag parameter
            else:
                mapped[lsf_param] = str(value)

        # Handle email notifications
        if spec.email_events:
            email_flags = []
            if EmailEvent.START in spec.email_events or "start" in spec.email_events:
                email_flags.append("-B")
            if EmailEvent.END in spec.email_events or "end" in spec.email_events:
                email_flags.append("-N")
            mapped.update({flag: "" for flag in email_flags})

        return mapped

    def format_command_args(self, mapped_params: Dict[str, str]) -> List[str]:
        """Format mapped parameters as LSF bsub arguments."""
        args = []
        for param, value in mapped_params.items():
            if value == "":  # Flag parameters
                args.append(param)
            else:
                args.extend([param, value])
        return args


class ResourceManager:
    """Main interface for resource specification and mapping."""

    MAPPER_REGISTRY = {
        "slurm": SlurmMapper,
        "pbs": PbsMapper,
        "torque": PbsMapper,  # Torque uses PBS format
        "lsf": LsfMapper,
    }

    @classmethod
    def create_spec(
        cls, spec_type: str = "cluster", **kwargs
    ) -> Union[BaseResourceSpec, ComputeResourceSpec, ClusterResourceSpec]:
        """Create a resource specification of the appropriate type."""
        if spec_type == "base":
            return BaseResourceSpec(**kwargs)
        elif spec_type == "compute":
            return ComputeResourceSpec(**kwargs)
        elif spec_type == "cluster":
            return ClusterResourceSpec(**kwargs)
        else:
            raise ValueError(
                f"Unknown spec type: {spec_type}. Use 'base', 'compute', or 'cluster'."
            )

    @classmethod
    def get_mapper(cls, scheduler_type: str) -> SchedulerMapper:
        """Get mapper for specific scheduler type."""
        mapper_class = cls.MAPPER_REGISTRY.get(scheduler_type.lower())
        if not mapper_class:
            raise ValueError(f"Unsupported scheduler type: {scheduler_type}")
        return mapper_class()

    @classmethod
    def map_to_scheduler(
        cls, spec: ResourceSpec, scheduler_type: str
    ) -> Dict[str, str]:
        """Map resource spec to scheduler-specific parameters."""
        mapper = cls.get_mapper(scheduler_type)
        return mapper.map_resources(spec)

    @classmethod
    def format_command_args(cls, spec: ResourceSpec, scheduler_type: str) -> List[str]:
        """Format resource spec as command line arguments for scheduler."""
        mapper = cls.get_mapper(scheduler_type)
        mapped_params = mapper.map_resources(spec)
        return mapper.format_command_args(mapped_params)


# Convenience functions for common use cases
def create_compute_job(
    cpu_count: int,
    memory: str,
    time_limit: str,
    cmd: Union[str, List[str]],
    queue: str = "compute",
    **kwargs,
) -> ClusterResourceSpec:
    """Create a standard compute job specification."""
    return ClusterResourceSpec(
        cmd=cmd,
        queue=queue,
        cpu_count=cpu_count,
        memory=memory,
        time_limit=time_limit,
        **kwargs,
    )


def create_gpu_job(
    cmd: Union[str, List[str]],
    gpu_count: int,
    cpu_count: int,
    memory: str,
    time_limit: str,
    gpu_type: Optional[str] = None,
    queue: str = "gpu",
    **kwargs,
) -> ClusterResourceSpec:
    """Create a GPU job specification."""
    return ClusterResourceSpec(
        cmd=cmd,
        queue=queue,
        gpu_count=gpu_count,
        gpu_type=gpu_type,
        cpu_count=cpu_count,
        memory=memory,
        time_limit=time_limit,
        **kwargs,
    )


def create_array_job(
    cmd: Union[str, List[str]],
    array_spec: str,
    cpu_count: int,
    memory: str,
    time_limit: str,
    queue: str = "compute",
    **kwargs,
) -> ClusterResourceSpec:
    """Create an array job specification."""
    return ClusterResourceSpec(
        cmd=cmd,
        queue=queue,
        cpu_count=cpu_count,
        memory=memory,
        time_limit=time_limit,
        array_spec=array_spec,
        **kwargs,
    )


def create_high_memory_job(
    cmd: Union[str, List[str]],
    memory: str,
    cpu_count: int,
    time_limit: str,
    queue: str = "highmem",
    exclusive_node: bool = True,
    **kwargs,
) -> ClusterResourceSpec:
    """Create a high memory job specification."""
    return ClusterResourceSpec(
        cmd=cmd,
        queue=queue,
        cpu_count=cpu_count,
        memory=memory,
        time_limit=time_limit,
        exclusive_node=exclusive_node,
        **kwargs,
    )
