"""Immutable client value constructors; no rendering or lifecycle logic."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def _wire(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        value = asdict(value)
    if isinstance(value, dict):
        return {k: _wire(v) for k, v in value.items() if v is not None}
    if isinstance(value, (list, tuple)):
        return [_wire(v) for v in value]
    return value


class Value:
    """Project immutable value fields into protocol JSON."""

    def to_wire(self) -> dict[str, Any]:
        """Return fields without defaults merging or semantic interpretation.

        Returns:
            dict[str, Any]: JSON-shaped fields with None fields omitted and tuples
                converted to arrays. Validation remains a runtime operation.
        """
        return _wire(self)


@dataclass(frozen=True)
class Accelerator(Value):
    """A formal allocation resource quantity, with explicit scope.

    Args:
        kind (str): "gpu" or "nvidia_mps"; no automatic resource fallback.
        quantity (int): Positive resource quantity in the declared scope.
        scope (str): "per_node" or "total".
        model (str | None): Exact native model selector, if specified.
    """

    kind: str
    quantity: int
    scope: str = "per_node"
    model: str | None = None


@dataclass(frozen=True)
class Resources(Value):
    """Allocation budget; omitted fields are resolved by the runtime.

    Args:
        nodes (int | None): Positive allocation node count.
        tasks (int | None): Positive total task budget.
        cpus_per_task (int | None): Positive CPU budget per task.
        memory_bytes (str | None): Positive per-node bytes as a decimal string.
        time_limit_seconds (int | None): Positive allocation time limit in seconds.
        accelerators (tuple[Accelerator, ...] | None): Explicit accelerator requests.
    """

    nodes: int | None = None
    tasks: int | None = None
    cpus_per_task: int | None = None
    memory_bytes: str | None = None
    time_limit_seconds: int | None = None
    accelerators: tuple[Accelerator, ...] | None = None


@dataclass(frozen=True)
class JobArray(Value):
    """Indices and optional concurrency limit for a native Slurm job array.

    Args:
        indices (tuple[int, ...]): One to 64 unique nonnegative indices.
        max_concurrent (int | None): Maximum simultaneously running elements.
    """

    indices: tuple[int, ...]
    max_concurrent: int | None = None


@dataclass(frozen=True)
class Scheduling(Value):
    """Scheduling intent, independent of execution content.

    Args:
        name (str | None): Native allocation name.
        comment (str | None): Native allocation comment.
        partition (str | None): Exact native partition selector.
        account (str | None): Native billing account.
        qos (str | None): Native quality-of-service selector.
        reservation (str | None): Native reservation selector.
        priority (int | None): Nonnegative native priority request.
        exclusive (bool | None): Whether to request exclusive allocation.
        dependencies (tuple[dict[str, Any], ...] | None): Each dependency supplies
            a registry-qualified ref and an explicit condition: after,
            after_success, after_failure or after_started.
        array (JobArray | None): Optional native Slurm array.
    """

    name: str | None = None
    comment: str | None = None
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    reservation: str | None = None
    priority: int | None = None
    exclusive: bool | None = None
    dependencies: tuple[dict[str, Any], ...] | None = None
    array: JobArray | None = None


@dataclass(frozen=True)
class Launch(Value):
    """Direct or explicitly configured MPI launch intent.

    Args:
        kind (str): "direct" or "mpi".
        ranks (int | None): Positive MPI rank count when requested.
        threads (int | None): Positive thread count per launched process.
    """

    kind: str = "direct"
    ranks: int | None = None
    threads: int | None = None


@dataclass(frozen=True)
class ExecutionUnit(Value):
    """One allocation-internal command and optional placement.

    Args:
        id (str): Unique unit identifier referenced by the execution plan.
        command (dict[str, Any]): Canonical argv, shell or script command value.
        env (dict[str, str] | None): Unit environment additions or overrides.
        cwd (str | None): Target working directory for this unit.
        stdout (str | None): Explicit target stdout path.
        stderr (str | None): Explicit target stderr path.
        launch (Launch | None): Direct or MPI process launch request.
        placement (dict[str, int] | None): Explicit tasks/cpus_per_task budget
            within the allocation.
    """

    id: str
    command: dict[str, Any]
    env: dict[str, str] | None = None
    cwd: str | None = None
    stdout: str | None = None
    stderr: str | None = None
    launch: Launch | None = None
    placement: dict[str, int] | None = None

    @classmethod
    def argv(cls, id: str, argv: list[str], **kwargs: Any) -> ExecutionUnit:
        """Construct a command without shell interpretation.

        Args:
            id (str): Unique unit identifier.
            argv (list[str]): Executable and its literal argument vector.
            **kwargs (Any): Additional ExecutionUnit constructor fields.

        Returns:
            ExecutionUnit: An argv command holding a copy of the argument list.

        Raises:
            TypeError: Additional fields do not match the constructor signature.
        """
        return cls(id, {"kind": "argv", "argv": list(argv)}, **kwargs)

    @classmethod
    def shell(cls, id: str, command: str, **kwargs: Any) -> ExecutionUnit:
        """Construct explicit shell content, interpreted only on the target.

        Args:
            id (str): Unique unit identifier.
            command (str): Shell command text to run on the target.
            **kwargs (Any): Additional ExecutionUnit constructor fields.

        Returns:
            ExecutionUnit: An explicitly requested shell command.

        Raises:
            TypeError: Additional fields do not match the constructor signature.
        """
        return cls(id, {"kind": "shell", "command": command}, **kwargs)

    @classmethod
    def script(cls, id: str, text: str, **kwargs: Any) -> ExecutionUnit:
        """Construct inline payload script content.

        Args:
            id (str): Unique unit identifier.
            text (str): Script payload rendered into the native submission.
            **kwargs (Any): Additional ExecutionUnit constructor fields.

        Returns:
            ExecutionUnit: An inline script command.

        Raises:
            TypeError: Additional fields do not match the constructor signature.
        """
        return cls(id, {"kind": "script", "text": text}, **kwargs)


@dataclass(frozen=True)
class UnitRef(Value):
    """Reference one execution unit in a compositional plan.

    Args:
        ref (str): Identifier of a unit in the execution's units tuple.
        kind (str): Wire discriminator; must remain "unit".
    """

    ref: str
    kind: str = "unit"


@dataclass(frozen=True)
class Sequence(Value):
    """Run child plans in order, stopping on failure.

    Args:
        children (tuple[Any, ...]): Nonempty sequence of UnitRef, Sequence,
            Parallel or equivalent wire plan dictionaries.
        kind (str): Wire discriminator; must remain "sequence".
    """

    children: tuple[Any, ...]
    kind: str = "sequence"


@dataclass(frozen=True)
class Parallel(Value):
    """Run child plans concurrently, awaiting all started children.

    Args:
        children (tuple[Any, ...]): Nonempty sequence of UnitRef, Sequence,
            Parallel or equivalent wire plan dictionaries.
        kind (str): Wire discriminator; must remain "parallel".
    """

    children: tuple[Any, ...]
    kind: str = "parallel"


@dataclass(frozen=True)
class Execution(Value):
    """One or more units with an optional compositional plan.

    Args:
        units (tuple[ExecutionUnit, ...]): Nonempty tuple of uniquely named units.
        plan (UnitRef | Sequence | Parallel | None): Plan referencing every unit
            exactly once; required when more than one unit is supplied.
        env (dict[str, str] | None): Execution-wide environment additions.
        cwd (str | None): Execution-wide target working directory.
    """

    units: tuple[ExecutionUnit, ...]
    plan: UnitRef | Sequence | Parallel | None = None
    env: dict[str, str] | None = None
    cwd: str | None = None


@dataclass(frozen=True)
class JobSpec(Value):
    """Orthogonal allocation, scheduling and execution request values.

    Args:
        execution (Execution): Allocation-internal commands and composition.
        resources (Resources | None): Explicit allocation budget, if supplied.
        scheduling (Scheduling | None): Explicit native scheduling intent.

    Constructors do not validate native representation, scheduling resources or
    permissions. Use a cluster's validate or preview method before submission
    when representation validation is required.
    """

    execution: Execution
    resources: Resources | None = None
    scheduling: Scheduling | None = None

    @classmethod
    def argv(
        cls,
        argv: list[str],
        *,
        resources: Resources | None = None,
        scheduling: Scheduling | None = None,
    ) -> JobSpec:
        """Construct a specification for one literal argument-vector command.

        Args:
            argv (list[str]): Executable and its literal arguments, copied into
                the single execution unit.
            resources (Resources | None): Optional allocation budget.
            scheduling (Scheduling | None): Optional queue placement intent.

        Returns:
            JobSpec: A single-unit request without validation or defaults merging.
        """
        return cls(
            execution=Execution((ExecutionUnit.argv("main", argv),)),
            resources=resources,
            scheduling=scheduling,
        )
