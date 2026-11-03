"""Generated from protocol/contract.json; edit its declarative source."""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict


class Command0(TypedDict):
    """Canonical wire shape."""

    kind: Literal["argv"]
    argv: list[str]


class Command1(TypedDict):
    """Canonical wire shape."""

    kind: Literal["shell"]
    command: str


class Command2(TypedDict):
    """Canonical wire shape."""

    kind: Literal["script"]
    text: str


Command = Command0 | Command1 | Command2


class Accelerator(TypedDict):
    """Canonical wire shape."""

    kind: Literal["gpu", "nvidia_mps"]
    quantity: int
    scope: Literal["per_node", "total"]
    model: NotRequired[str]


class Resources(TypedDict):
    """Canonical wire shape."""

    nodes: NotRequired[int]
    tasks: NotRequired[int]
    cpus_per_task: NotRequired[int]
    memory_bytes: NotRequired[str]
    time_limit_seconds: NotRequired[int]
    accelerators: NotRequired[list[Accelerator]]


class FileLocation(TypedDict):
    """Canonical wire shape."""

    cluster: NotRequired[str]
    path: str
    root: NotRequired[str]


class JobRef(TypedDict):
    """Canonical wire shape."""

    registry_id: str
    cluster_id: str
    native_id: str
    incarnation: str


class Dependency(TypedDict):
    """Canonical wire shape."""

    ref: JobRef
    condition: Literal["after", "after_success", "after_failure", "after_started"]


class Scheduling(TypedDict):
    """Canonical wire shape."""

    name: NotRequired[str]
    comment: NotRequired[str]
    partition: NotRequired[str]
    account: NotRequired[str]
    qos: NotRequired[str]
    reservation: NotRequired[str]
    priority: NotRequired[int]
    exclusive: NotRequired[bool]
    dependencies: NotRequired[list[Dependency]]


class Launch(TypedDict):
    """Canonical wire shape."""

    kind: Literal["direct", "mpi"]
    ranks: NotRequired[int]
    threads: NotRequired[int]


class Placement(TypedDict):
    """Canonical wire shape."""

    tasks: NotRequired[int]
    cpus_per_task: NotRequired[int]


class ExecutionUnit(TypedDict):
    """Canonical wire shape."""

    id: str
    command: Command
    env: NotRequired[dict[str, str]]
    cwd: NotRequired[str]
    stdout: NotRequired[str]
    stderr: NotRequired[str]
    launch: NotRequired[Launch]
    placement: NotRequired[Placement]


class Plan0(TypedDict):
    """Canonical wire shape."""

    kind: Literal["unit"]
    ref: str


class Plan1(TypedDict):
    """Canonical wire shape."""

    kind: Literal["sequence", "parallel"]
    children: list[Plan]


Plan = Plan0 | Plan1


class Execution(TypedDict):
    """Canonical wire shape."""

    units: list[ExecutionUnit]
    plan: NotRequired[Plan]
    env: NotRequired[dict[str, str]]
    cwd: NotRequired[str]


class JobSpec(TypedDict):
    """Canonical wire shape."""

    resources: NotRequired[Resources]
    scheduling: NotRequired[Scheduling]
    execution: Execution


class Defaults(TypedDict):
    """Canonical wire shape."""

    resources: NotRequired[Resources]
    scheduling: NotRequired[Scheduling]
    env: NotRequired[dict[str, str]]
    cwd: NotRequired[str]


class TransportDefinition0(TypedDict):
    """Canonical wire shape."""

    kind: Literal["local"]


class TransportDefinition1(TypedDict):
    """Canonical wire shape."""

    kind: Literal["ssh"]
    alias: str


TransportDefinition = TransportDefinition0 | TransportDefinition1


class ClusterDefinition(TypedDict):
    """Canonical wire shape."""

    name: str
    scheduler: Literal["shell", "slurm", "pbs", "lsf"]
    transport: NotRequired[TransportDefinition]
    defaults: NotRequired[Defaults]
    launcher: NotRequired[Literal["srun", "mpirun", "mpiexec"]]
    pbs_dialect: NotRequired[Literal["openpbs"]]


class ErrorData(TypedDict):
    """Canonical wire shape."""

    kind: str
    message: str
    outcome: Literal["not_applied", "applied", "unknown"]
    context: dict[str, Any]


class Freshness(TypedDict):
    """Canonical wire shape."""

    status: Literal["live", "cached", "stale", "unavailable"]
    age_seconds: float


class Completion(TypedDict):
    """Canonical wire shape."""

    all_confirmed_terminal: bool
    successful: bool | None


class JobSnapshot(TypedDict):
    """Canonical wire shape."""

    ref: JobRef
    state: Literal[
        "queued", "running", "succeeded", "failed", "cancelled", "timed_out", "unknown"
    ]
    terminal: bool
    observed_at: str
    freshness: Freshness
    source: Literal["queue", "accounting", "shell_receipt", "absent", "unavailable"]
    raw_state: str
    exit_code: int | None
    name: NotRequired[str | None]
    comment: NotRequired[str | None]
    submitted_at: NotRequired[str | None]
    started_at: NotRequired[str | None]
    ended_at: NotRequired[str | None]
    error: NotRequired[ErrorData]
    last_known: NotRequired[JobSnapshot]


class Coverage(TypedDict):
    """Canonical wire shape."""

    complete: bool
    errors: list[ErrorData]


class ObservedChange(TypedDict):
    """Canonical wire shape."""

    cursor: str
    observed_at: str
    ref: JobRef
    previous_state: str | None
    state: str
    terminal: bool


class ObservationResultquery(TypedDict):
    """Canonical wire shape."""

    since: str
    until: str
    time_field: Literal["ended_at"]
    account_scope: Literal["current_target_user"]
    source: Literal["accounting"]
    observed_at: str
    freshness: Freshness
    limit: int


class ObservationResult(TypedDict):
    """Canonical wire shape."""

    query: NotRequired[ObservationResultquery]
    snapshots: list[JobSnapshot]
    coverage: Coverage
    completion: Completion
    changes: NotRequired[list[ObservedChange]]
    cursor: NotRequired[str]
    resync: NotRequired[bool]


class ClusterRecord(TypedDict):
    """Canonical wire shape."""

    name: str
    scheduler: Literal["shell", "slurm", "pbs", "lsf"]
    transport: NotRequired[TransportDefinition]
    defaults: NotRequired[Defaults]
    launcher: NotRequired[Literal["srun", "mpirun", "mpiexec"]]
    pbs_dialect: NotRequired[Literal["openpbs"]]
    id: str
    revision: int


class MolqHelloParams(TypedDict):
    """Canonical wire shape."""

    pass


class SchemaDiscoverParams(TypedDict):
    """Canonical wire shape."""

    pass


class ClustersListParams(TypedDict):
    """Canonical wire shape."""

    pass


class ClustersGetParams(TypedDict):
    """Canonical wire shape."""

    cluster: str


ClustersRegisterParams = ClusterDefinition


class ClustersUpdateParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    definition: ClusterDefinition
    expected_revision: int


class ClustersRemoveParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    expected_revision: int


class JobsValidateParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    spec: JobSpec


class JobsPreviewParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    spec: JobSpec


class JobsSubmitParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    spec: JobSpec
    request_key: NotRequired[str]


class JobsGetParams(TypedDict):
    """Canonical wire shape."""

    ref: JobRef
    consistency: NotRequired[Literal["live", "bounded", "cached"]]
    max_age: NotRequired[float]


class JobsGet_ManyParams(TypedDict):
    """Canonical wire shape."""

    refs: list[JobRef]
    consistency: NotRequired[Literal["live", "bounded", "cached"]]
    max_age: NotRequired[float]


class JobsObserveParams(TypedDict):
    """Canonical wire shape."""

    refs: NotRequired[list[JobRef]]
    consistency: NotRequired[Literal["live", "bounded", "cached"]]
    max_age: NotRequired[float]
    cluster: NotRequired[str]
    cursor: NotRequired[str]


class JobsListParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    states: NotRequired[
        list[
            Literal[
                "queued",
                "running",
                "succeeded",
                "failed",
                "cancelled",
                "timed_out",
                "unknown",
            ]
        ]
    ]
    name: NotRequired[str]


class JobsHistoryParams0(TypedDict):
    """Canonical wire shape."""

    cluster: str
    refs: list[JobRef]


class JobsHistoryParams1(TypedDict):
    """Canonical wire shape."""

    cluster: str
    since: NotRequired[str]
    until: NotRequired[str]
    states: NotRequired[list[Literal["succeeded", "failed", "cancelled", "timed_out"]]]
    name: NotRequired[str]
    limit: NotRequired[int]


JobsHistoryParams = JobsHistoryParams0 | JobsHistoryParams1


class JobsCancelParams(TypedDict):
    """Canonical wire shape."""

    ref: JobRef
    request_key: NotRequired[str]


class JobsCancel_ManyParams(TypedDict):
    """Canonical wire shape."""

    refs: list[JobRef]


class JobsPurgeParams(TypedDict):
    """Canonical wire shape."""

    cluster: NotRequired[str]


class LogsReadParams(TypedDict):
    """Canonical wire shape."""

    ref: JobRef
    stream: NotRequired[Literal["stdout", "stderr"]]
    offset: NotRequired[int]
    limit: NotRequired[int]


class FilesTransferParams(TypedDict):
    """Canonical wire shape."""

    sources: list[FileLocation]
    destination: FileLocation
    options: NotRequired[list[str]]
    timeout_seconds: NotRequired[int]


class FilesStatParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    root: NotRequired[str]


class FilesListParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    root: NotRequired[str]
    with_stat: NotRequired[bool]
    limit: NotRequired[int]
    after: NotRequired[str]


class FilesReadParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    offset: NotRequired[int]
    limit: NotRequired[int]
    root: NotRequired[str]


class FilesWriteParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    data: str
    offset: NotRequired[int]
    root: NotRequired[str]


class FilesMkdirParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    parents: NotRequired[bool]
    exist_ok: NotRequired[bool]
    mode: NotRequired[int]
    root: NotRequired[str]


class FilesRemoveParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    directory: NotRequired[bool]
    root: NotRequired[str]
    recursive: NotRequired[bool]


class FilesRenameParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    target: str
    root: NotRequired[str]


class FilesTouchParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    exist_ok: NotRequired[bool]
    root: NotRequired[str]


class ConfigReloadParams(TypedDict):
    """Canonical wire shape."""

    pass


class ConfigGetParams(TypedDict):
    """Canonical wire shape."""

    key: str


class ConfigSetParams(TypedDict):
    """Canonical wire shape."""

    key: str
    value: Any


class PresetsListParams(TypedDict):
    """Canonical wire shape."""

    pass


class PresetsGetParams(TypedDict):
    """Canonical wire shape."""

    key: str


class PresetsSetParams(TypedDict):
    """Canonical wire shape."""

    key: str
    value: Defaults


class EventsSubscribeParams(TypedDict):
    """Canonical wire shape."""

    cursor: NotRequired[str]


class FilesChmodParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    root: NotRequired[str]
    mode: int


class FilesSymlinkParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    root: NotRequired[str]
    target: str


class FilesResolveParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    path: str
    root: NotRequired[str]


class FilesRead_ManyParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    root: NotRequired[str]
    paths: list[str]
    max_bytes: NotRequired[int]


class ConnectionsAuthenticateParams(TypedDict):
    """Canonical wire shape."""

    cluster: str
    code: str
