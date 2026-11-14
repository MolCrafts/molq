"""Generated async OOP projection; edit objects.py and regenerate."""

from __future__ import annotations

import asyncio
import builtins
import inspect
import math
import sys
import time
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import Any

from molq.client.async_wire import AsyncWire
from molq.domain import JobSpec, Value
from molq.errors import MolqError
from molq.protocol import contract


def wire_value(value: Any) -> Any:
    """Convert a local value constructor without interpreting its schema.

    Args:
        value (Any): A request value or an already constructed wire value.

    Returns:
        Any: The value's wire dictionary, or the unchanged non-Value argument.
    """
    return value.to_wire() if isinstance(value, Value) else value


def _job_spec(spec: JobSpec) -> dict[str, Any]:
    if not isinstance(spec, JobSpec):
        raise MolqError("INVALID_INPUT", "Expected a JobSpec request")
    return spec.to_wire()


async def asubmit(
    cluster_name: str,
    cluster_type: str | None = None,
    *,
    endpoint: str | None = None,
    registry: Path | str | None = None,
    config: Path | str | None = None,
    token: str | None = None,
    timeout: float = 130,
    registry_id: str | None = None,
    **definition: Any,
) -> AsyncCluster:
    """Bind a destination for ``@destination`` and ``yield JobSpec`` submission.

    Args:
        cluster_name (str): Registered destination name or ID.
        cluster_type (str | None): Scheduler for an explicit destination binding:
            local (Shell), shell, slurm, pbs or lsf. None selects an existing one.
        endpoint (str | None): HTTP(S) service endpoint; None uses temporary stdio.
        registry (Path | str | None): Registry for temporary stdio.
        config (Path | str | None): Settings file for temporary stdio.
        token (str | None): HTTP bearer credential.
        timeout (float): Per-RPC response deadline in seconds.
        registry_id (str | None): Expected registry identity.
        **definition (Any): Destination fields such as transport, defaults and
            launcher. New local destinations default to local transport. Jobs
            run under ~/.molcrafts/molq/jobs on the target; an SSH target's job
            directory moves to the serve host after the job ends. Existing
            bindings must match the supplied fields.

    Returns:
        AsyncCluster: Callable destination owning its connection. Call close() when
            finished. Selecting by name alone is lazy; explicit binding uses RPC.

    Raises:
        MolqError: The definition conflicts, is invalid or an RPC fails. An
            uncertain registration or submission is never automatically resent.

    Example:
        local = await asubmit("dev", "local")

        @local
        def hello(name):
            job = yield JobSpec.argv(["echo", name])
            return job
    """
    if cluster_type is None and definition:
        raise MolqError("INVALID_INPUT", "Destination fields require cluster_type")
    if {"name", "scheduler"} & definition.keys():
        raise MolqError("INVALID_INPUT", "Use cluster_name and cluster_type")
    fields = contract()["$defs"]["ClusterDefinition"]["properties"]
    if definition.keys() - fields.keys():
        raise MolqError("INVALID_INPUT", "Unknown destination fields")
    if cluster_type is not None and cluster_type not in (
        "local",
        *fields["scheduler"]["enum"],
    ):
        raise MolqError("INVALID_INPUT", "Unknown destination scheduler")
    if "transport" in definition and (not isinstance(definition["transport"], dict)):
        raise MolqError("INVALID_INPUT", "Destination transport must be an object")
    mq = AsyncMolq(
        endpoint=endpoint,
        registry=registry,
        config=config,
        token=token,
        timeout=timeout,
        registry_id=registry_id,
    )
    cluster = mq.cluster(cluster_name)
    if cluster_type is None:
        return cluster
    scheduler = "shell" if cluster_type == "local" else cluster_type
    expected = {"scheduler": scheduler, **definition}
    if cluster_type == "local" and "transport" not in expected:
        expected["transport"] = {"kind": "local"}
    try:
        try:
            existing = await mq.rpc("clusters.get", cluster=cluster_name)
        except MolqError as error:
            if error.kind != "CLUSTER_NOT_FOUND":
                raise
            binding: dict[str, Any] = {"name": cluster_name, **expected}
            registered = await mq.rpc("clusters.register", **binding)
            return mq.cluster(registered["id"])
        for field, value in expected.items():
            if existing.get(field) != wire_value(value):
                raise MolqError(
                    "INVALID_INPUT",
                    "Existing destination binding differs",
                    cluster=cluster_name,
                    field=field,
                )
        return cluster
    except BaseException:
        try:
            await mq.close()
        except BaseException:
            pass
        raise


class AsyncMolq:
    """Connect to one Runtime; absent endpoint uses its temporary stdio form.

    Args:
        endpoint (str | None): HTTP(S) endpoint; None selects temporary stdio serve.
        registry (Path | str | None): Registry path for a temporary stdio session.
        config (Path | str | None): Configuration file for temporary stdio serve.
        token (str | None): Bearer credential for the HTTP endpoint.
        timeout (float): Per-RPC response deadline in seconds.
        registry_id (str | None): Expected registry identity, checked on connection.

    Connection is lazy unless the client is entered as a context manager. RPC
    methods raise MolqError for structured runtime and connection failures. A
    dispatched mutation with an unknown outcome must be reconciled before resend.
    """

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        registry: Path | str | None = None,
        config: Path | str | None = None,
        token: str | None = None,
        timeout: float = 130,
        registry_id: str | None = None,
    ):
        self._wire = AsyncWire(
            endpoint=endpoint,
            registry=registry,
            config=config,
            token=token,
            timeout=timeout,
            registry_id=registry_id,
        )
        self.clusters = AsyncClusterRegistry(self)

    async def __aenter__(self) -> AsyncMolq:
        try:
            await self._wire.connect()
        except BaseException:
            try:
                await self.close()
            except BaseException:
                pass
            raise
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close this client session without cancelling accepted allocations.

        Returns:
            None: This session's connection is released.
        """
        await self._wire.close()

    async def rpc(self, method: str, **params: Any) -> Any:
        """Call any named operation in the public RPC contract.

        Args:
            method (str): Public method name, such as "jobs.get_many".
            **params (Any): Named parameters; None values are omitted and local
                Value instances are converted to wire dictionaries.

        Returns:
            Any: The runtime's method-specific result.

        Raises:
            MolqError: The runtime rejects the request or the connection fails;
                dispatched mutation failures may have outcome "unknown".
        """
        return await self._wire.call(
            method, {k: wire_value(v) for k, v in params.items() if v is not None}
        )

    def cluster(self, name: str) -> AsyncCluster:
        """Construct a handle without querying the destination.

        Args:
            name (str): Registered cluster name or ID, resolved by the runtime.

        Returns:
            AsyncCluster: A handle using this client's connection.
        """
        return AsyncCluster(self, name)

    def job(self, ref: dict) -> AsyncJob:
        """Construct a handle to a registry-qualified native allocation.

        Args:
            ref (dict): JobRef with registry_id, cluster_id, native_id and incarnation.

        Returns:
            AsyncJob: A handle holding a shallow copy of the reference.
        """
        return AsyncJob(self, dict(ref))

    def jobs(self, refs: builtins.list[dict]) -> AsyncJobCollection:
        """Construct a handle for batch operations on saved references.

        Args:
            refs (list[dict]): Registry-qualified native allocation references.

        Returns:
            AsyncJobCollection: A handle holding a copy of each reference.
        """
        return AsyncJobCollection(self, list(refs))


class AsyncClusterRegistry:
    """RPC-only access to durable destination definitions.

    Args:
        molq (AsyncMolq): Client used for registry operations.
    """

    def __init__(self, molq: AsyncMolq):
        self.molq = molq

    async def register(self, **definition: Any) -> AsyncCluster:
        """Register an explicit destination definition.

        Args:
            **definition (Any): Canonical cluster fields, including name, scheduler,
                and transport, plus required transport/site settings.

        Returns:
            AsyncCluster: A handle using the new cluster's stable ID.

        Raises:
            MolqError: The definition is invalid, conflicts with the registry, or
                the RPC fails; a disconnected mutation may have applied.
        """
        result = await self.molq.rpc("clusters.register", **definition)
        return AsyncCluster(self.molq, result["id"])

    async def list(self) -> builtins.list[dict]:
        """List destination definitions without querying native job state.

        Returns:
            list[dict]: Runtime-owned definitions with stable IDs and revisions.

        Raises:
            MolqError: The registry request or RPC connection fails.
        """
        return await self.molq.rpc("clusters.list")


class AsyncCluster:
    """A destination handle whose operations use public RPC.

    Args:
        molq (AsyncMolq): Client used for destination operations.
        name (str): Registered destination name or ID.
    """

    def __init__(self, molq: AsyncMolq, name: str):
        self.molq, self.name = (molq, name)

    def __call__(self, function: Callable) -> Callable:
        """Submit each yielded JobSpec and preserve the function's return.

        Args:
            function (Callable): Synchronous function or generator. Each yielded
                JobSpec receives its submitted AsyncJob. Ordinary functions return
                their result without submitting.

        Returns:
            Callable: A function with the original signature and metadata. A
                generator returns its own result. Decoration performs no RPC.
                The client owns connection lifetime.

        Raises:
            TypeError: The function is async or is not callable.
            MolqError: A request is not a JobSpec, or submission fails; the error
                propagates without resend.

        Example:
            @cluster
            def hello(name):
                job = yield JobSpec.argv(["echo", name])
                return job
        """
        if (
            not callable(function)
            or inspect.iscoroutinefunction(function)
            or inspect.isasyncgenfunction(function)
        ):
            raise TypeError("Decorate a synchronous function or generator")

        @wraps(function)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            value: Any = function(*args, **kwargs)
            if not inspect.isgenerator(value):
                return value
            try:
                request: Any = next(value)
                while True:
                    job = await self.submit(request)
                    request = value.send(job)
            except StopIteration as completed:
                return completed.value
            finally:
                close_generator = value.close
                error = sys.exception()
                try:
                    close_generator()
                except BaseException:
                    if error is None:
                        raise

        return invoke

    async def close(self) -> None:
        """Release this destination's client connection, leaving jobs running.

        Returns:
            None: The connection is released, including other handles sharing it.
        """
        await self.molq.close()

    async def authenticate(self, code: str) -> dict:
        """Answer one SSH authentication prompt without storing the response."""
        return await self.molq.rpc(
            "connections.authenticate", cluster=self.name, code=code
        )

    async def definition(self) -> dict:
        """Read the runtime-owned definition and revision.

        Returns:
            dict: The canonical cluster definition and current revision.

        Raises:
            MolqError: The cluster cannot be resolved or the RPC fails.
        """
        return await self.molq.rpc("clusters.get", cluster=self.name)

    async def update(self, definition: dict, *, expected_revision: int) -> dict:
        """Replace destination configuration with a revision check.

        Args:
            definition (dict): Complete canonical destination definition.
            expected_revision (int): Revision returned by the most recent read.

        Returns:
            dict: The runtime's updated definition and revision.

        Raises:
            MolqError: Validation, cluster resolution or the revision check fails,
                or the RPC fails with a potentially unknown mutation outcome.
        """
        return await self.molq.rpc(
            "clusters.update",
            cluster=self.name,
            definition=definition,
            expected_revision=expected_revision,
        )

    async def remove(self, *, expected_revision: int) -> dict:
        """Remove a destination definition without cancelling native jobs.

        Args:
            expected_revision (int): Revision returned by the most recent read.

        Returns:
            dict: The runtime's removal result.

        Raises:
            MolqError: AsyncCluster resolution or the revision check fails, or the RPC
                fails with a potentially unknown mutation outcome.
        """
        return await self.molq.rpc(
            "clusters.remove", cluster=self.name, expected_revision=expected_revision
        )

    async def submit(
        self, spec: JobSpec, *, request_key: str | None = None
    ) -> AsyncJob | AsyncJobCollection:
        """Submit one allocation or a native array.

        Args:
            spec (JobSpec): Immutable allocation specification.
            request_key (str | None): Optional durable key. Repeating the same
                request reconciles the prior submission without resending it.

        Returns:
            AsyncJob | AsyncJobCollection: One allocation or the accepted array elements.

        Raises:
            MolqError: The request is not a JobSpec, the runtime or
                native scheduler rejects the request, or the RPC fails. An unknown
                mutation outcome may mean the allocation was already accepted.

        Example:
            job = cluster.submit(JobSpec.argv(["python", "work.py"]))
        """
        result = await self.molq.rpc(
            "jobs.submit",
            cluster=self.name,
            spec=_job_spec(spec),
            request_key=request_key,
        )
        if "refs" in result:
            return AsyncJobCollection(self.molq, result["refs"])
        return AsyncJob(self.molq, result["ref"])

    async def transfer(
        self,
        source_path: str,
        destination: AsyncCluster,
        destination_path: str,
        *options: str,
        timeout: int = 90,
    ) -> dict:
        """Copy a path from this destination to another with one rsync run.

        Args:
            source_path (str): Path on this destination. A relative path is
                relative to the login home; a trailing slash is passed to rsync.
            destination (AsyncCluster): Destination that receives the copy.
            destination_path (str): Path on ``destination``.
            *options (str): rsync options passed unchanged, each one word with
                any value attached (``"--exclude=build/"``). Nothing is implied.

        Returns:
            dict: Runtime transfer result. ``outcome`` is ``applied`` when the
                copy finishes; ``output`` is rsync's standard output.

        Raises:
            MolqError: ``destination`` is not a AsyncCluster, a path or option is
                rejected, or the copy fails. An unknown mutation outcome may
                mean the destination was already changed.

        Example:
            local.transfer("/src/", hpc, "/scratch/project/", "-a", "--exclude=build/")
        """
        if not isinstance(destination, AsyncCluster):
            raise MolqError("INVALID_INPUT", "Expected a AsyncCluster destination")
        return await self.molq.rpc(
            "files.transfer",
            sources=[{"cluster": self.name, "path": source_path}],
            destination={"cluster": destination.name, "path": destination_path},
            options=list(options),
            timeout_seconds=timeout,
        )

    async def validate(self, spec: JobSpec) -> dict:
        """Validate schema and exact native representation through the runtime.

        Args:
            spec (JobSpec): Allocation specification to validate.

        Returns:
            dict: Runtime validation result; it does not prove resource availability
                or scheduling permission on the target.

        Raises:
            MolqError: The specification is invalid, cannot be represented, or the
                RPC fails.
        """
        return await self.molq.rpc(
            "jobs.validate", cluster=self.name, spec=_job_spec(spec)
        )

    async def preview(self, spec: JobSpec) -> dict:
        """Preview an allocation using the scheduler's submission renderer.

        Args:
            spec (JobSpec): Allocation specification to render.

        Returns:
            dict: Runtime-rendered submission content without a native submission.

        Raises:
            MolqError: The specification is invalid, cannot be represented, or the
                RPC fails.
        """
        return await self.molq.rpc(
            "jobs.preview", cluster=self.name, spec=_job_spec(spec)
        )

    async def list(
        self, *, states: builtins.list[str] | None = None, name: str | None = None
    ) -> dict:
        """Query the target account's native queue.

        Args:
            states (list[str] | None): Canonical states to select; None selects all.
            name (str | None): Exact native job name to select.

        Returns:
            dict: Canonical snapshots with query coverage and freshness metadata.

        Raises:
            MolqError: The filters are invalid, the native query fails, or the RPC
                fails.
        """
        return await self.molq.rpc(
            "jobs.list", cluster=self.name, states=states, name=name
        )

    async def history(
        self,
        *,
        refs: builtins.list[dict] | None = None,
        since: str | None = None,
        until: str | None = None,
        states: builtins.list[str] | None = None,
        name: str | None = None,
        limit: int | None = None,
    ) -> dict:
        """Query refs or account history by RFC3339 ended-at window.

        Args:
            refs (list[dict] | None): Explicit refs; excludes every range filter.
            since (str | None): Inclusive RFC3339 ended-at lower bound, defaulting
                to until minus 24 hours.
            until (str | None): Exclusive RFC3339 upper bound, defaulting to the
                runtime's current UTC time.
            states (list[str] | None): Canonical terminal states to select; None
                selects all terminal states.
            name (str | None): Exact native job name to select.
            limit (int | None): Result bound; defaults to 100 and is at most 1024.

        Returns:
            dict: Native history snapshots with coverage and freshness metadata.

        Raises:
            MolqError: Filters are invalid, history is unavailable or unsupported,
                or the RPC fails.
        """
        return await self.molq.rpc(
            "jobs.history",
            cluster=self.name,
            refs=refs,
            since=since,
            until=until,
            states=states,
            name=name,
            limit=limit,
        )

    async def observe(self, *, cursor: str | None = None) -> dict:
        """Request one cluster sample and its runtime-owned diff.

        Args:
            cursor (str | None): Cursor from a previous observation of this scope;
                None establishes a baseline.

        Returns:
            dict: Snapshots, changes, completion and the next cursor. A missing or
                expired baseline is reported as a resynchronization.

        Raises:
            MolqError: The cluster sample or RPC fails.
        """
        return await self.molq.rpc("jobs.observe", cluster=self.name, cursor=cursor)


class AsyncJob:
    """One allocation whose lifecycle interpretation is returned by the runtime.

    Args:
        molq (AsyncMolq): Client used for allocation operations.
        ref (dict): Registry-qualified reference, copied into this handle.
    """

    def __init__(self, molq: AsyncMolq, ref: dict):
        self.molq, self.ref = (molq, dict(ref))

    async def status(self, *, consistency: str = "live", max_age: float = 5) -> dict:
        """Read canonical allocation observations through the runtime.

        Args:
            consistency (str): "live" queries native state, "bounded" may use a
                sufficiently fresh cache entry, and "cached" only reads the cache.
            max_age (float): Maximum cache age in seconds for bounded consistency.

        Returns:
            dict: A one-snapshot collection with runtime completion, coverage and
                freshness metadata.

        Raises:
            MolqError: References or consistency arguments are invalid, required
                cached data is missing, or the RPC fails.
        """
        return await self.molq.rpc(
            "jobs.get", ref=self.ref, consistency=consistency, max_age=max_age
        )

    async def requeue(self) -> dict:
        """Ask Slurm to restart this incomplete job or array element.

        Returns:
            dict: Native acceptance and the reference used for the request.
        """
        return await self.molq.rpc("jobs.requeue", ref=self.ref)

    async def cancel(self, *, request_key: str | None = None) -> dict:
        """Request cancellation and return the native request outcome.

        Args:
            request_key (str | None): Optional bounded deduplication key within
                this serve instance.

        Returns:
            dict: Cancellation outcome; acceptance does not confirm terminal state.

        Raises:
            MolqError: The reference is invalid, native cancellation fails, or the
                RPC fails with a potentially unknown mutation outcome.
        """
        return await self.molq.rpc("jobs.cancel", ref=self.ref, request_key=request_key)

    async def wait(self, *, timeout: float = 3600, interval: float = 1) -> dict:
        """Sample until the runtime confirms this allocation is terminal.

        Args:
            timeout (float): Positive foreground wait deadline in seconds.
            interval (float): Positive pause between samples in seconds.

        Returns:
            dict: The collection observation confirming terminal completion.

        Raises:
            MolqError: Arguments are invalid, an observation fails, or the wait
                deadline expires with kind WAIT_TIMEOUT. Expiration preserves
                the native allocation; each in-flight RPC has its own deadline.
        """
        return await self.molq.jobs([self.ref]).wait(timeout=timeout, interval=interval)

    async def logs(
        self, *, stream: str = "stdout", offset: int = 0, limit: int = 262144
    ) -> dict:
        """Read a finite target log chunk through the runtime.

        Args:
            stream (str): "stdout" or "stderr".
            offset (int): Nonnegative byte offset in the selected log.
            limit (int): Maximum bytes to read, up to 262144.

        Returns:
            dict: Log text, next_offset and bytes_read for this chunk.

        Raises:
            MolqError: The reference or range is invalid, the target file cannot
                be read, or the RPC fails.
        """
        return await self.molq.rpc(
            "logs.read", ref=self.ref, stream=stream, offset=offset, limit=limit
        )


class AsyncJobCollection:
    """Batch operations sharing the runtime's collection completion semantics.

    Args:
        molq (AsyncMolq): Client used for batch operations.
        refs (list[dict]): Allocation references; each is copied into this handle.
    """

    def __init__(self, molq: AsyncMolq, refs: builtins.list[dict]):
        self.molq, self.refs = (molq, [dict(ref) for ref in refs])

    async def status(self, *, consistency: str = "live", max_age: float = 5) -> dict:
        """Read canonical allocation observations through the runtime.

        Args:
            consistency (str): "live" queries native state, "bounded" may use a
                sufficiently fresh cache entry, and "cached" only reads the cache.
            max_age (float): Maximum cache age in seconds for bounded consistency.

        Returns:
            dict: Canonical snapshots with runtime completion, coverage and
                freshness metadata.

        Raises:
            MolqError: References or consistency arguments are invalid, or the
                RPC fails. Per-reference failures, including CACHE_MISS, are
                returned in query coverage.
        """
        return await self.molq.rpc(
            "jobs.get_many", refs=self.refs, consistency=consistency, max_age=max_age
        )

    async def cancel(self) -> dict:
        """Request cancellation of this collection in cluster batches.

        Returns:
            dict: Explicit per-item cancellation outcomes and failures. Accepted
                cancellation requests do not confirm terminal completion.

        Raises:
            MolqError: The batch request is invalid or the RPC fails with a
                potentially unknown mutation outcome.
        """
        return await self.molq.rpc("jobs.cancel_many", refs=self.refs)

    async def observe(self, *, cursor: str | None = None) -> dict:
        """Request one runtime-owned diff and collection completion result.

        Args:
            cursor (str | None): Cursor from a previous observation of these refs;
                None establishes a baseline.

        Returns:
            dict: Snapshots, changes, runtime completion and the next cursor.
                Baseline loss is reported as a resynchronization.

        Raises:
            MolqError: The batch request is invalid or the RPC fails.
        """
        return await self.molq.rpc("jobs.observe", refs=self.refs, cursor=cursor)

    async def wait(self, *, timeout: float = 3600, interval: float = 1) -> dict:
        """Sample until every reference has confirmed terminal state.

        Args:
            timeout (float): Positive foreground wait deadline in seconds.
            interval (float): Positive pause between samples in seconds.

        Returns:
            dict: The observation with all_confirmed_terminal set by the runtime.

        Raises:
            MolqError: Arguments are invalid, an observation fails, or the wait
                deadline expires with kind WAIT_TIMEOUT. Expiration preserves
                native allocations; each in-flight RPC has its own deadline.

        Example:
            result = collection.wait(timeout=60, interval=2)
        """
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or (not math.isfinite(value))
            or (value <= 0)
            for value in (timeout, interval)
        ):
            raise MolqError(
                "INVALID_INPUT", "wait timeout/interval must be finite and positive"
            )
        deadline = time.monotonic() + timeout
        cursor = None
        while True:
            result = await self.observe(cursor=cursor)
            cursor = result["cursor"]
            if result["completion"]["all_confirmed_terminal"]:
                return result
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise MolqError("WAIT_TIMEOUT", "Foreground wait deadline reached")
            await asyncio.sleep(min(interval, remaining))
