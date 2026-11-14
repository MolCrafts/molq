"""Generated async OOP projection; edit jobs.py and regenerate."""

from __future__ import annotations

import asyncio
import builtins
import math
import time
from typing import TYPE_CHECKING

from molq.errors import MolqError

if TYPE_CHECKING:
    from molq.client.async_objects import AsyncMolq


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
