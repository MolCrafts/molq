"""Native async RPC connection; no Scheduler/registry imports."""

from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path
from typing import Any

from aiohttp import ClientError

from molq.client.codec import (
    decode_frame,
    encode_request,
    is_event,
    positive_timeout,
    response_result,
    validate_connection,
    validate_hello,
)
from molq.errors import MolqError


class AsyncWire:
    """Bounded asyncio stdio or aiohttp connection to the same public contract."""

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
        validate_connection(endpoint, token, registry_id)
        self.config = config
        self.endpoint = endpoint
        self.registry, self.token, self.timeout, self.registry_id = (
            registry,
            token,
            positive_timeout(timeout),
            registry_id,
        )
        self.process: asyncio.subprocess.Process | None = None
        self.events = deque(maxlen=128)
        self.sequence = 0
        self.hello: dict | None = None
        self.lock = asyncio.Lock()
        self.session = None

    async def connect(self) -> dict:
        """Verify registry identity before sending a business operation."""
        if self.hello is None:
            hello = await self._request("molq.hello", {})
            validate_hello(hello)
            if self.registry_id and hello["registry_id"] != self.registry_id:
                raise MolqError(
                    "REGISTRY_MISMATCH", "Endpoint serves a different registry"
                )
            self.hello = hello
        return self.hello

    async def call(self, method: str, params: dict | None = None) -> Any:
        """Send a wire request without duplicating business semantics."""
        async with self.lock:
            params = {} if params is None else params
            encode_request(method, params, self.sequence + 1)
            if method != "molq.hello":
                await self.connect()
            return await self._request(method, params)

    async def _request(self, method: str, params: dict) -> Any:
        self.sequence += 1
        data = encode_request(method, params, self.sequence)
        mutation = method in {
            "clusters.register",
            "clusters.update",
            "clusters.remove",
            "config.set",
            "config.reload",
            "presets.set",
            "jobs.submit",
            "jobs.cancel",
            "jobs.cancel_many",
            "jobs.purge",
            "files.transfer",
            "files.write",
            "files.mkdir",
            "files.remove",
            "files.rename",
            "files.touch",
        }
        try:
            async with asyncio.timeout(self.timeout):
                if self.endpoint:
                    from aiohttp import ClientSession, ClientTimeout

                    if self.session is None:
                        self.session = ClientSession(
                            timeout=ClientTimeout(total=self.timeout)
                        )
                    async with self.session.post(
                        self.endpoint.rstrip("/") + "/rpc",
                        data=data,
                        headers={
                            "Content-Type": "application/json",
                            **(
                                {"Authorization": "Bearer " + self.token}
                                if self.token
                                else {}
                            ),
                        },
                    ) as response:
                        chunks = bytearray()
                        async for chunk in response.content.iter_chunked(65536):
                            chunks.extend(chunk)
                            if len(chunks) > 1048576:
                                raise OSError("RPC response exceeds 1 MiB")
                        frame = bytes(chunks)
                else:
                    if self.process is None:
                        from molq.client.binary import executable

                        argv = [executable(self.config), "serve", "--stdio"]
                        if self.config is not None:
                            argv.extend(["--config", str(self.config)])
                        if self.registry is not None:
                            argv.extend(["--registry", str(self.registry)])
                        self.process = await asyncio.create_subprocess_exec(
                            *argv,
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,
                            limit=1048577,
                        )
                    if self.process.returncode is not None:
                        raise MolqError(
                            "RUNTIME_UNAVAILABLE",
                            "RPC session is closed; close before reconnecting",
                        )
                    assert self.process.stdin and self.process.stdout
                    self.process.stdin.write(data + b"\n")
                    await self.process.stdin.drain()
                    while True:
                        frame = await self.process.stdout.readline()
                        frame = frame.removesuffix(b"\n")
                        if len(frame) > 1048576 or not frame:
                            raise OSError("Invalid frame length")
                        decoded = decode_frame(frame)
                        if is_event(decoded):
                            self.events.append(decoded["params"])
                            continue
                        break
                if not frame or len(frame) > 1048576:
                    raise OSError("Invalid frame length")
                return response_result(decode_frame(frame), self.sequence)
        except asyncio.CancelledError:
            # An unread response cannot be reused by the next request. Keep the
            # dead session until explicit close: never restart an uncertain call.
            if self.process and self.process.returncode is None:
                self.process.kill()
                await asyncio.shield(self.process.wait())
            raise
        except (
            ClientError,
            OSError,
            ValueError,
            KeyError,
            TypeError,
            RecursionError,
            TimeoutError,
        ) as exc:
            if self.process and self.process.returncode is None:
                self.process.kill()
                await self.process.wait()
            raise MolqError(
                "OUTCOME_UNKNOWN" if mutation else "RUNTIME_UNAVAILABLE",
                "RPC connection failed",
                outcome="unknown" if mutation else "not_applied",
            ) from exc

    async def close(self) -> None:
        """Close this client session without touching accepted allocations."""
        if self.session is not None:
            await self.session.close()
            self.session = None
        if self.process:
            if self.process.stdin:
                self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), self.timeout)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
            self.process = None
        self.hello = None
