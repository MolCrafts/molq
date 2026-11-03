"""RPC codec and native connection management; no molq business behavior."""

from __future__ import annotations

import subprocess
import threading
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any

from molq.client.codec import (
    MAX_FRAME,
    decode_frame,
    encode_request,
    is_event,
    positive_timeout,
    response_result,
    validate_connection,
    validate_hello,
)
from molq.errors import MolqError


class Wire:
    """One stdio session or an explicit authenticated HTTP endpoint."""

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
        self.registry = registry
        self.token = token
        self.executable = None
        self.timeout = positive_timeout(timeout)
        self.registry_id = registry_id
        self.process: subprocess.Popen | None = None
        self.events = deque(maxlen=128)
        self.sequence = 0
        self.lock = threading.Lock()
        self.hello: dict | None = None

    def _start(self) -> None:
        if self.process is not None:
            return
        from molq.client.binary import executable

        argv = [executable(self.config), "serve", "--stdio"]
        if self.config is not None:
            argv.extend(["--config", str(self.config)])
        if self.registry is not None:
            argv.extend(["--registry", str(self.registry)])
        # stdout is protocol-only; stderr is diagnostic and cannot fill a pipe.
        self.process = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None
        )

    def connect(self) -> dict:
        """Verify optional registry identity before any business call."""
        if self.hello is None:
            hello = self._request("molq.hello", {})
            validate_hello(hello)
            if self.registry_id and hello["registry_id"] != self.registry_id:
                raise MolqError(
                    "REGISTRY_MISMATCH", "Endpoint serves a different registry"
                )
            self.hello = hello
        return self.hello

    def call(self, method: str, params: dict | None = None) -> Any:
        """Send one operation without renderer, defaults merge or state logic."""
        with self.lock:
            params = {} if params is None else params
            encode_request(method, params, self.sequence + 1)
            if method != "molq.hello":
                self.connect()
            return self._request(method, params)

    def _request(self, method: str, params: dict) -> Any:
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
            if self.endpoint:
                request = urllib.request.Request(
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
                )
                try:
                    with urllib.request.urlopen(
                        request, timeout=self.timeout
                    ) as response:
                        frame = response.read(MAX_FRAME + 1)
                except urllib.error.HTTPError as exc:
                    frame = exc.read(MAX_FRAME + 1)
            else:
                self._start()
                assert self.process and self.process.stdin and self.process.stdout
                if self.process.poll() is not None:
                    raise MolqError(
                        "RUNTIME_UNAVAILABLE",
                        "RPC session is closed; close before reconnecting",
                    )
                self.process.stdin.write(data + b"\n")
                self.process.stdin.flush()
                # One finite read bridge supports Windows ordinary pipes too.
                stdout = self.process.stdout
                value: list[bytes | BaseException] = []

                def read():
                    try:
                        while True:
                            received = stdout.readline(MAX_FRAME + 2).removesuffix(
                                b"\n"
                            )
                            if not received or len(received) > MAX_FRAME:
                                raise OSError("Invalid frame length")
                            decoded = decode_frame(received)
                            if is_event(decoded):
                                self.events.append(decoded["params"])
                                continue
                            value.append(received)
                            break
                    except BaseException as exc:
                        value.append(exc)

                reader = threading.Thread(target=read, daemon=True)
                reader.start()
                reader.join(self.timeout)
                if reader.is_alive():
                    self.process.kill()
                    self.process.wait(timeout=5)
                    reader.join(5)
                    raise TimeoutError("RPC response deadline")
                if not value or isinstance(value[0], BaseException):
                    raise OSError("RPC pipe closed")
                frame = value[0]
            if not frame or len(frame) > MAX_FRAME:
                raise OSError("Empty/oversized RPC response")
            return response_result(decode_frame(frame), self.sequence)
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            RecursionError,
            TimeoutError,
        ) as exc:
            if self.process and self.process.poll() is None:
                try:
                    self.process.kill()
                    self.process.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    pass
            raise MolqError(
                "OUTCOME_UNKNOWN" if mutation else "RUNTIME_UNAVAILABLE",
                "RPC connection failed",
                outcome="unknown" if mutation else "not_applied",
            ) from exc

    def close(self) -> None:
        """Close only this session; accepted jobs and user SSH masters survive."""
        if self.process:
            if self.process.stdin:
                self.process.stdin.close()
            try:
                self.process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
            if self.process.stdout:
                self.process.stdout.close()
            self.process = None
        self.hello = None
