"""Shared bounded JSON-RPC codec; no scheduling or service logic."""

import json
import math
from typing import Any
from urllib.parse import urlsplit

from molq.errors import MolqError

MAX_FRAME = 1048576


def validate_connection(endpoint: Any, token: Any, registry_id: Any) -> None:
    """Reject malformed explicit connection settings without opening a session."""
    if endpoint is not None:
        try:
            if (
                not isinstance(endpoint, str)
                or not endpoint
                or any(
                    character.isspace() or ord(character) < 32 for character in endpoint
                )
            ):
                raise ValueError("Invalid endpoint")
            parsed = urlsplit(endpoint)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise ValueError("Invalid endpoint")
            # Accessing port validates its integer form and legal range.
            parsed.port
        except ValueError as error:
            raise MolqError(
                "INVALID_INPUT", "Endpoint must be an absolute HTTP(S) URL"
            ) from error
    if token is not None and (
        not isinstance(token, str)
        or any(ord(character) < 32 or ord(character) == 127 for character in token)
    ):
        raise MolqError("INVALID_INPUT", "Bearer token must be a string")
    if registry_id is not None and (
        not isinstance(registry_id, str) or not registry_id
    ):
        raise MolqError("INVALID_INPUT", "Registry identity must be a nonempty string")


def positive_timeout(value: Any) -> float:
    """Validate a finite positive timeout before establishing a connection."""
    try:
        valid = (
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
            and value > 0
        )
    except OverflowError:
        valid = False
    if not valid:
        raise MolqError("INVALID_INPUT", "RPC timeout must be finite and positive")
    return value


def encode_request(method: str, params: Any, sequence: int) -> bytes:
    """Encode a named-parameter request or reject it without dispatch."""
    if not isinstance(method, str) or not method or not isinstance(params, dict):
        raise MolqError("INVALID_INPUT", "RPC requires a method and named parameters")
    try:
        data = json.dumps(
            {"jsonrpc": "2.0", "id": sequence, "method": method, "params": params},
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise MolqError(
            "INVALID_INPUT", "RPC parameters must be JSON values"
        ) from error
    if len(data) > MAX_FRAME:
        raise MolqError("INVALID_INPUT", "RPC frame exceeds 1 MiB")
    return data


def decode_frame(frame: bytes) -> dict[str, Any]:
    """Decode strict JSON objects; invalid peer data raises a connection error."""
    if not frame or len(frame) > MAX_FRAME:
        raise OSError("Invalid RPC frame length")

    def reject_constant(value: str):
        raise ValueError("Nonfinite JSON number")

    def finite_float(value: str):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("Nonfinite JSON number")
        return number

    result = json.loads(
        frame.decode("utf-8"), parse_constant=reject_constant, parse_float=finite_float
    )
    if not isinstance(result, dict) or result.get("jsonrpc") != "2.0":
        raise OSError("Invalid RPC envelope")
    return result


def is_event(value: dict[str, Any]) -> bool:
    """Recognize the public event notification without accepting bad envelopes."""
    if value.get("method") != "events" or "id" in value:
        return False
    if (
        not isinstance(value.get("params"), dict)
        or "result" in value
        or "error" in value
    ):
        raise OSError("Invalid event notification")
    return True


def response_result(response: dict[str, Any], sequence: int) -> Any:
    """Validate response identity and structured errors before projection."""
    if (
        "id" not in response
        or (
            (type(response["id"]) is not int or response["id"] != sequence)
            and not (response["id"] is None and "error" in response)
        )
        or ("result" in response) == ("error" in response)
        or "method" in response
    ):
        raise OSError("Invalid RPC response envelope")
    if "error" not in response:
        return response["result"]
    error = response["error"]
    if (
        not isinstance(error, dict)
        or type(error.get("code")) is not int
        or not isinstance(error.get("message"), str)
    ):
        raise OSError("Invalid RPC error envelope")
    data = error.get("data")
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("kind"), str)
        or not isinstance(data.get("message"), str)
        or data.get("outcome") not in ("not_applied", "applied", "unknown")
        or not isinstance(data.get("context"), dict)
    ):
        raise OSError("Invalid RPC ErrorData")
    raise MolqError.from_wire(data)


def validate_hello(hello: Any) -> None:
    """Reject malformed handshake data before using the peer identity."""
    if not isinstance(hello, dict) or not isinstance(hello.get("registry_id"), str):
        raise MolqError("RUNTIME_UNAVAILABLE", "Invalid RPC handshake")
