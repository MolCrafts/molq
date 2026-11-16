"""Explicit invalid endpoints never select a temporary stdio connection."""

import pytest

from molq import AsyncMolq, Molq, MolqError
from molq.client.async_wire import AsyncWire
from molq.client.wire import Wire


@pytest.mark.parametrize("client", [Wire, AsyncWire, Molq, AsyncMolq])
@pytest.mark.parametrize(
    "options",
    [
        {"endpoint": ""},
        {"endpoint": False},
        {"endpoint": 42},
        {"endpoint": "/rpc"},
        {"endpoint": "file:///tmp/serve"},
        {"endpoint": "https://"},
        {"endpoint": "http://localhost:wrong"},
        {"endpoint": "http://localhost:65536"},
        {"endpoint": "http://[::1"},
        {"endpoint": "http://bad host"},
        {"endpoint": "http://localhost\n"},
        {"token": 42},
        {"token": "header\nvalue"},
        {"token": "header\x00value"},
        {"registry_id": ""},
        {"registry_id": False},
    ],
)
def test_invalid_explicit_connection_is_rejected_locally(client, options):
    with pytest.raises(MolqError) as caught:
        client(**options)
    assert caught.value.kind == "INVALID_INPUT"
    assert caught.value.outcome == "not_applied"


@pytest.mark.parametrize("wire_type", [Wire, AsyncWire])
@pytest.mark.parametrize(
    "endpoint", [None, "http://127.0.0.1:8000", "https://[::1]/molq"]
)
def test_valid_connections_remain_lazy(wire_type, endpoint):
    wire = wire_type(endpoint=endpoint)
    assert wire.endpoint == endpoint
    assert wire.process is None
