import os
import sys
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
PROTO = os.path.join(ROOT, "proto")
for p in (ROOT, PROTO):
    if p not in sys.path:
        sys.path.insert(0, p)

import main
from proto import broker_pb2


class DummyRpcError(grpc.RpcError):
    def __init__(self, code=grpc.StatusCode.UNKNOWN, details="err"):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


def _make_client(stub):
    with patch("main.grpc.insecure_channel") as mock_channel, \
         patch("main.broker_pb2_grpc.BrokerServiceStub", return_value=stub):
        channel = MagicMock()
        mock_channel.return_value = channel
        client = main.BobberClient("host", 123)
        return client, channel


def test_healthcheck_ok():
    stub = MagicMock()
    stub.HealthCheck.return_value = SimpleNamespace(status="OK")

    client, _ = _make_client(stub)
    assert client.healthcheck() is True
    args, _ = stub.HealthCheck.call_args
    assert isinstance(args[0], broker_pb2.HealthCheckRequest)


def test_healthcheck_error():
    stub = MagicMock()
    stub.HealthCheck.side_effect = DummyRpcError()

    client, _ = _make_client(stub)
    assert client.healthcheck() is False


def test_produce_success():
    stub = MagicMock()

    client, _ = _make_client(stub)
    assert client.produce("t", "k", "v") is True
    args, _ = stub.Produce.call_args
    assert isinstance(args[0], broker_pb2.ProduceRequest)
    assert args[0].topic == "t"
    assert args[0].key == "k"
    assert args[0].value == "v"


def test_produce_error():
    stub = MagicMock()
    stub.Produce.side_effect = DummyRpcError(grpc.StatusCode.UNAVAILABLE, "down")

    client, _ = _make_client(stub)
    assert client.produce("t", "k", "v") is False


def test_fetch_success():
    stub = MagicMock()
    response = broker_pb2.FetchResponse(messages=[
        broker_pb2.Message(topic="t", key="k1", value="v1", offset=1, partition=0),
        broker_pb2.Message(topic="t", key="k2", value="v2", offset=2, partition=0),
    ])
    stub.Fetch.return_value = response

    client, _ = _make_client(stub)
    out = client.fetch("t", partition=0, offset=1, limit=2)
    assert out == [
        {"topic": "t", "key": "k1", "value": "v1", "offset": 1, "partition": 0},
        {"topic": "t", "key": "k2", "value": "v2", "offset": 2, "partition": 0},
    ]
    args, _ = stub.Fetch.call_args
    assert isinstance(args[0], broker_pb2.FetchRequest)
    assert args[0].topic == "t"
    assert args[0].partition == 0
    assert args[0].offset == 1
    assert args[0].limit == 2


def test_fetch_error():
    stub = MagicMock()
    stub.Fetch.side_effect = DummyRpcError(grpc.StatusCode.UNAVAILABLE)

    client, _ = _make_client(stub)
    assert client.fetch("t") == []


def test_subscribe_calls_callback():
    stub = MagicMock()
    messages = [
        broker_pb2.Message(topic="t", key="k1", value="v1", offset=1, partition=0),
        broker_pb2.Message(topic="t", key="k2", value="v2", offset=2, partition=0),
    ]

    def stream():
        for m in messages:
            yield m

    stub.Subscribe.return_value = stream()

    client, _ = _make_client(stub)
    received = []
    lock = threading.Lock()

    def cb(msg):
        with lock:
            received.append(msg)

    thread = client.subscribe("t", cb)
    thread.join(timeout=2)

    assert received == [
        {"topic": "t", "key": "k1", "value": "v1", "offset": 1, "partition": 0},
        {"topic": "t", "key": "k2", "value": "v2", "offset": 2, "partition": 0},
    ]


def test_subscribe_cancelled_error():
    stub = MagicMock()
    stub.Subscribe.side_effect = DummyRpcError(grpc.StatusCode.CANCELLED)

    client, _ = _make_client(stub)
    called = []

    def cb(_):
        called.append(True)

    thread = client.subscribe("t", cb)
    thread.join(timeout=2)
    assert called == []


def test_close_closes_channel():
    stub = MagicMock()
    client, channel = _make_client(stub)

    client.close()
    channel.close.assert_called_once()
