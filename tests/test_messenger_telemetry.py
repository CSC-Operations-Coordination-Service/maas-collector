"""Messenger telemetry: publish span, trace headers, chunk links, counts, returns"""

import dataclasses
import json
from unittest.mock import MagicMock, Mock

import pytest

from maas_model import MAASMessage, telemetry

from maas_collector.rawdata.messenger import Messenger


@dataclasses.dataclass
class FakeConfig:
    """the attributes Messenger.handle_message and node_name read"""

    routing_key: str
    model_name: str = "M"
    exchange_name: str = ""
    interface_name: str = ""


@pytest.fixture
def messenger():
    """a Messenger whose producer is a mock"""
    instance = Messenger("amqp://localhost:5672", exchange_name="ex")
    instance._producer = MagicMock()
    instance._producer.channel = MagicMock()
    return instance


def test_publish_has_trace_headers_and_span(telemetry_capture, messenger):
    with telemetry.span("collect.file"):
        messenger.send_to_queue(
            "rk",
            MAASMessage(document_class="M", document_ids=["a", "b"]),
            exchange_name="ex",
        )
    kwargs = messenger._producer.publish.call_args.kwargs
    assert "traceparent" in kwargs["headers"]
    s = telemetry_capture.span("publish ex")
    assert s.parent.span_id == telemetry_capture.span("collect.file").context.span_id
    assert s.attributes["maas.document.count"] == 2
    assert (
        telemetry_capture.point(
            "maas.messages.published", exchange="ex", routing_key="rk", outcome="ok"
        ).value
        == 1
    )


def test_disabled_publish_body_unchanged(messenger):
    payload = MAASMessage(document_class="M", document_ids=["a"])
    messenger.send_to_queue("rk", payload, exchange_name="ex")
    call = messenger._producer.publish.call_args
    assert call.kwargs["headers"] == {}
    expected = dataclasses.asdict(payload)
    assert call.args[0] == json.dumps(expected)
    assert "traceparent" not in call.args[0]


def test_chunk_links_to_each_file(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 3}
    for f in ("f1", "f2", "f3"):
        with telemetry.span("collect.file", attributes={"maas.file.name": f}):
            messenger.handle_message(
                FakeConfig(routing_key="rk", model_name="M", exchange_name="ex"),
                f"id-{f}",
            )
    pub = telemetry_capture.span("publish ex")
    assert pub.parent is None and len(pub.links) == 3
    assert messenger._group_links == {}


def test_flush_passes_links_and_clears(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 10}
    for f in ("f1", "f2"):
        with telemetry.span("collect.file"):
            messenger.handle_message(FakeConfig("rk", exchange_name="ex"), f)
    assert len(messenger._group_links[("ex", "rk", "M")]) == 2
    messenger.flush_message_groups()
    assert len(telemetry_capture.span("publish ex").links) == 2
    assert messenger._group_links == {}
    assert messenger.message_groups == {}


def test_links_capped_at_128(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 1000}
    for _ in range(130):
        with telemetry.span("collect.file"):
            messenger.handle_message(FakeConfig("rk", exchange_name="ex"), "d")
    assert len(messenger._group_links[("ex", "rk", "M")]) == 128


def test_same_span_context_not_duplicated(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 1000}
    with telemetry.span("collect.file"):
        for i in range(3):
            messenger.handle_message(FakeConfig("rk", exchange_name="ex"), f"d{i}")
    assert len(messenger._group_links[("ex", "rk", "M")]) == 1


def test_routed_counter(telemetry_capture, messenger):
    config = FakeConfig("rk", exchange_name="ex", interface_name="iface")
    messenger.handle_message(config, "a")
    messenger.handle_message(config, "b")
    point = telemetry_capture.point(
        "maas.collector.documents.routed", node="iface", routing_key="rk"
    )
    assert point.value == 2


def test_publish_error_outcome(telemetry_capture, messenger):
    messenger._producer.publish.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        messenger.send_to_queue(
            "rk", MAASMessage(document_class="M", document_ids=["a"]), "ex"
        )
    assert (
        telemetry_capture.point(
            "maas.messages.published", exchange="ex", routing_key="rk", outcome="error"
        ).value
        == 1
    )


def test_on_return_counts(telemetry_capture, messenger):
    messenger._on_return(Exception(312, "NO_ROUTE"), "ex", "rk", Mock())
    assert (
        telemetry_capture.point("maas.messages.returned", reason="NO_ROUTE").value == 1
    )
