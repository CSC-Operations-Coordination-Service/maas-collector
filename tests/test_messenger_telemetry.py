"""Messenger telemetry: publish span, trace headers, chunk links, counts, returns"""

import dataclasses
import json
from unittest.mock import MagicMock, Mock

import pytest

from maas_model import MAASMessage, telemetry

from maas_collector.rawdata.messenger import Messenger

from test_messenger import FakeConfig


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
    messenger.send_to_queue(
        "rk",
        MAASMessage(
            message_id="mid", date="2026-01-01T00:00:00.000Z", document_class="M", document_ids=["a"], pipeline=["x"]
        ),
        exchange_name="ex",
    )
    call = messenger._producer.publish.call_args
    assert call.kwargs["headers"] == {}
    fresh = MAASMessage(
        message_id="mid", date="2026-01-01T00:00:00.000Z", document_class="M", document_ids=["a"], pipeline=["x"]
    )
    fresh.document_indices = []
    fresh.pipeline = ["Collector"]
    assert call.args[0] == json.dumps(dataclasses.asdict(fresh))
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
    assert messenger._group_contexts == {}


def _file(name, messenger, doc, config=None):
    with telemetry.span("collect.file", attributes={"maas.file.name": name}):
        messenger.handle_message(config or FakeConfig("rk", "M", "ex"), doc)


def _linked_files(telemetry_capture, pub):
    names = {
        s.context.span_id: s.attributes["maas.file.name"]
        for s in [x for x in telemetry_capture.spans() if x.name == "collect.file"]
    }
    return {names[link.context.span_id] for link in pub.links}


def test_links_exact_per_chunk_and_flush_is_root(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 3}
    for name in ("A", "A", "B", "C", "C"):
        _file(name, messenger, f"d-{name}")
    with telemetry.span("outer"):
        messenger.flush_message_groups()
    pubs = [x for x in telemetry_capture.spans() if x.name == "publish ex"]
    assert len(pubs) == 2
    first, flushed = pubs
    assert _linked_files(telemetry_capture, first) == {"A", "B"}
    assert _linked_files(telemetry_capture, flushed) == {"C"}
    assert first.parent is None and flushed.parent is None
    assert messenger._group_contexts == {}
    assert messenger.message_groups == {}


def test_links_capped_at_128(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 200}
    for i in range(130):
        _file(f"f{i}", messenger, f"d{i}")
    messenger.flush_message_groups()
    assert len(telemetry_capture.span("publish ex").links) == 128


def test_same_file_linked_once(telemetry_capture, messenger):
    messenger.chunk_config = {"rk": 1000}
    with telemetry.span("collect.file"):
        for i in range(3):
            messenger.handle_message(FakeConfig("rk", "M", "ex"), f"d{i}")
    messenger.flush_message_groups()
    assert len(telemetry_capture.span("publish ex").links) == 1


def test_direct_send_stays_child_of_file(telemetry_capture, messenger):
    _file("A", messenger, "d")
    pub = telemetry_capture.span("publish ex")
    assert pub.parent.span_id == telemetry_capture.span("collect.file").context.span_id


def test_routed_counter(telemetry_capture, messenger):
    config = FakeConfig("rk", exchange_name="ex", interface_name="iface")
    messenger.handle_message(config, "a")
    messenger.handle_message(config, "b")
    point = telemetry_capture.point(
        "maas.collector.documents.routed",
        node="iface",
        exchange="ex",
        routing_key="rk",
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


def test_on_return_never_raises(telemetry_capture, messenger):
    messenger._on_return(Exception("x"), "ex", "rk", Mock())
    messenger._on_return(Exception(), "ex", "rk", Mock())
    assert (
        telemetry_capture.point("maas.messages.returned", reason="other").value == 2
    )
