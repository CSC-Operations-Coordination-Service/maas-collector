"""File collector telemetry: collect.file span, counts, metrics and links"""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest

from opensearchpy.connection.connections import connections as es_connections

from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
)
from maas_model import telemetry

from maas_collector.rawdata.messenger import Messenger

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf", "backup_report_off")

BULK = "maas_collector.rawdata.collector.filecollector.parallel_bulk"

NODE = "BACKED_UP"

RESULTS = [
    (True, {"index": {"_id": "a", "_index": "raw-x", "result": "created"}}),
    (True, {"index": {"_id": "b", "_index": "raw-x", "result": "updated"}}),
    (False, {"index": {"_id": "c", "error": "boom"}}),
]


@pytest.fixture(name="default_connection")
def fixture_default_connection():
    """register a mock under the alias extract_from_file uses"""
    es_connections.add_connection("default", Mock())
    yield
    es_connections._conn = {}
    es_connections._kwargs = {}


@pytest.fixture(name="collector")
def fixture_collector(default_connection):
    """a collector with a mocked healthcheck and a real messenger on a mock producer"""
    instance = FileCollector(CollectorArgs(rawdata_config_dir=CONF_DIR))
    instance.load_config()
    instance.configs[0].backup = None
    instance._healthcheck = Mock()
    instance._messenger = Messenger("amqp://localhost:5672", exchange_name="ex")
    instance._messenger._producer = MagicMock()
    instance._messenger._producer.channel = MagicMock()
    return instance


@pytest.fixture(name="sample_file")
def fixture_sample_file(tmp_path):
    """a file to ingest"""
    path = tmp_path / "sample.log"
    path.write_text("a line\n")
    return str(path)


def test_file_span_and_metrics(telemetry_capture, collector, sample_file):
    with patch(BULK, return_value=RESULTS):
        collector.extract_from_file(sample_file, collector.configs[0])
    s = telemetry_capture.span("collect.file")
    assert s.attributes["maas.documents.created"] == 1
    assert s.attributes["maas.documents.updated"] == 1
    assert s.attributes["maas.documents.errors"] == 1
    assert s.attributes["maas.documents.unmodified"] == 0
    assert s.attributes["maas.file.name"] == os.path.basename(sample_file)
    assert s.attributes["maas.interface"] == NODE
    assert s.attributes["maas.collector.type"] == "FileCollector"
    assert (
        telemetry_capture.point("maas.collector.files", node=NODE, outcome="error").value
        == 1
    )
    assert (
        telemetry_capture.point(
            "maas.collector.documents", node=NODE, result="created"
        ).value
        == 1
    )
    assert telemetry_capture.point("maas.collector.file.duration", node=NODE).count == 1


def test_publish_is_child_of_file_span(telemetry_capture, collector, sample_file):
    collector.configs[0].routing_key = "rk"
    with patch(BULK, return_value=RESULTS[:1]):
        collector.extract_from_file(sample_file, collector.configs[0])
    parent = telemetry_capture.span("collect.file")
    publish = telemetry_capture.span("publish ex")
    assert publish.parent.span_id == parent.context.span_id
    assert publish.context.trace_id == parent.context.trace_id
    headers = collector._messenger._producer.publish.call_args.kwargs["headers"]
    assert "maas.origin.interface" in headers["baggage"]


def test_exception_still_measured(telemetry_capture, collector, sample_file):
    with patch(BULK, side_effect=RuntimeError("down")):
        with pytest.raises(RuntimeError):
            collector.extract_from_file(sample_file, collector.configs[0])
    assert (
        telemetry_capture.point("maas.collector.files", node=NODE, outcome="error").value
        == 1
    )


def test_empty_file_outcome(telemetry_capture, collector, sample_file):
    with patch(BULK, return_value=[]):
        collector.extract_from_file(sample_file, collector.configs[0])
    assert (
        telemetry_capture.point("maas.collector.files", node=NODE, outcome="empty").value
        == 1
    )


def test_unmodified_counted_without_force_message(
    telemetry_capture, collector, sample_file
):
    def bulk(_conn, iterator, **_kwargs):
        iterator.unmodified_ids.extend(["u1", "u2"])
        return []

    with patch(BULK, side_effect=bulk):
        collector.extract_from_file(sample_file, collector.configs[0])
    assert telemetry_capture.span("collect.file").attributes[
        "maas.documents.unmodified"
    ] == 2
    assert (
        telemetry_capture.point(
            "maas.collector.documents", node=NODE, result="unmodified"
        ).value
        == 2
    )


def test_disabled_extract_unchanged(collector, sample_file):
    collector._messenger = Mock()
    with patch(BULK, return_value=RESULTS):
        collector.extract_from_file(sample_file, collector.configs[0])
    assert collector._messenger.handle_message.call_count == 2
    assert collector._last_collect_span_context is None


def test_ingest_success_links_file_span(telemetry_capture, collector, sample_file):
    config = collector.configs[0]
    config.file_routing_key = "file.rk"
    collector._messenger = Mock()
    with patch(BULK, return_value=RESULTS[:1]):
        collector.extract_from_file(sample_file, config)
    collector.on_ingest_success(sample_file, config)
    links = collector._messenger.send_to_queue.call_args.kwargs["links"]
    span_context = telemetry_capture.span("collect.file").context
    assert [link.span_id for link in links] == [span_context.span_id]


def test_second_ingest_success_has_no_stale_links(
    telemetry_capture, collector, sample_file
):
    config = collector.configs[0]
    config.file_routing_key = "file.rk"
    collector._messenger = Mock()
    with patch(BULK, return_value=RESULTS[:1]):
        collector.extract_from_file(sample_file, config)
    collector.on_ingest_success(sample_file, config)
    collector.on_ingest_success(sample_file, config)
    calls = collector._messenger.send_to_queue.call_args_list
    assert "links" in calls[0].kwargs
    assert "links" not in calls[1].kwargs


def test_bad_sidecar_still_measured(telemetry_capture, collector, sample_file):
    with open(sample_file + "-maas-meta.json", "w", encoding="utf-8") as sidecar:
        sidecar.write("{not json")
    with patch(BULK, return_value=[]):
        with pytest.raises(ValueError):
            collector.extract_from_file(sample_file, collector.configs[0])
    assert (
        telemetry_capture.point("maas.collector.files", node=NODE, outcome="error").value
        == 1
    )
    assert telemetry_capture.point("maas.collector.file.duration", node=NODE).count == 1
    assert telemetry_capture.span("collect.file").status.status_code.name == "ERROR"


def test_loop_duration_recorded(telemetry_capture, collector):
    """one iteration records one loop duration, run then shuts telemetry down"""
    collector.args.watch_period = 0
    collector.start_healthcheck = Mock()
    collector.stop_healthcheck = Mock()
    collector.loop_body = Mock()
    collector._messenger = Mock()

    collector.run([])

    assert (
        telemetry_capture.point(
            "maas.collector.loop.duration", collector_type="FileCollector"
        ).count
        == 1
    )
    assert not telemetry.enabled()
