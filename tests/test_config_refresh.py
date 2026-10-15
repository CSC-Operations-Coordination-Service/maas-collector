"""Tests for restarting a collector when its OpenSearch configuration changes"""

import logging
import time
from unittest.mock import patch

import pytest
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError

from fakeopensearch import add_config_document

from maas_collector.rawdata import configuration
from maas_collector.rawdata.collector import filecollector
from maas_collector.rawdata.collector.filecollector import (
    CONFIG_CHANGED_EXIT_CODE,
    CollectorArgs,
    FileCollector,
)
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.configstore import ConfigStore

INDEX = "maas-collector-config"


class FakeClock:
    """replace the time module of filecollector: monotonic moves on demand, the
    other functions are the real ones"""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        """seconds"""
        return self.now

    @staticmethod
    def time() -> float:
        """wall clock"""
        return time.time()

    @staticmethod
    def sleep(seconds: float):
        """real sleep"""
        time.sleep(seconds)


def entry(model, interface_name: str) -> dict:
    """a collector entry building with FileCollectorConfiguration"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": model,
        "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
    }


def inline(model_name: str, interface_name: str = "ITF_A") -> dict:
    """content with one collector entry declaring its model"""
    return {
        "collectors": [
            entry({"name": model_name, "index": "", "fields": []}, interface_name)
        ]
    }


@pytest.fixture
def clock(monkeypatch):
    fake_clock = FakeClock()
    monkeypatch.setattr(filecollector, "time", fake_clock)
    return fake_clock


@pytest.fixture
def store(fake_opensearch):
    """a configuration index holding odata/a.json v1 and shared/b.json v1"""
    config_store = ConfigStore(fake_opensearch, INDEX)
    config_store.publish("odata/a.json", inline("RefreshModelA"), ["odata"], "a", "v1")
    config_store.publish(
        "shared/b.json",
        inline("RefreshModelB", "ITF_B"),
        ["odata", "sftp"],
        "a",
        "v1",
    )
    return config_store


def start_collector(**kwargs) -> FileCollector:
    collector = FileCollector(
        CollectorArgs(
            es_url="http://opensearch.example:9200",
            config_backend="opensearch",
            config_store=ConfigStoreArgs(index=INDEX, service="odata"),
            **kwargs,
        )
    )
    with patch("opensearchpy.connection.connections.Connections.create_connection"):
        collector.setup()
    return collector


def test_nothing_is_read_before_the_refresh_period(clock, store):
    collector = start_collector()
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")

    clock.now += 299
    with patch.object(
        collector.config_provider, "load", wraps=collector.config_provider.load
    ) as load:
        collector.check_config_changes()

    load.assert_not_called()


def test_unchanged_configuration_keeps_the_collector_running(clock, store):
    collector = start_collector()

    clock.now += 300
    collector.check_config_changes()

    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_changed_configuration_exits_with_code_75(clock, store, caplog):
    collector = start_collector()
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")
    store.publish(
        "odata/c.json", inline("RefreshModelC", "ITF_C"), ["odata"], "b", "v1"
    )

    clock.now += 300
    with pytest.raises(SystemExit) as exit_info:
        collector.check_config_changes()

    assert exit_info.value.code == CONFIG_CHANGED_EXIT_CODE == 75
    assert (
        "configuration changed: odata/a.json v1→v2, odata/c.json added, restarting"
        in caplog.text
    )


def test_collector_loop_exits_after_its_clean_up(clock, store):
    collector = start_collector(watch_period=0)
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")
    clock.now += 300

    with patch.object(collector, "start_healthcheck"), patch.object(
        collector, "stop_healthcheck"
    ) as stop_healthcheck, patch.object(collector, "_healthcheck"), patch.object(
        collector, "loop_body"
    ) as loop_body, patch.object(
        collector, "_flush_message_groups"
    ) as flush, pytest.raises(
        SystemExit
    ) as exit_info:
        collector.run([])

    assert exit_info.value.code == 75
    loop_body.assert_not_called()
    stop_healthcheck.assert_called_once()
    flush.assert_called()


def test_invalid_change_is_logged_and_ignored(clock, store, fake_opensearch, caplog):
    collector = start_collector()
    before = dict(configuration.MODEL_DICT)
    # written straight into the index, without the validation of ConfigStore
    add_config_document(
        fake_opensearch,
        "odata/a.json",
        2,
        ["odata"],
        {
            "collectors": [
                {
                    **inline("RejectedModel")["collectors"][0],
                    "extractor": {"class": "NoSuchExtractor"},
                }
            ]
        },
    )

    clock.now += 300
    collector.check_config_changes()

    assert (
        "configuration change rejected: odata/a.json@2: AttributeError" in caplog.text
    )
    assert configuration.MODEL_DICT == before
    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_rejected_versions_are_not_validated_again(clock, store, fake_opensearch):
    collector = start_collector()
    add_config_document(
        fake_opensearch, "odata/a.json", 2, ["odata"], {"interfaces": []}
    )

    with patch(
        "maas_collector.rawdata.collector.filecollector.validate_config_sources",
        wraps=filecollector.validate_config_sources,
    ) as validate:
        clock.now += 300
        collector.check_config_changes()
        clock.now += 300
        collector.check_config_changes()

    assert validate.call_count == 1

    # a fixed version is a new change
    store.publish("odata/a.json", inline("RefreshModelA3"), ["odata"], "b", "fix")
    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()


def test_unreachable_opensearch_keeps_the_collector_running(
    clock, store, fake_opensearch, caplog
):
    caplog.set_level(logging.INFO)
    collector = start_collector()
    fake_opensearch.outage = OpenSearchConnectionError("N/A", "unreachable", None)

    clock.now += 300
    collector.check_config_changes()
    clock.now += 300
    collector.check_config_changes()

    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 2
    assert errors[0].exc_info is not None
    assert errors[1].exc_info is None
    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]

    # back online, and changed meanwhile
    fake_opensearch.outage = None
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")
    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()
    assert "Configuration checked again" in caplog.text


def test_service_left_without_configuration_keeps_running(clock, store, caplog):
    """deleting every file of a service must not turn into a restart loop"""
    collector = start_collector()
    store.delete("odata/a.json", "b", "gone")
    store.publish(
        "shared/b.json", inline("RefreshModelB", "ITF_B"), ["sftp"], "b", "sftp only"
    )

    clock.now += 300
    collector.check_config_changes()

    assert "no configuration for service odata" in caplog.text
    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_service_removed_from_a_shared_path_restarts(clock, store, caplog):
    collector = start_collector()
    store.publish(
        "shared/b.json", inline("RefreshModelB", "ITF_B"), ["sftp"], "b", "sftp only"
    )

    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()

    assert "configuration changed: shared/b.json removed, restarting" in caplog.text


def test_model_defined_in_another_document_resolves(clock, store, caplog):
    collector = start_collector()
    store.publish(
        "odata/models.json",
        {"models": [{"name": "SharedRefreshModel", "index": "", "fields": []}]},
        ["odata"],
        "b",
        "shared model",
    )
    store.publish(
        "odata/a.json",
        {"collectors": [entry("SharedRefreshModel", "ITF_A")]},
        ["odata"],
        "b",
        "use the shared model",
    )

    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()


def test_model_missing_from_the_new_set_is_rejected(clock, store, caplog):
    collector = start_collector()
    store.publish(
        "odata/a.json",
        {"collectors": [entry("ModelNobodyDefines", "ITF_A")]},
        ["odata"],
        "b",
        "dangling model",
    )

    clock.now += 300
    collector.check_config_changes()

    assert (
        "configuration change rejected: odata/a.json@2: model ModelNobodyDefines"
        in caplog.text
    )


def test_zero_refresh_period_never_checks(clock, store):
    collector = start_collector(config_refresh_period=0)
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")

    clock.now += 100_000
    collector.check_config_changes()

    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_file_backend_never_checks(clock, tmp_path):
    conf = tmp_path / "conf.json"
    conf.write_text('{"amqp": {}}', encoding="UTF-8")
    collector = FileCollector(CollectorArgs(rawdata_config=str(conf)))
    collector.load_config()
    conf.write_text('{"amqp": {"rk": {"chunk_size": 3}}}', encoding="UTF-8")

    clock.now += 100_000
    with patch.object(collector.config_provider, "load") as load:
        collector.check_config_changes()

    load.assert_not_called()
