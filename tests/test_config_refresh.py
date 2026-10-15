"""Tests for restarting a collector when its OpenSearch configuration changes"""

import json
import logging
import os
import subprocess
import sys
import textwrap
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
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.configstore import ConfigStore

INDEX = "maas-collector-config"

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


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


MONITOR_CONF = os.path.join(CONF_DIR, "maas-collector-interface-monitoring.json")

PROBED_CONF = os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json")

CREDENTIAL_FILE = os.path.join(CONF_DIR, "test-maas-collector-credentials.json")


def test_monitor_rejects_a_probed_entry_its_collector_cannot_load(
    clock, fake_opensearch, caplog
):
    """the monitoring class accepts any entry, the probed collector class may not"""
    store = ConfigStore(fake_opensearch, INDEX)
    store.publish(
        "monitor/main.json", read_json(MONITOR_CONF), ["monitor-main"], "a", "v1"
    )
    store.publish(
        "odata/basic.json", read_json(PROBED_CONF), ["monitor-main"], "a", "v1"
    )
    monitor = InterfaceMonitor(
        CollectorArgs(
            es_url="http://opensearch.example:9200",
            config_backend="opensearch",
            config_store=ConfigStoreArgs(index=INDEX, service="monitor-main"),
            credential_file=CREDENTIAL_FILE,
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    with patch("opensearchpy.connection.connections.Connections.create_connection"):
        monitor.setup()

    broken = read_json(PROBED_CONF)
    broken["collectors"][0]["class"] = "NoSuchCollectorConfiguration"
    add_config_document(
        fake_opensearch, "odata/basic.json", 2, ["monitor-main"], broken
    )

    try:
        clock.now += 300
        monitor.check_config_changes()
    finally:
        monitor.exit_gracefully(9, "")

    assert "configuration change rejected: odata/basic.json@2: KeyError" in caplog.text


MONITOR_RESTART_SCRIPT = """
import json
import sys
from unittest.mock import MagicMock, patch

from opensearchpy.connection.connections import connections

from fakeopensearch import FakeOpenSearch
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.configstore import ConfigStore

conf_dir, credential_file = sys.argv[1], sys.argv[2]


def read_json(name):
    with open(f"{conf_dir}/{name}", encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


client = FakeOpenSearch()
connections.add_connection("default", client)
store = ConfigStore(client, "maas-collector-config")
store.publish(
    "monitor/main.json",
    read_json("maas-collector-interface-monitoring.json"),
    ["monitor-main"],
    "a",
    "v1",
)
probed = read_json("test-maas-collector-odata-basic.json")
store.publish("odata/basic.json", probed, ["monitor-main"], "a", "v1")

monitor = InterfaceMonitor(
    CollectorArgs(
        es_url="http://opensearch.example:9200",
        config_backend="opensearch",
        config_store=ConfigStoreArgs(service="monitor-main"),
        config_refresh_period=1,
        credential_file=credential_file,
        watch_period=1,
    ),
    InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
)
with patch("opensearchpy.connection.connections.Connections.create_connection"):
    monitor.setup()

store.publish("odata/basic.json", probed, ["monitor-main"], "a", "v2")
monitor._config_check_time -= 10

monitor._healthcheck = MagicMock()
with patch.object(monitor, "start_healthcheck"), patch.object(
    monitor, "stop_healthcheck"
), patch.object(monitor, "loop_body"):
    monitor.run([""])
"""


def test_monitor_process_exits_on_a_configuration_change(tmp_path):
    """the threads of the interface monitor must not keep the process alive"""
    script = tmp_path / "monitor_restart.py"
    script.write_text(textwrap.dedent(MONITOR_RESTART_SCRIPT))
    tests_dir = os.path.dirname(__file__)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [os.path.join(tests_dir, "..", "src"), tests_dir, env.get("PYTHONPATH", "")]
    )

    result = subprocess.run(
        [sys.executable, str(script), CONF_DIR, CREDENTIAL_FILE],
        env=env,
        capture_output=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == CONFIG_CHANGED_EXIT_CODE, result.stderr.decode()[-2000:]
