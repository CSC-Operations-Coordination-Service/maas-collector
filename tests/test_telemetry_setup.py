"""Tests of the telemetry setup of the collectors: flag, configure, topology."""

import json
import shutil
from types import SimpleNamespace
from unittest.mock import patch

from maas_model import telemetry

from maas_collector.rawdata.cli.lib.args import common_parser, get_collector_args
from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
)
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
from maas_collector.rawdata.telemetry import (
    declare_topology,
    node_name,
    setup_telemetry,
)

import os

CONF = os.path.join(os.path.dirname(__file__), "conf")


def model(name):
    return {
        "index": f"raw-data-{name.lower()}",
        "name": name,
        "fields": [{"name": "line", "type": "Keyword"}],
    }


def collector_conf(**extra):
    conf = {
        "id_field": "",
        "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
    }
    conf.update(extra)
    return conf


def write_confs(tmp_path, collectors):
    (tmp_path / "conf.json").write_text(json.dumps({"collectors": collectors}))
    return str(tmp_path)


def load(conf_dir, **args):
    collector = FileCollector(CollectorArgs(rawdata_config_dir=conf_dir, **args))
    collector.load_config()
    return collector


def two_configs(tmp_path):
    return write_confs(
        tmp_path,
        [
            collector_conf(
                interface_name="IF_ONE",
                routing_key="new.raw.one",
                file_routing_key="file.raw.one",
                exchange_name="custom-ex",
                model=model("ModelOne"),
            ),
            collector_conf(
                routing_key="new.raw.two",
                model=model("ModelTwo"),
            ),
        ],
    )


def test_flag_parsed():
    assert common_parser().parse_args(["--telemetry"]).telemetry is True
    assert common_parser().parse_args([]).telemetry is False
    args = get_collector_args(
        CollectorArgs, common_parser().parse_args(["--telemetry"])
    )
    assert args.telemetry is True
    assert CollectorArgs().telemetry is False


def test_setup_declares_topology(telemetry_capture, tmp_path):
    collector = load(two_configs(tmp_path))

    declare_topology(collector)

    point = telemetry_capture.point(
        "maas.topology.route_info", node="IF_ONE", routing_key="new.raw.one"
    )
    assert point.attributes["exchange"] == "custom-ex"
    assert point.attributes["direction"] == "out"
    assert point.attributes["document_class"] == "ModelOne"
    file_point = telemetry_capture.point(
        "maas.topology.route_info", routing_key="file.raw.one"
    )
    assert file_point.attributes["exchange"] != "custom-ex"
    assert file_point.attributes["node"] == "IF_ONE"
    assert (
        telemetry_capture.point("maas.node_info", node="IF_ONE")
        .attributes["impl"]
        .endswith("FileCollector")
    )


def test_node_name_falls_back_to_model_name(telemetry_capture, tmp_path):
    collector = load(two_configs(tmp_path))
    assert node_name(collector.configs[0]) == "IF_ONE"
    assert node_name(collector.configs[1]) == "ModelTwo"

    declare_topology(collector)

    telemetry_capture.point(
        "maas.topology.route_info", node="ModelTwo", routing_key="new.raw.two"
    )


def test_setup_telemetry_disabled_by_default(monkeypatch, tmp_path):
    monkeypatch.delenv("MAAS_TELEMETRY", raising=False)
    collector = load(two_configs(tmp_path))

    assert setup_telemetry(collector) is False
    assert telemetry.enabled() is False


def test_setup_telemetry_enabled_by_flag(monkeypatch, tmp_path):
    monkeypatch.delenv("MAAS_TELEMETRY", raising=False)
    collector = load(two_configs(tmp_path), telemetry=True)
    try:
        assert setup_telemetry(collector) is True
        assert telemetry.enabled() is True
    finally:
        telemetry.topology._reset_for_tests()
        telemetry.shutdown()


@patch("opensearchpy.connection.connections.Connections.create_connection")
def test_file_collector_setup_calls_setup_telemetry(_es, tmp_path):
    collector = FileCollector(CollectorArgs(rawdata_config_dir=two_configs(tmp_path)))
    with patch("maas_collector.rawdata.collector.filecollector.setup_telemetry") as st:
        collector.setup()
    st.assert_called_once_with(collector)


@patch("opensearchpy.connection.connections.Connections.create_connection")
@patch("maas_collector.rawdata.messenger.Messenger.setup", return_value=True)
def test_monitor_setup_declares_only_monitor_configs(
    _amqp, _es, telemetry_capture, tmp_path
):
    for conf in (
        "test-maas-collector-odata-basic.json",
        "test-maas-collector-odata-no-auth.json",
    ):
        shutil.copy(os.path.join(CONF, conf), tmp_path)
    args = CollectorArgs(
        rawdata_config=os.path.join(CONF, "maas-collector-interface-monitoring.json"),
        rawdata_config_dir=str(tmp_path),
        credential_file=os.path.join(CONF, "test-maas-collector-credentials.json"),
    )
    monitor = InterfaceMonitor(
        args, InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring")
    )
    monitor.setup()
    monitor.exit_gracefully(9, "")

    routes = {
        p.attributes["node"]
        for p in telemetry_capture.points("maas.topology.route_info")
    }
    assert routes == {"OMCS_Monitoring"}
