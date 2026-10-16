"""A failing interface must not prevent the next ones from being collected"""

import logging
from unittest.mock import MagicMock, Mock, patch

import pytest
from requests.exceptions import ConnectionError as RequestConnectionError

from maas_collector.rawdata.cli.odata import odata_collector_main
from maas_collector.rawdata.collector.ftpcollector import FTPCollector
from maas_collector.rawdata.collector.httpcollector import HttpCollector
from maas_collector.rawdata.collector.jiraxcollector import JIRAExtendedCollector
from maas_collector.rawdata.collector.mpipcollector import MpipCollector
from maas_collector.rawdata.collector.odatacollector import ODataCollector
from maas_collector.rawdata.collector.rosftpcollector import ReadOnlySFTPCollector
from maas_collector.rawdata.collector.s3collector import S3Collector
from maas_collector.rawdata.replay import ReplayArgs


def make_collector(collector_class, interface_names, **config_attributes):
    """A collector instance without running its constructor nor reaching databases"""
    collector = collector_class.__new__(collector_class)
    collector.logger = logging.getLogger(collector_class.__name__)
    collector.configs = [
        Mock(interface_name=name, no_credential=False, **config_attributes)
        for name in interface_names
    ]
    collector.should_stop_loop = False
    collector._messenger = Mock()
    collector._healthcheck = Mock()
    collector.replay_failures = []
    return collector


@pytest.mark.parametrize(
    "collector_class,module",
    [
        (HttpCollector, "maas_collector.rawdata.collector.httpcollector"),
        (ODataCollector, "maas_collector.rawdata.collector.odatacollector"),
    ],
)
def test_failing_interface_does_not_skip_next_ones(collector_class, module):
    """An unreachable interface is logged and the loop moves to the next one"""
    collector = make_collector(collector_class, ["first", "second", "third"])

    with patch(f"{module}.CollectorJournal", MagicMock()), patch.object(
        collector_class,
        "ingest_http",
        side_effect=[None, RequestConnectionError("unreachable"), None],
    ) as ingest_http:
        collector.ingest_all_interfaces()

    assert [call.args[0].interface_name for call in ingest_http.call_args_list] == [
        "first",
        "second",
        "third",
    ]


@pytest.mark.parametrize(
    "collector_class,entry_point,collect_method,config_attributes",
    [
        (MpipCollector, "ingest_all_interfaces", "ingest_http", {}),
        (S3Collector, "ingest_all_interfaces", "ingest_interface", {"buckets": ["b"]}),
        (JIRAExtendedCollector, "ingest", "ingest_tickets", {}),
        (ReadOnlySFTPCollector, "ingest", "ingest_rosftp", {}),
        (FTPCollector, "ingest", "get_remote_file_list", {"directories": ["/out"]}),
    ],
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
def test_other_collectors_go_on_after_a_failing_interface(
    collector_class, entry_point, collect_method, config_attributes
):
    """The collect of the second interface fails: the third one is still collected"""
    collector = make_collector(
        collector_class, ["first", "second", "third"], **config_attributes
    )

    # the FTP connection is opened and closed around each interface: no server here
    with patch(
        f"{collector_class.__module__}.CollectorJournal", MagicMock()
    ), patch.object(FTPCollector, "build_connection"), patch.object(
        FTPCollector, "clear_ftp_connection"
    ), patch.object(
        collector_class,
        collect_method,
        side_effect=[[], RequestConnectionError("unreachable"), []],
    ) as collect:
        getattr(collector, entry_point)()

    assert collect.call_count == 3


def test_replay_still_retries_a_failing_interface():
    """Replay relies on ingest_interface raising: the error must not be swallowed"""
    collector = make_collector(HttpCollector, ["first"])
    collector.args = Mock()

    replay_args = ReplayArgs(
        interface_name=["first"],
        start_date_arg="2026-01-01T00:00:00Z",
        end_date_arg="2026-01-02T00:00:00Z",
        retry=2,
    )

    module = "maas_collector.rawdata.collector.httpcollector"

    with patch(f"{module}.CollectorReplayJournal") as journal_class, patch(
        f"{module}.sleep"
    ), patch.object(
        HttpCollector,
        "ingest_http",
        side_effect=[RequestConnectionError("unreachable"), None],
    ) as ingest_http:
        journal_class.return_value.last_date = None

        collector.ingest_replay(replay_args)

    assert ingest_http.call_count == 2
    journal_class.return_value.complete.assert_called_once()


def test_replay_refreshes_the_credentials_before_each_retry():
    """a replay runs inside one loop: a rotated secret must reach its retries"""
    collector = make_collector(HttpCollector, ["first"])
    collector.args = Mock()
    collector.refresh_credentials = Mock()

    replay_args = ReplayArgs(
        interface_name=["first"],
        start_date_arg="2026-01-01T00:00:00Z",
        end_date_arg="2026-01-02T00:00:00Z",
        retry=3,
    )

    module = "maas_collector.rawdata.collector.httpcollector"

    with patch(f"{module}.CollectorReplayJournal") as journal_class, patch(
        f"{module}.sleep"
    ), patch.object(
        HttpCollector,
        "ingest_http",
        side_effect=[
            RequestConnectionError("401"),
            RequestConnectionError("401"),
            None,
        ],
    ):
        journal_class.return_value.last_date = None

        collector.ingest_replay(replay_args)

    assert collector.refresh_credentials.call_count == 2


def run_replay(collector, ingest_http_effect, retry):
    """run a one-interface replay of the collector with a scripted ingest_http"""
    collector.args = Mock()
    module = "maas_collector.rawdata.collector.httpcollector"

    with patch(f"{module}.CollectorReplayJournal") as journal_class, patch(
        f"{module}.sleep"
    ), patch.object(HttpCollector, "ingest_http", side_effect=ingest_http_effect):
        journal_class.return_value.last_date = None

        collector.ingest_replay(
            ReplayArgs(
                ["A"], "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "s", retry
            )
        )


def test_exhausted_replay_is_listed_in_replay_failures():
    collector = make_collector(HttpCollector, ["A"])

    run_replay(collector, RequestConnectionError("unreachable"), retry=1)

    assert collector.replay_failures == ["A"]


def test_successful_replay_has_no_failures():
    collector = make_collector(HttpCollector, ["A"])

    run_replay(collector, None, retry=1)

    assert collector.replay_failures == []


REPLAY_ARGV = [
    "--replay-interface-name",
    "A",
    "--replay-start-date",
    "2026-01-01T00:00:00Z",
    "--replay-end-date",
    "2026-01-02T00:00:00Z",
]


def test_odata_cli_exits_1_when_a_replay_failed():
    with patch("maas_collector.rawdata.cli.odata.ODataCollector") as collector_class:
        collector = collector_class.return_value
        collector.replay_failures = []
        collector.run.side_effect = lambda *_: collector.replay_failures.append("A")

        with pytest.raises(SystemExit) as exit_info:
            odata_collector_main(REPLAY_ARGV)

    assert exit_info.value.code == 1


def test_odata_cli_returns_when_no_replay_failed():
    with patch("maas_collector.rawdata.cli.odata.ODataCollector") as collector_class:
        collector_class.return_value.replay_failures = []

        odata_collector_main(REPLAY_ARGV)
