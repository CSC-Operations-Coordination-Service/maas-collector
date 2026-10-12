"""A failing interface must not prevent the next ones from being collected"""

import logging
from unittest.mock import MagicMock, Mock, patch

import pytest
from requests.exceptions import ConnectionError as RequestConnectionError

from maas_collector.rawdata.collector.httpcollector import HttpCollector
from maas_collector.rawdata.collector.odatacollector import ODataCollector
from maas_collector.rawdata.replay import ReplayArgs


def make_collector(collector_class, interface_names):
    """A collector instance without running its constructor nor reaching databases"""
    collector = collector_class.__new__(collector_class)
    collector.logger = logging.getLogger(collector_class.__name__)
    collector.configs = [
        Mock(interface_name=name, no_credential=False) for name in interface_names
    ]
    collector.should_stop_loop = False
    collector._messenger = Mock()
    collector._healthcheck = Mock()
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
