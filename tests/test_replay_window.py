"""A replay collects exactly the requested window, offsets are for live runs"""

import datetime
from unittest.mock import Mock, patch

from maas_collector.rawdata.collector.odatacollector import ODataCollector

START = datetime.datetime(2026, 1, 1, 10, 0, tzinfo=datetime.UTC)
END = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC)


def make_strategy(start, end, journal):
    """Build the OData v4 strategy with a config using both offsets"""
    config = Mock(
        end_date_time_offset=15,
        odata_start_offset=30,
        refresh_interval=10,
        max_time_window=15,
    )
    with patch(
        "maas_collector.rawdata.collector.http.abstract_query_strategy."
        "build_authentication"
    ):
        return ODataCollector.IMPL_DIR["v4"](Mock(), config, Mock(), start, end, journal)


def test_replay_window_is_exact():
    journal = Mock(is_a_replay_journal=True, last_date=None)
    strategy = make_strategy(START, END, journal)
    assert strategy.start_date == START
    assert strategy.end_date == END


def test_normal_run_still_applies_both_offsets():
    journal = Mock(is_a_replay_journal=False, last_date=START)
    strategy = make_strategy(None, END, journal)
    assert strategy.end_date == END - datetime.timedelta(minutes=15)
    assert strategy.start_date == START - datetime.timedelta(minutes=30)


def test_resumed_replay_starts_at_journal_last_date_without_offset():
    resumed = START + datetime.timedelta(hours=1)
    journal = Mock(is_a_replay_journal=True, last_date=resumed)
    strategy = make_strategy(START, END, journal)
    assert strategy.start_date == resumed
