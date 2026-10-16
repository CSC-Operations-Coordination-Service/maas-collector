"""Replay of jira interfaces"""

import datetime
import logging
from unittest.mock import MagicMock, Mock, patch

import pytest

from maas_collector.rawdata.cli.jirax import jirax_collector_main
from maas_collector.rawdata.collector.journal import CollectorReplayJournal
from maas_collector.rawdata.collector.jiraxcollector import (
    JIRAExtendedCollector,
    jql_for,
)
from maas_collector.rawdata.replay import ReplayArgs

MODULE = "maas_collector.rawdata.collector.jiraxcollector"

JQL = "project = SOA AND updated > {date_criteria} ORDER BY updated ASC"

UTC = datetime.UTC
D1 = datetime.datetime(2026, 10, 1, 10, 0, tzinfo=UTC)
D2 = datetime.datetime(2026, 10, 2, 10, 0, tzinfo=UTC)
D3 = datetime.datetime(2026, 10, 1, 18, 30, tzinfo=UTC)


def cfg(jql):
    return Mock(jql_query=jql)


def live_journal(last_date):
    return Mock(is_a_replay_journal=False, last_date=last_date)


def replay_journal(start, end, last_date):
    return Mock(
        is_a_replay_journal=True, start_date=start, end_date=end, last_date=last_date
    )


def test_live_jql_is_unchanged():
    assert jql_for(cfg(JQL), live_journal(None)) == JQL.format(date_criteria="-30d")
    assert jql_for(cfg(JQL), live_journal(D1)) == JQL.format(
        date_criteria='"2026-10-01 10:00"'
    )


def test_replay_jql_is_bounded_by_start_and_end():
    jql = jql_for(cfg(JQL), replay_journal(D1, D2, None))
    assert jql == (
        'project = SOA AND updated > "2026-10-01 10:00" AND updated <= '
        '"2026-10-02 10:00" ORDER BY updated ASC'
    )


def test_resumed_replay_starts_at_last_date():
    jql = jql_for(cfg(JQL), replay_journal(D1, D2, D3))
    assert 'updated > "2026-10-01 18:30" AND' in jql
    assert 'updated <= "2026-10-02 10:00"' in jql


def test_resumed_replay_ignores_last_date_older_than_start():
    jql = jql_for(cfg(JQL), replay_journal(D3, D2, D1))
    assert 'updated > "2026-10-01 18:30" AND' in jql


def make_collector(names, replay_names, retry=0):
    collector = JIRAExtendedCollector.__new__(JIRAExtendedCollector)
    collector.logger = logging.getLogger("jira-test")
    collector.configs = [
        Mock(interface_name=name, end_point="http://jira", refresh_interval=0) for name in names
    ]
    collector.should_stop_loop = False
    collector._healthcheck = Mock()
    collector.replay_failures = []
    collector.disabled_interface_names = set()
    collector._flush_message_groups = Mock()
    collector.args = Mock(
        replay=ReplayArgs(
            interface_name=replay_names,
            start_date_arg=D1.isoformat(),
            end_date_arg=D2.isoformat(),
            suffix=None,
            retry=retry,
        )
    )
    return collector


def replay_args_for(collector):
    return collector.args.replay


@pytest.fixture(autouse=True)
def fake_replay_journal():
    """A replay journal that does not reach any database"""
    with patch.object(CollectorReplayJournal, "load"), patch.object(
        CollectorReplayJournal, "write"
    ), patch.object(CollectorReplayJournal, "_create_document_instance"), patch.object(
        CollectorReplayJournal, "complete"
    ) as complete:
        yield complete


def test_ingest_in_replay_mode_only_collects_the_replayed_interfaces(
    fake_replay_journal,
):
    collector = make_collector(["A", "B"], ["B"])
    seen = []

    with patch.object(
        JIRAExtendedCollector,
        "ingest_tickets",
        side_effect=lambda config, journal: seen.append((config, journal)),
    ), patch.object(CollectorReplayJournal, "__enter__", lambda self: self), patch.object(
        CollectorReplayJournal, "__exit__", lambda *a: False
    ):
        collector.ingest()

    assert [config.interface_name for config, _ in seen] == ["B"]
    assert isinstance(seen[0][1], CollectorReplayJournal)
    fake_replay_journal.assert_called_once()
    assert not collector.replay_failures


def test_unknown_replay_interface_raises_before_collecting():
    collector = make_collector(["A", "B"], ["A", "nope"])
    collector.get_configuration_by_interface_name = lambda name: next(
        (c for c in collector.configs if c.interface_name == name), None
    )
    collector.get_interface_names = lambda: ["A", "B"]

    with patch.object(JIRAExtendedCollector, "ingest_tickets") as ingest_tickets:
        with pytest.raises(ValueError, match="nope does not exists"):
            collector.ingest()

    ingest_tickets.assert_not_called()


def test_failed_jira_replay_is_listed_in_replay_failures(fake_replay_journal):
    collector = make_collector(["A", "B"], ["A", "B"])

    with patch.object(
        JIRAExtendedCollector,
        "ingest_tickets",
        side_effect=[RuntimeError("jira down"), None],
    ) as ingest_tickets, patch.object(
        CollectorReplayJournal, "__enter__", lambda self: self
    ), patch.object(
        CollectorReplayJournal, "__exit__", lambda *a: False
    ):
        collector.ingest()

    assert collector.replay_failures == ["A"]
    # one attempt per interface, the failure does not skip the next one
    assert ingest_tickets.call_count == 2
    fake_replay_journal.assert_called_once()


def test_replay_disables_the_watch_loop():
    collector = make_collector(["A"], ["A"])
    collector.args.watch_period = 60

    with patch.object(JIRAExtendedCollector, "ingest_tickets"), patch.object(
        CollectorReplayJournal, "__enter__", lambda self: self
    ), patch.object(CollectorReplayJournal, "__exit__", lambda *a: False):
        collector.ingest()

    assert collector.args.watch_period == 0


def test_replay_of_an_interface_without_end_point_raises_before_collecting():
    collector = make_collector(["A", "attachments"], ["A", "attachments"])
    collector.configs[1].end_point = ""
    collector.get_configuration_by_interface_name = lambda name: next(
        (c for c in collector.configs if c.interface_name == name), None
    )

    with patch.object(JIRAExtendedCollector, "ingest_tickets") as ingest_tickets:
        with pytest.raises(ValueError, match="attachments has no end_point"):
            collector.ingest()

    ingest_tickets.assert_not_called()


REPLAY_ARGV = [
    "--replay-interface-name",
    "A",
    "--replay-start-date",
    "2026-01-01T00:00:00Z",
    "--replay-end-date",
    "2026-01-02T00:00:00Z",
]


def test_jirax_cli_exits_1_when_a_replay_failed():
    with patch("maas_collector.rawdata.cli.jirax.JIRAExtendedCollector") as cls:
        collector = cls.return_value
        collector.replay_failures = []
        collector.run.side_effect = lambda *_: collector.replay_failures.append("A")

        with pytest.raises(SystemExit) as exit_info:
            jirax_collector_main(REPLAY_ARGV)

    assert exit_info.value.code == 1


def test_jirax_cli_returns_when_no_replay_failed():
    with patch("maas_collector.rawdata.cli.jirax.JIRAExtendedCollector") as cls:
        cls.return_value.replay_failures = []
        jirax_collector_main(REPLAY_ARGV)
