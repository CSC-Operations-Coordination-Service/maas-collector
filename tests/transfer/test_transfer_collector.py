import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from maas_collector.rawdata.cli.odata import odata_collector_main
from maas_collector.rawdata.collector.filecollector import (
    BACKUP_FAILED_EXIT_CODE,
    CONFIG_CHANGED_EXIT_CODE,
    REPLAY_FAILED_EXIT_CODE,
    TRANSFER_FAILED_EXIT_CODE,
    CollectorArgs,
    FileCollector,
)
from maas_collector.rawdata.collector.odatacollector import (
    ODataCollector,
    ODataConfiguration,
)
from maas_collector.rawdata.transfer.rule import ArchiveRule

REPLAY_ARGV = [
    "--replay-interface-name",
    "ITF_A",
    "--replay-start-date",
    "2026-01-01T00:00:00Z",
    "--replay-end-date",
    "2026-01-02T00:00:00Z",
]

CONFIG = SimpleNamespace(
    model=SimpleNamespace(__name__="ProductA"),
    archive_rule=ArchiveRule.default("archive-a"),
    backup=None,
    routing_key="k",
    interface_name="ITF_A",
)


class StubIterator:
    """what ActionIterator exposes after a run"""

    def __init__(self, *args, **kwargs):
        self.unmodified_ids = ["d3"]
        self.unmodified_documents = [("d3", "i0")]
        self.document_count = 2
        self.action_iterator_errors = []

    def __iter__(self):
        return iter(())


def bulk_results():
    return [
        (True, {"index": {"_id": "d1", "_index": "i1", "result": "created"}}),
        (True, {"index": {"_id": "d2", "_index": "i1", "result": "updated"}}),
    ]


@pytest.fixture(name="collector")
def fixture_collector():
    collector = FileCollector(CollectorArgs())
    collector._healthcheck = Mock()
    collector._messenger = Mock()
    collector._transfer_requester = Mock()
    with patch(
        "maas_collector.rawdata.collector.filecollector.es_connections"
    ), patch(
        "maas_collector.rawdata.collector.filecollector.ActionIterator", StubIterator
    ), patch(
        "maas_collector.rawdata.collector.filecollector.parallel_bulk",
        return_value=bulk_results(),
    ):
        yield collector


def test_created_updated_and_unchanged_documents_go_to_the_requester(collector):
    collector.extract_from_file("/tmp/page.json", CONFIG)
    collector._transfer_requester.documents_indexed.assert_called_once_with(
        CONFIG, [("d1", "i1"), ("d2", "i1")], [("d3", "i0")]
    )


def test_entry_without_rule_builds_no_requester(collector):
    collector._transfer_requester = None
    collector.extract_from_file(
        "/tmp/page.json", SimpleNamespace(**vars(CONFIG) | {"archive_rule": None})
    )
    assert collector._transfer_requester is None


def test_unchanged_documents_get_no_product_message(collector):
    collector.extract_from_file("/tmp/page.json", CONFIG)
    assert [
        call.args[1] for call in collector._messenger.handle_message.call_args_list
    ] == ["d1", "d2"]


def test_the_requester_is_built_on_first_use():
    collector = FileCollector(CollectorArgs(replay=None))
    assert collector.transfer_failures == 0
    assert collector._transfer_requester is None
    requester = collector.transfer_requester
    assert collector.transfer_requester is requester
    assert (requester.service, requester.replay) == ("", None)


def write_conf(tmp_path):
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "odata.json").write_text(
        json.dumps(
            {
                "models": [
                    {
                        "name": "ProductA",
                        "index": "raw-data-a",
                        "fields": [{"name": "product_name", "type": "Keyword"}],
                    }
                ],
                "backups": [
                    {"name": "archive-a", "type": "BackupLocal", "directory": "/archive"}
                ],
                "collectors": [
                    {
                        "class": "ODataCollectorConfiguration",
                        "interface_name": "ITF_A",
                        "id_field": "product_name",
                        "routing_key": "",
                        "model": "ProductA",
                        "odata_product_url": "https://catalogue.example",
                        "archive": {"backup": "archive-a"},
                    }
                ],
            }
        ),
        encoding="UTF-8",
    )
    return conf


def test_archive_rule_without_service_fails_at_load(tmp_path):
    conf = write_conf(tmp_path)
    collector = ODataCollector(
        CollectorArgs(rawdata_config_dir=str(conf)), ODataConfiguration()
    )
    with pytest.raises(ValueError, match="CONFIG_SERVICE"):
        collector.load_config()


def test_archive_rule_with_a_service_loads(tmp_path):
    from maas_collector.rawdata.configprovider import ConfigStoreArgs

    conf = write_conf(tmp_path)
    collector = ODataCollector(
        CollectorArgs(
            rawdata_config_dir=str(conf), config_store=ConfigStoreArgs(service="svc")
        ),
        ODataConfiguration(),
    )
    collector.load_config()
    assert collector.transfer_service == "svc"
    assert collector.configs[0].archive_rule.backup == "archive-a"


def test_each_loop_retries_the_pending_requests_then_closes(collector):
    collector.args.watch_period = 0
    with patch.object(collector, "start_healthcheck"), patch.object(
        collector, "stop_healthcheck"
    ), patch.object(collector, "loop_body"), patch.object(
        collector, "refresh_credentials"
    ), patch.object(
        collector, "check_config_changes"
    ):
        collector.run(["x"])
    collector._transfer_requester.retry_pending.assert_called_once()
    collector._transfer_requester.close.assert_called()


def run_cli(capsys, *, replay=(), transfers=0, backups=None):
    with patch("maas_collector.rawdata.cli.odata.ODataCollector") as klass:
        collector = klass.return_value
        collector.replay_failures = list(replay)
        collector.backup_failures = backups or {}
        collector.transfer_failures = transfers
        collector.backup_failure_line.return_value = (
            "backup failed: 2 files to archive-a"
        )
        with pytest.raises(SystemExit) as exit_info:
            odata_collector_main(REPLAY_ARGV)
    return exit_info.value.code, capsys.readouterr().out.strip().splitlines()


def test_odata_cli_exits_on_transfer_failures(capsys):
    code, lines = run_cli(capsys, transfers=3)
    assert code == TRANSFER_FAILED_EXIT_CODE
    assert lines[-1] == "transfer requests failed: 3 documents"


def test_a_failed_replay_keeps_exit_code_1(capsys):
    code, lines = run_cli(capsys, replay=["ITF_A"], transfers=3, backups={"archive-a": 2})
    assert code == REPLAY_FAILED_EXIT_CODE
    assert lines[-2:] == [
        "backup failed: 2 files to archive-a",
        "transfer requests failed: 3 documents",
    ]


def test_transfer_and_backup_failures_print_both_lines_and_the_transfer_code_wins(capsys):
    code, lines = run_cli(capsys, transfers=3, backups={"archive-a": 2})
    assert code == TRANSFER_FAILED_EXIT_CODE
    assert lines[-2:] == [
        "backup failed: 2 files to archive-a",
        "transfer requests failed: 3 documents",
    ]


def test_backup_failure_alone_keeps_plan_1_code(capsys):
    code, lines = run_cli(capsys, backups={"archive-a": 2})
    assert code == BACKUP_FAILED_EXIT_CODE
    assert lines[-1] == "backup failed: 2 files to archive-a"


def test_exit_codes_are_distinct():
    assert (
        len(
            {
                CONFIG_CHANGED_EXIT_CODE,
                REPLAY_FAILED_EXIT_CODE,
                BACKUP_FAILED_EXIT_CODE,
                TRANSFER_FAILED_EXIT_CODE,
            }
        )
        == 4
    )
    assert TRANSFER_FAILED_EXIT_CODE >= 69
