"""Tests for the destination guard and the reference integrity of ConfigStore"""

import pytest
from opensearchpy.exceptions import AuthorizationException

from backup_samples import LOCAL, ODATA, S3, backups_file, entry, models_file
from fakeopensearch import add_report_document
from maas_collector.rawdata.configprovider import (
    BackupDestinationError,
    ConfigIntegrityError,
)
from maas_collector.rawdata.configstore import ConfigStore

INDEX = "maas-collector-config"

BACKUPS = "backups/archive-a.json"

ENTRIES = "odata/entries.json"

REPORTS = "raw-data-backup-2026"


def put(store, path, content, services=("odata",), **kwargs):
    return store.publish(path, content, list(services), "alice", "test", **kwargs)


def history_length(store, path):
    return len(store.history(path))


@pytest.fixture(name="store")
def fixture_store(fake_opensearch):
    return ConfigStore(fake_opensearch, INDEX)


# --- integrity


def test_a_backup_and_an_entry_using_it_publish_in_that_order(store):
    put(store, BACKUPS, backups_file(LOCAL))
    put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})


def test_an_entry_naming_an_unknown_backup_is_refused_and_nothing_is_written(store):
    with pytest.raises(ConfigIntegrityError) as error:
        put(store, ENTRIES, {"collectors": [entry(backup="nope")]})

    assert "ITF_A" in error.value.problems[0]
    assert "nope" in error.value.problems[0]
    assert store.history(ENTRIES) == []


def test_a_backup_tagged_for_another_service_only_does_not_serve(store):
    put(store, BACKUPS, backups_file(LOCAL), services=("jirax",))

    with pytest.raises(ConfigIntegrityError):
        put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})


def test_deleting_a_backup_still_used_is_refused_even_by_a_disabled_entry(store):
    put(store, BACKUPS, backups_file(LOCAL))
    put(store, ENTRIES, {"collectors": [entry(backup="archive-a", enabled=False)]})

    with pytest.raises(ConfigIntegrityError, match="archive-a"):
        store.delete(BACKUPS, "alice", "c")

    assert history_length(store, BACKUPS) == 1


def test_untagging_a_service_that_uses_the_backup_is_refused_and_one_that_does_not_is_accepted(
    store,
):
    both = ("odata", "jirax")
    put(store, BACKUPS, backups_file(LOCAL), services=both)
    put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})

    # no entry of jirax uses it
    put(store, BACKUPS, backups_file(LOCAL), services=("odata",))

    put(store, BACKUPS, backups_file(LOCAL), services=both)
    put(
        store,
        "jirax/entries.json",
        {"collectors": [entry("ITF_J", backup="archive-a")]},
        services=("jirax",),
    )

    with pytest.raises(ConfigIntegrityError, match="service jirax"):
        put(store, BACKUPS, backups_file(LOCAL), services=("odata",))


def test_renaming_a_backup_used_by_an_entry_is_refused(store):
    put(store, BACKUPS, backups_file(LOCAL))
    put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})

    with pytest.raises(ConfigIntegrityError, match="archive-a"):
        put(store, BACKUPS, backups_file(LOCAL | {"name": "archive-z"}))


def test_rolling_back_to_a_version_without_the_backup_is_refused(store):
    put(store, BACKUPS, backups_file(LOCAL))
    put(store, BACKUPS, backups_file(S3))
    put(store, ENTRIES, {"collectors": [entry(backup="archive-b")]})

    with pytest.raises(ConfigIntegrityError, match="archive-b"):
        store.rollback(BACKUPS, 1, "alice", "back")


def test_rolling_back_to_a_tombstone_is_refused_while_the_backup_is_used(store):
    put(store, BACKUPS, backups_file(LOCAL))
    store.delete(BACKUPS, "alice", "c")
    put(store, BACKUPS, backups_file(LOCAL))
    put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})

    with pytest.raises(ConfigIntegrityError, match="archive-a"):
        store.rollback(BACKUPS, 2, "alice", "back")


def test_an_archive_rule_naming_an_unknown_backup_is_refused(store):
    with pytest.raises(ConfigIntegrityError, match="gone"):
        put(
            store,
            ENTRIES,
            {"collectors": [entry(archive={"backup": "gone"}, **{"class": ODATA})]},
        )


def test_the_backup_may_live_in_another_file_of_the_service(store):
    put(store, "odata/deep/backups/one.json", backups_file(LOCAL))
    put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})


def test_a_report_model_must_be_defined_by_the_service(store):
    with pytest.raises(ConfigIntegrityError, match="ReportM"):
        put(store, BACKUPS, backups_file(LOCAL | {"report_model": "ReportM"}))

    put(store, "odata/models.json", models_file("ReportM"))
    put(store, BACKUPS, backups_file(LOCAL | {"report_model": "ReportM"}))

    with pytest.raises(ConfigIntegrityError, match="ReportM"):
        store.delete("odata/models.json", "alice", "c")


def test_an_inline_model_of_an_entry_of_another_file_defines_the_report_model(store):
    put(store, ENTRIES, {"collectors": [entry()]})
    put(store, BACKUPS, backups_file(LOCAL | {"report_model": "ModelA"}))


def test_a_name_defined_in_two_files_of_one_service_is_refused_but_not_across_services(
    store,
):
    put(store, BACKUPS, backups_file(LOCAL))

    with pytest.raises(ConfigIntegrityError, match="both"):
        put(store, "odata/other.json", backups_file(LOCAL))

    put(store, "jirax/other.json", backups_file(LOCAL), services=("jirax",))


def test_every_write_runs_the_check_on_the_services_it_affects_only(store, monkeypatch):
    # a dangling reference already stored for service jirax
    with monkeypatch.context() as bypass:
        bypass.setattr(store, "_check_integrity", lambda *args, **kwargs: None)
        put(
            store,
            "jirax/entries.json",
            {"collectors": [entry("ITF_J", backup="archive-a")]},
            services=("jirax",),
        )

    # accepted: service jirax is not scanned
    put(store, "odata/entries.json", {"collectors": [entry("ITF_A")]})

    # a write that does affect jirax is refused
    with pytest.raises(ConfigIntegrityError, match="service jirax"):
        put(
            store,
            "jirax/other.json",
            {"collectors": [entry("ITF_K")]},
            services=("jirax",),
        )


# --- destination guard


@pytest.mark.parametrize(
    "change",
    [
        {"directory": "/data/other"},
        {"calendar_tree": True},
        {"enable_gzip": True},
        {"s3_endpoint_url": "https://other.example"},
        {"buckets": ["bucket-a"]},
    ],
)
def test_a_destination_change_is_refused_once_the_backup_has_reports(
    store, fake_opensearch, change
):
    put(store, BACKUPS, backups_file(S3))
    add_report_document(fake_opensearch, REPORTS, "archive-b")

    with pytest.raises(BackupDestinationError, match=next(iter(change))):
        put(store, BACKUPS, backups_file(S3 | change))


def test_the_same_changes_are_accepted_while_there_is_no_report(store):
    put(store, BACKUPS, backups_file(S3))
    put(store, BACKUPS, backups_file(S3 | {"directory": "other"}))
    put(store, BACKUPS, backups_file(S3 | {"calendar_tree": True}))
    put(store, BACKUPS, backups_file(S3 | {"enable_gzip": True}))
    put(store, BACKUPS, backups_file(S3 | {"s3_endpoint_url": "https://other.example"}))
    put(store, BACKUPS, backups_file(S3 | {"buckets": ["bucket-a"]}))


def test_extending_a_backup_with_reports_is_accepted(store, fake_opensearch):
    put(store, "odata/models.json", models_file("ReportM"))
    put(store, BACKUPS, backups_file(S3))
    add_report_document(fake_opensearch, REPORTS, "archive-b")

    put(store, BACKUPS, backups_file(S3 | {"description": "d"}))
    put(store, BACKUPS, backups_file(S3 | {"report_model": "ReportM"}))
    put(store, BACKUPS, backups_file(S3 | {"buckets": ["bucket-a", "bucket-b", "c"]}))
    put(
        store,
        BACKUPS,
        backups_file(
            S3
            | {"buckets": ["bucket-a", "bucket-b", "c"], "credentials": "backup-other"}
        ),
    )


def test_reports_of_another_backup_do_not_block(store, fake_opensearch):
    put(store, BACKUPS, backups_file(LOCAL))
    add_report_document(fake_opensearch, REPORTS, "archive-z")

    put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/other"}))


def test_reports_in_an_older_yearly_partition_block_a_destination_change(
    store, fake_opensearch
):
    # Review Focus 2: nothing in raw-data-backup-2026
    put(store, BACKUPS, backups_file(LOCAL))
    add_report_document(fake_opensearch, "raw-data-backup-2025", "archive-a")

    with pytest.raises(BackupDestinationError, match="directory"):
        put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/other"}))


def test_the_guard_applies_to_a_rollback(store, fake_opensearch):
    put(store, BACKUPS, backups_file(LOCAL))
    put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/other"}))
    add_report_document(fake_opensearch, REPORTS, "archive-a")

    with pytest.raises(BackupDestinationError, match="directory"):
        store.rollback(BACKUPS, 1, "alice", "back")


def test_delete_then_recreate_with_another_destination_is_refused_and_with_the_same_is_accepted(
    store, fake_opensearch
):
    put(store, BACKUPS, backups_file(LOCAL))
    add_report_document(fake_opensearch, REPORTS, "archive-a")
    store.delete(BACKUPS, "alice", "c")

    with pytest.raises(BackupDestinationError, match="directory"):
        put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/other"}))

    put(store, BACKUPS, backups_file(LOCAL))


def test_a_name_with_reports_cannot_move_to_another_path(store, fake_opensearch):
    put(store, BACKUPS, backups_file(LOCAL))
    add_report_document(fake_opensearch, REPORTS, "archive-a")
    store.delete(BACKUPS, "alice", "c")

    with pytest.raises(BackupDestinationError, match="already has reports") as error:
        put(
            store,
            "backups/archive-a2.json",
            backups_file(LOCAL | {"directory": "/data/other"}),
        )

    assert "archive-a" in str(error.value)
    assert store.history("backups/archive-a2.json") == []


def test_a_new_name_is_accepted_without_reports_and_blocked_by_reports_of_another_service(
    store, fake_opensearch
):
    put(store, "backups/archive-c.json", backups_file(LOCAL | {"name": "archive-c"}))

    # written by a backup of service jirax
    add_report_document(fake_opensearch, REPORTS, "archive-d")

    with pytest.raises(BackupDestinationError, match="already has reports"):
        put(
            store,
            "backups/archive-d.json",
            backups_file(LOCAL | {"name": "archive-d"}),
        )


def test_a_missing_report_index_means_no_reports(store, fake_opensearch):
    put(store, BACKUPS, backups_file(LOCAL))

    assert not any(
        name.startswith("raw-data-backup") for name in fake_opensearch.documents
    )

    put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/other"}))


def test_an_unreadable_report_index_refuses_the_change(
    store, fake_opensearch, monkeypatch
):
    put(store, BACKUPS, backups_file(S3))
    real_search = fake_opensearch.search

    def search(index=None, **kwargs):
        if str(index).startswith("raw-data-backup"):
            raise AuthorizationException(403, "forbidden")
        return real_search(index=index, **kwargs)

    monkeypatch.setattr(fake_opensearch, "search", search)

    with pytest.raises(AuthorizationException):
        put(store, BACKUPS, backups_file(S3 | {"directory": "other"}))


def test_the_report_index_is_configurable(fake_opensearch):
    store = ConfigStore(fake_opensearch, INDEX, report_index="custom-reports")
    put(store, BACKUPS, backups_file(LOCAL))

    # the default index is not read
    add_report_document(fake_opensearch, REPORTS, "archive-a")
    put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/other"}))

    add_report_document(fake_opensearch, "custom-reports-2026", "archive-a")
    with pytest.raises(BackupDestinationError):
        put(store, BACKUPS, backups_file(LOCAL | {"directory": "/data/third"}))


def test_ordinary_writes_do_not_query_the_report_index(
    store, fake_opensearch, monkeypatch
):
    put(store, BACKUPS, backups_file(LOCAL))
    queried = []
    real_search = fake_opensearch.search

    def search(index=None, **kwargs):
        queried.append(str(index))
        return real_search(index=index, **kwargs)

    monkeypatch.setattr(fake_opensearch, "search", search)

    put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})
    put(store, BACKUPS, backups_file(LOCAL | {"description": "d"}))

    assert queried
    assert not [name for name in queried if name.startswith("raw-data-backup")]
