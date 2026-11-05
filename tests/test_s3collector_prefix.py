"""Prefix listing and move-after-ingest helpers of the S3 collector"""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from maas_collector.rawdata.collector.s3collector import (
    S3CollectorConfiguration,
    build_list_kwargs,
    move_object,
    moved_key,
)

NOW = datetime.datetime(2026, 10, 28, 6, 0, tzinfo=datetime.UTC)


def make_config(**kwargs):
    """The helpers only read attributes: take the defaults of the real class"""
    defaults = {
        name: field.default
        for name, field in S3CollectorConfiguration.__dataclass_fields__.items()
        if name in ("s3_prefix", "s3_move_to")
    }
    return SimpleNamespace(**{**defaults, "s3_max_keys": 50, **kwargs})


def test_list_kwargs_default_lists_the_whole_bucket():
    assert build_list_kwargs(make_config(), "b") == {"Bucket": "b", "MaxKeys": 50}


def test_list_kwargs_with_prefix_and_start_after():
    config = make_config(s3_prefix="to_ingest/")
    assert build_list_kwargs(config, "b", "to_ingest/a.tgz") == {
        "Bucket": "b",
        "MaxKeys": 50,
        "Prefix": "to_ingest/",
        "StartAfter": "to_ingest/a.tgz",
    }


def test_moved_key_none_without_move_to():
    assert moved_key(make_config(), "a.tgz", NOW) is None


def test_moved_key_replaces_date_and_keeps_subfolder():
    config = make_config(s3_prefix="to_ingest/", s3_move_to="ingested/{date}/")
    assert (
        moved_key(config, "to_ingest/AUXIP/a.tgz", NOW)
        == "ingested/2026-10-28/AUXIP/a.tgz"
    )


def test_moved_key_key_outside_prefix_is_kept_whole():
    config = make_config(s3_prefix="to_ingest/", s3_move_to="ingested/{date}/")
    assert moved_key(config, "other/a.tgz", NOW) == "ingested/2026-10-28/other/a.tgz"


def test_move_object_copies_then_deletes():
    client = MagicMock()
    move_object(client, "b", "to_ingest/a.tgz", "ingested/2026-10-28/a.tgz")
    assert [call[0] for call in client.method_calls] == ["copy_object", "delete_object"]
    client.copy_object.assert_called_once_with(
        Bucket="b",
        Key="ingested/2026-10-28/a.tgz",
        CopySource={"Bucket": "b", "Key": "to_ingest/a.tgz"},
    )
    client.delete_object.assert_called_once_with(Bucket="b", Key="to_ingest/a.tgz")


def test_move_object_keeps_the_source_when_the_copy_fails():
    client = MagicMock()
    client.copy_object.side_effect = RuntimeError("boom")
    try:
        move_object(client, "b", "k", "d")
    except RuntimeError:
        pass
    client.delete_object.assert_not_called()
