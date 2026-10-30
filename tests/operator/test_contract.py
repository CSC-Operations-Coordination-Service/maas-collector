import datetime
import json

import pytest

from maas_collector.rawdata.operator import contract as c

UTC = datetime.UTC
T0 = datetime.datetime(2026, 10, 16, 10, 0, tzinfo=UTC)


def replay(**kw):
    base = {
        "operation_id": "op1",
        "interface_name": "odata",
        "start_date": "2026-10-16T10:00:00Z",
        "end_date": "2026-10-17T10:00:00+00:00",
    }
    base.update(kw)
    return base


def test_names():
    assert c.replay_routing_key("odata", "long") == "maas.operator.odata.replay.long"
    assert c.replay_queue_name("odata", "short") == "maas-operator-odata-replay-short"
    assert c.check_routing_key("odata") == "maas.operator.odata.check"
    assert c.check_queue_name("odata") == "maas-operator-odata-check"
    ex = c.exchange()
    assert (ex.name, ex.type, ex.durable) == ("maas-operator-exchange", "topic", True)


def test_lane_boundary():
    day = datetime.timedelta(hours=24)
    assert c.lane_for(T0, T0 + day) == "short"
    assert c.lane_for(T0, T0 + day + datetime.timedelta(seconds=1)) == "long"


def test_replay_round_trip_and_forms():
    r = c.ReplayRequest.from_body(replay(requested_by="me", lane="long"))
    assert r.start_date == T0 and r.end_date == T0 + datetime.timedelta(days=1)
    body = r.to_body()
    assert body["start_date"] == "2026-10-16T10:00:00Z"
    assert c.ReplayRequest.from_body(body) == r
    assert c.ReplayRequest.from_body(json.dumps(replay())).lane == "short"
    assert c.ReplayRequest.from_body(json.dumps(replay()).encode()).requested_by == ""


def test_replay_naive_is_utc_and_offset_converted():
    r = c.ReplayRequest.from_body(
        replay(start_date="2026-10-16T10:00:00", end_date="2026-10-16T14:00:00+02:00")
    )
    assert r.start_date == T0
    assert r.end_date == datetime.datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    assert r.end_date.utcoffset() == datetime.timedelta(0)


@pytest.mark.parametrize(
    "change",
    [
        {"operation_id": None},
        {"interface_name": ""},
        {"start_date": None},
        {"start_date": "yesterday"},
        {"end_date": "2026-10-16T10:00:00Z"},
        {"end_date": "2026-10-15T10:00:00Z"},
        {"lane": "medium"},
    ],
)
def test_replay_errors(change):
    body = replay(**change)
    body = {k: v for k, v in body.items() if v is not None}
    with pytest.raises(ValueError):
        c.ReplayRequest.from_body(body)


def test_bad_bodies():
    for bad in ("not json", "[1]", b"\xff"):
        with pytest.raises(ValueError):
            c.ReplayRequest.from_body(bad)


def test_check_request():
    r = c.CheckRequest.from_body(
        {"operation_id": "o", "interface_name": "i", "product_id": "p"}
    )
    assert (r.product_id, r.product_name) == ("p", "")
    assert c.CheckRequest.from_body(r.to_body()) == r
    base = {"operation_id": "o", "interface_name": "i"}
    for extra in ({}, {"product_id": "p", "product_name": "n"}):
        with pytest.raises(ValueError):
            c.CheckRequest.from_body({**base, **extra})
    with pytest.raises(ValueError):
        c.CheckRequest.from_body({"interface_name": "i", "product_id": "p"})


def test_check_reply_body():
    reply = c.CheckReply(status="ok", diff=[{"field": "a", "remote": 1, "stored": 2}])
    assert reply.to_body() == {
        "status": "ok",
        "remote": None,
        "stored": None,
        "diff": [{"field": "a", "remote": 1, "stored": 2}],
        "error": None,
    }


BASE = {
    "operation_id": "op1",
    "interface_name": "ITF_A",
    "start_date": T0,
    "end_date": T0 + datetime.timedelta(days=1),
    "requested_by": "bob",
}


def test_custom_names():
    assert c.CUSTOM_SUFFIX == "@custom"
    assert (
        c.replay_custom_routing_key("odata", "long")
        == "maas.operator.odata.replay-custom.long"
    )
    assert (
        c.replay_custom_queue_name("odata", "short")
        == "maas-operator-odata-replay-custom-short"
    )


def test_a_plain_request_is_unchanged():
    r = c.ReplayRequest.from_body(replay())
    assert (r.is_custom, r.overrides, r.configuration, r.archive) == (
        False,
        None,
        "",
        None,
    )
    assert r.to_body() == c.ReplayRequest.from_body(r.to_body()).to_body()
    assert set(r.to_body()) == {
        "operation_id",
        "interface_name",
        "start_date",
        "end_date",
        "requested_by",
        "lane",
    }
    assert r.routing_key("odata") == "maas.operator.odata.replay.short"


@pytest.mark.parametrize(
    "fields",
    [
        {"overrides": {"jql_query": "x"}},
        {"overrides": {}},
        {"configuration": "named@2"},
        {
            "archive": {
                "backup": "archive-a",
                "backup_version": 3,
                "duplicates": "skip",
                "force": False,
            }
        },
    ],
)
def test_a_custom_request_round_trips_with_the_suffixed_name(fields):
    r = c.ReplayRequest(**{**BASE, "lane": "long"}, **fields)
    body = r.to_body()
    assert (r.is_custom, body["interface_name"], body["interface"]) == (
        True,
        "ITF_A@custom",
        "ITF_A",
    )
    assert c.ReplayRequest.from_body(body) == r
    assert r.routing_key("odata") == "maas.operator.odata.replay-custom.long"


@pytest.mark.parametrize(
    "change",
    [
        {"interface_name": "ITF_A@custom"},
        {"overrides": {"a": 1}},
        {"overrides": {}},
        {"interface_name": "ITF_A@custom", "interface": "ITF_A", "configuration": ""},
        {
            "interface_name": "ITF_A@custom",
            "interface": "OTHER",
            "overrides": {"a": 1},
        },
        {
            "interface_name": "A@custom@custom",
            "interface": "A@custom",
            "overrides": {"a": 1},
        },
        {"interface_name": "ITF_A@custom", "interface": "ITF_A", "overrides": [1]},
        {
            "interface_name": "ITF_A@custom",
            "interface": "ITF_A",
            "archive": "archive-a",
        },
        {"interface_name": "ITF_A@custom", "interface": "ITF_A", "configuration": 3},
    ],
)
def test_inconsistent_custom_bodies_are_refused(change):
    with pytest.raises(ValueError):
        c.ReplayRequest.from_body(replay(**change))
