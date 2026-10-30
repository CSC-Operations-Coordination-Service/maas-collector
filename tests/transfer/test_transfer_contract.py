import json

import pytest

from maas_collector.rawdata.transfer.contract import (
    TRANSFER_EXCHANGE,
    TransferRequest,
    transfer_exchange,
    transfer_queue,
    transfer_queue_name,
    transfer_routing_key,
)

BODY = {
    "service": "svc",
    "interface_name": "ITF_A",
    "documents": [
        {"id": "d1", "index": "raw-data-a-2026"},
        {"id": "d2", "index": "raw-data-a-2025"},
    ],
    "backup": "archive-a",
    "backup_version": 3,
    "duplicates": "replace_if_changed",
    "force": True,
    "operation_id": "op-1",
    "requested_by": "alice",
    "origin": "recollect",
}


def test_names():
    assert (
        TRANSFER_EXCHANGE,
        transfer_routing_key("svc"),
        transfer_queue_name("svc"),
    ) == ("maas-transfer-exchange", "maas.transfer.svc", "maas-transfer-svc")
    assert transfer_exchange().type == "topic" and transfer_exchange().durable
    assert (transfer_queue("svc").name, transfer_queue("svc").routing_key) == (
        "maas-transfer-svc",
        "maas.transfer.svc",
    )
    assert transfer_queue("svc").durable


def test_body_round_trip():
    request = TransferRequest.from_body(json.dumps(BODY).encode())
    assert request.documents == (("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2025"))
    assert request.to_body() == BODY


def test_live_defaults():
    keys = (
        "service",
        "interface_name",
        "documents",
        "backup",
        "backup_version",
        "duplicates",
    )
    request = TransferRequest.from_body({key: BODY[key] for key in keys})
    assert (
        request.force,
        request.operation_id,
        request.requested_by,
        request.origin,
    ) == (False, "", "", "live")


def test_unknown_fields_are_ignored():
    request = TransferRequest.from_body(BODY | {"extra": 1})
    assert request.to_body() == BODY


@pytest.mark.parametrize(
    "change, field",
    [
        ({"documents": []}, "documents"),
        ({"documents": [{"id": "d1"}]}, "documents"),
        ({"service": ""}, "service"),
        ({"interface_name": 3}, "interface_name"),
        ({"backup": None}, "backup"),
        ({"backup_version": 0}, "backup_version"),
        ({"backup_version": True}, "backup_version"),
        ({"duplicates": "keep"}, "duplicates"),
        ({"force": "yes"}, "force"),
        ({"origin": "manual"}, "origin"),
    ],
)
def test_bad_body_is_refused_naming_the_field(change, field):
    with pytest.raises(ValueError, match=field):
        TransferRequest.from_body(BODY | change)


def test_body_that_is_not_an_object_is_refused():
    with pytest.raises(ValueError):
        TransferRequest.from_body(b"[1]")
