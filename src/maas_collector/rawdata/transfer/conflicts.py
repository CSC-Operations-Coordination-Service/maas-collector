"""Archive conflicts: a product seen again under the same name with other bytes."""

import datetime
import hashlib
import json

from opensearchpy.exceptions import NotFoundError

from maas_model import datetime_to_zulu

CONFLICT_INDEX = "maas-archive-conflict"
OPEN, RESOLVED = "OPEN", "RESOLVED"


def conflict_id(backup: str, interface: str, product_name: str) -> str:
    """Identifier of the conflict of a product: one per backup, interface and name"""
    return hashlib.md5(
        json.dumps([backup, interface, product_name]).encode()
    ).hexdigest()


def _current(client, identifier: str) -> dict | None:
    try:
        return client.get(index=CONFLICT_INDEX, id=identifier)["_source"]
    except NotFoundError:
        return None


def open_conflict(
    client,
    backup: str,
    interface: str,
    product: dict,
    reference: dict,
    new_checksum: str,
    now: datetime.datetime,
) -> None:
    """Record, or refresh, the open conflict of a product

    Args:
        reference: the archived report the product was compared with
        new_checksum: the checksum the catalogue (or the download) now gives
        now: the time of this sighting
    """
    identifier = conflict_id(backup, interface, product["product_name"])
    current = _current(client, identifier)
    seen = datetime_to_zulu(now)
    first_seen = current["first_seen"] if current and current.get("status") == OPEN else seen
    client.index(
        index=CONFLICT_INDEX,
        id=identifier,
        body={
            "backup_name": backup,
            "interface_name": interface,
            "product_name": product["product_name"],
            "product_id": product.get("product_id") or "",
            "document_index": product.get("document_index") or "",
            "status": OPEN,
            "archived_md5": reference.get("written_md5") or "",
            "archived_at": reference.get("end_time") or reference.get("start_time"),
            "catalogue_checksum": new_checksum,
            "publication_date": product.get("publication_date"),
            "first_seen": first_seen,
            "last_seen": seen,
        },
        refresh="wait_for",
    )


def resolve_conflict(client, backup: str, interface: str, product_name: str) -> bool:
    """Mark the open conflict of a product resolved

    Returns:
        True when an OPEN conflict became RESOLVED
    """
    identifier = conflict_id(backup, interface, product_name)
    current = _current(client, identifier)
    if not current or current.get("status") != OPEN:
        return False
    client.update(
        index=CONFLICT_INDEX,
        id=identifier,
        body={"doc": {"status": RESOLVED}},
        refresh="wait_for",
    )
    return True
