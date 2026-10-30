"""Transfer requests: what the collectors ask the transfer stage to archive.

Names are shared with the operator backend (cross-repo).
"""

from dataclasses import dataclass

import kombu

from maas_collector.rawdata.operator.contract import _load
from maas_collector.rawdata.operator.overrides import DUPLICATES

TRANSFER_EXCHANGE = "maas-transfer-exchange"
ORIGINS = ("live", "recollect", "download")


def transfer_routing_key(service: str) -> str:
    """Routing key of the transfer requests of a service"""
    return f"maas.transfer.{service}"


def transfer_queue_name(service: str) -> str:
    """Queue name of the transfer requests of a service"""
    return f"maas-transfer-{service}"


def transfer_exchange() -> kombu.Exchange:
    """Transfer topic exchange"""
    return kombu.Exchange(TRANSFER_EXCHANGE, type="topic", durable=True)


def transfer_queue(service: str) -> kombu.Queue:
    """Durable queue of the transfer stage of a service"""
    return kombu.Queue(
        transfer_queue_name(service),
        exchange=transfer_exchange(),
        routing_key=transfer_routing_key(service),
        durable=True,
    )


def _text(data: dict, name: str, default: str | None = None) -> str:
    value = data.get(name, default)
    if not isinstance(value, str) or (not value and default is None):
        raise ValueError(f"missing or invalid field: {name}")
    return value


def _documents(data: dict) -> tuple[tuple[str, str], ...]:
    value = data.get("documents")
    if not isinstance(value, list) or not value:
        raise ValueError("missing or invalid field: documents")
    documents = []
    for item in value:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("id"), str)
            or not item["id"]
            or not isinstance(item.get("index"), str)
            or not item["index"]
        ):
            raise ValueError("invalid field: documents (each needs an id and an index)")
        documents.append((item["id"], item["index"]))
    return tuple(documents)


@dataclass(frozen=True)
class TransferRequest:
    """Request to archive documents (several products) into a named backup"""

    service: str
    interface_name: str
    documents: tuple[tuple[str, str], ...]  # (document id, index)
    backup: str
    backup_version: int | None
    duplicates: str  # "skip" | "replace_if_changed"
    force: bool = False
    operation_id: str = ""
    requested_by: str = ""
    origin: str = "live"  # "live" | "recollect" | "download"

    def to_body(self) -> dict:
        """JSON body of the request"""
        return {
            "service": self.service,
            "interface_name": self.interface_name,
            "documents": [
                {"id": document_id, "index": index}
                for document_id, index in self.documents
            ],
            "backup": self.backup,
            "backup_version": self.backup_version,
            "duplicates": self.duplicates,
            "force": self.force,
            "operation_id": self.operation_id,
            "requested_by": self.requested_by,
            "origin": self.origin,
        }

    @classmethod
    def from_body(cls, body) -> "TransferRequest":
        """Read a request from a message body

        Raises:
            ValueError: the body is not a valid request, naming the field
        """
        data = _load(body)
        version = data.get("backup_version")
        if version is not None and (
            isinstance(version, bool) or not isinstance(version, int) or version < 1
        ):
            raise ValueError("invalid field: backup_version")
        duplicates = data.get("duplicates")
        if duplicates not in DUPLICATES:
            raise ValueError(f"invalid field: duplicates (one of {DUPLICATES})")
        force = data.get("force", False)
        if not isinstance(force, bool):
            raise ValueError("invalid field: force")
        origin = data.get("origin", "live")
        if origin not in ORIGINS:
            raise ValueError(f"invalid field: origin (one of {ORIGINS})")
        return cls(
            service=_text(data, "service"),
            interface_name=_text(data, "interface_name"),
            documents=_documents(data),
            backup=_text(data, "backup"),
            backup_version=version,
            duplicates=duplicates,
            force=force,
            operation_id=_text(data, "operation_id", ""),
            requested_by=_text(data, "requested_by", ""),
            origin=origin,
        )
