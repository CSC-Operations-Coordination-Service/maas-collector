"""Archive reports: the outcome of every transfer of the archive.

They are backup reports (``BackupReport``) with the origin ``archive``, stored in the
index of the backup reports.
"""

import dataclasses
import datetime
import logging
from typing import Sequence

from opensearchpy.exceptions import NotFoundError

from maas_collector.backup.backup import (
    KO,
    OK,
    REPLACED,
    REQUESTED,
    SKIPPED,
    BackupReport,
)
from maas_collector.rawdata.backup_report import BackupReportWriter
from maas_collector.rawdata.configuration import build_model_class
from maas_collector.rawdata.transfer.contract import TransferRequest
from maas_collector.rawdata.transfer.rule import ArchiveRule, product_of

LOGGER = logging.getLogger(__name__)

REPORT_INDEX = "raw-data-backup"
ARCHIVE_ORIGIN = "archive"
OUTCOMES = (OK, REPLACED, KO, SKIPPED)
CONFLICT_ERROR = "ArchiveConflict"
"""error_type of a SKIPPED report that is a conflict"""

STATE_SIZE = 100


class ArchiveReportError(RuntimeError):
    """Archive reports could not be stored"""


_MODEL = None


def _field_type(field: dataclasses.Field) -> str:
    if field.name == "details":
        return "Text"
    if field.type in (int, "int"):
        return "Long"
    if field.type in (float, "float"):
        return "Float"
    if field.type in (bool, "bool"):
        return "Boolean"
    if field.name in ("start_time", "end_time"):
        return "Date"
    return "Keyword"


def report_model() -> type:
    """The model storing the reports, built once: the index of the backup reports,
    partitioned by year of ingestion, one field per field of ``BackupReport``"""
    global _MODEL  # pylint: disable=global-statement
    if _MODEL is None:
        _MODEL = build_model_class(
            {
                "name": "ArchiveBackupReport",
                "index": REPORT_INDEX,
                "fields": [
                    {"name": field.name, "type": _field_type(field)}
                    for field in dataclasses.fields(BackupReport)
                ],
                "partition_field": "ingestionTime",
                "partition_format": "%Y",
            },
            register=False,
        )
    return _MODEL


def write_reports(reports: list[BackupReport]) -> None:
    """Store reports in one bulk create, visible to the next search

    Raises:
        ArchiveReportError: a report is not stored
    """
    if not reports:
        return
    try:
        _stored, errors = BackupReportWriter(report_model()).write(
            reports, refresh="wait_for"
        )
    except Exception as error:  # pylint: disable=broad-except
        raise ArchiveReportError(f"cannot store the archive reports: {error}") from error
    if errors:
        raise ArchiveReportError(
            f"{len(errors)} of {len(reports)} archive reports were not stored: {errors}"
        )


def archive_fields(request: TransferRequest, product: dict) -> dict:
    """The fields every archive report of a product carries"""
    return {
        "origin": ARCHIVE_ORIGIN,
        "backup_name": request.backup,
        "operation_id": request.operation_id,
        "interface_name": request.interface_name,
        "product_id": product.get("product_id") or "",
        "product_name": product.get("product_name") or "",
        "catalogue_checksum": product.get("checksum") or "",
    }


def archive_report(
    request: TransferRequest, product: dict, status: str, **fields
) -> BackupReport:
    """A report of a product, started and ended now"""
    now = datetime.datetime.now(tz=datetime.UTC)
    # a product that is not in the index has no name: its document names the report,
    # so that two such reports never have the same identifier
    name = product.get("product_name") or product.get("document_id") or ""
    values = {
        **archive_fields(request, product),
        "filename": name,
        "source_filename": name,
        "start_time": now,
        "end_time": now,
        "status": status,
        **fields,
    }
    return BackupReport(**values)


def requested_reports(
    request: TransferRequest, products: list[dict]
) -> list[BackupReport]:
    """One REQUESTED report per product, for the operation to know what it sent"""
    return [
        archive_report(request, product, REQUESTED, details=product.get("details", ""))
        for product in products
    ]


def missing_product(document: tuple[str, str]) -> dict:
    """The stand-in of a document that is not in the index"""
    document_id, document_index = document
    return {
        "document_id": document_id,
        "document_index": document_index,
        "product_id": "",
        "product_name": "",
        "size": None,
        "checksum": None,
        "publication_date": None,
        "details": f"document {document_id} not found in {document_index}",
    }


def unreadable_product(document: tuple[str, str], error_kind: str) -> dict:
    """The stand-in of a document the index refuses to give, named by the class of
    the error, never by its message"""
    document_id, document_index = document
    return missing_product(document) | {
        "details": f"document {document_id} cannot be read from "
        f"{document_index} ({error_kind})"
    }


def archived_state(
    client, backup: str, interface: str, product_name: str
) -> dict[str, dict]:
    """The last archive report of a product for each destination of a backup

    Only the outcomes that tell whether the object is there count (OK, REPLACED,
    KO): the caller treats a destination whose last report is not OK or REPLACED as
    missing.

    Returns:
        destination -> report source, {} when there is no report index
    """
    query = {
        "bool": {
            "filter": [
                {"term": {"origin": ARCHIVE_ORIGIN}},
                {"term": {"backup_name": backup}},
                {"term": {"interface_name": interface}},
                {"term": {"product_name": product_name}},
                {"terms": {"status": [OK, REPLACED, KO]}},
            ]
        }
    }
    try:
        response = client.search(
            index=REPORT_INDEX,
            body={
                "query": query,
                "sort": [{"start_time": {"order": "desc"}}],
                "size": STATE_SIZE,
            },
        )
    except NotFoundError:
        return {}
    state: dict[str, dict] = {}
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        state.setdefault(source.get("destination", ""), source)
    return state


def read_products(
    client, documents: Sequence[tuple[str, str]], rule: ArchiveRule
) -> tuple[list[dict], list[tuple[str, str]]]:
    """Read the products the documents of a request describe

    Returns:
        (the products found, in order; the (id, index) of the documents not found)
    """
    products, missing = [], []
    for document_id, index in documents:
        try:
            source = client.get(index=index, id=document_id)["_source"]
        except NotFoundError:
            missing.append((document_id, index))
            continue
        products.append(product_of(source, rule, document_id, index))
    return products, missing

