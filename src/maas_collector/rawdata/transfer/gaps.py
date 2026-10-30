"""Gaps of the archive: products indexed a while ago that no archive report covers."""

import datetime

from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import scan

from maas_model import datetime_to_zulu

from maas_collector.rawdata.configprovider import (
    DEFAULT_CONFIG_INDEX,
    current_documents,
)
from maas_collector.rawdata.transfer.reports import (
    ARCHIVE_ORIGIN,
    OUTCOMES,
    REPORT_INDEX,
)
from maas_collector.rawdata.transfer.rule import DEFAULT_FIELDS, ArchiveRule

NAME_CHUNK_SIZE = 1024
"""product names per terms query"""


def named_model(client, name: str, config_index: str) -> dict:
    """The definition of a model name among the current, non-deleted documents of the
    configuration index: a `models` item, or the inline model of an entry

    Raises:
        ValueError: unknown, or defined with two indexes
    """
    try:
        documents = current_documents(client, config_index)
    except NotFoundError:
        documents = []

    found = []
    for document in documents:
        if document.get("deleted"):
            continue
        content = document.get("content") or {}
        candidates = list(content.get("models") or [])
        candidates.extend(
            meta["model"]
            for meta in content.get("collectors") or []
            if isinstance(meta, dict) and isinstance(meta.get("model"), dict)
        )
        found.extend(
            model
            for model in candidates
            if isinstance(model, dict) and model.get("name") == name
        )

    if not found:
        raise ValueError(f"model {name} is defined by no current configuration file")
    if len({model.get("index") for model in found}) > 1:
        raise ValueError(f"model {name} is defined with two indexes")
    return found[0]


def raw_model(client, entry: dict, config_index: str) -> dict:
    """The model of the raw documents of an entry

    Raises:
        ValueError: no model, an unknown one, or one without an `interface_name` field:
            the products of that interface could not be told apart, and the gaps would
            be missing
    """
    model = entry.get("model")
    if isinstance(model, str):
        model = named_model(client, model, config_index)
    if not isinstance(model, dict) or not model.get("index"):
        raise ValueError(f"{entry.get('interface_name')}: the entry has no model with an index")
    names = {field.get("name") for field in model.get("fields") or []}
    if "interface_name" not in names:
        raise ValueError(
            f"{entry.get('interface_name')}: model {model.get('name')} has no"
            " interface_name field, the products of the interface cannot be told"
            " apart: declare it and make the extractor fill it"
        )
    return model


def outcomes(client, backup: str, interface: str, names: list[str]) -> set[str]:
    """The names that have an outcome report in a backup"""
    reported = set()
    for start in range(0, len(names), NAME_CHUNK_SIZE):
        query = {
            "bool": {
                "filter": [
                    {"term": {"origin": ARCHIVE_ORIGIN}},
                    {"term": {"backup_name": backup}},
                    {"term": {"interface_name": interface}},
                    {"terms": {"status": list(OUTCOMES)}},
                    {"terms": {"product_name": names[start : start + NAME_CHUNK_SIZE]}},
                ]
            }
        }
        try:
            reported.update(
                hit["_source"]["product_name"]
                for hit in scan(client, index=REPORT_INDEX, query={"query": query})
            )
        except NotFoundError:
            return reported
    return reported


# pylint: disable-next=too-many-arguments,too-many-locals
def find_gaps(
    client,
    entry: dict,
    backup: str,
    start: datetime.datetime,
    end: datetime.datetime,
    delay: datetime.timedelta,
    *,
    now: datetime.datetime | None = None,
    config_index: str = DEFAULT_CONFIG_INDEX,
) -> list[dict]:
    """The products of an archived interface that no archive report covers

    A gap is a product indexed more than ``delay`` ago, within the window, with no
    archive report in its backup whose status is an outcome (`OK`, `REPLACED`, `KO`,
    `SKIPPED`): a `REQUESTED` report alone leaves a gap.

    Args:
        client: opensearch-py client
        entry: the collector entry of the interface
        backup: the backup archived into
        start, end: window on the ingestion time of the raw documents
        delay: documents indexed since less than this are not yet gaps
        now: the time, now by default
        config_index: the configuration index, to resolve a model name

    Raises:
        ValueError: see ``raw_model``

    Returns:
        [{"product_id", "product_name", "document_index", "indexed_at"}], sorted by
        indexed_at
    """
    now = now or datetime.datetime.now(tz=datetime.UTC)
    interface = entry["interface_name"]
    model = raw_model(client, entry, config_index)
    rule = ArchiveRule.from_entry(entry)
    fields = rule.fields if rule else DEFAULT_FIELDS

    upper = min(end, now - delay)
    query = {
        "bool": {
            "filter": [
                {"term": {"interface_name": interface}},
                {
                    "range": {
                        "ingestionTime": {
                            "gte": datetime_to_zulu(start),
                            "lt": datetime_to_zulu(upper),
                        }
                    }
                },
            ]
        }
    }
    try:
        hits = list(scan(client, index=model["index"], query={"query": query}))
    except NotFoundError:
        return []

    products = []
    for hit in hits:
        source = hit["_source"]
        name = source.get(fields["product_name"])
        # a document without a name cannot be archived, nor matched to a report
        if not name:
            continue
        products.append(
            {
                "product_id": source.get(fields["product_id"]),
                "product_name": name,
                "document_index": hit["_index"],
                "indexed_at": source.get("ingestionTime"),
            }
        )

    reported = outcomes(
        client, backup, interface, sorted({product["product_name"] for product in products})
    )
    gaps = [product for product in products if product["product_name"] not in reported]
    return sorted(gaps, key=lambda gap: gap["indexed_at"] or "")
