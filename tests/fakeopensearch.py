"""
In-memory stand-in for the opensearch-py client, for the configuration tests.

It covers what the configuration provider, the configuration store and the admin
CLI call: indices.exists / create, index (op_type=create answers 409 on an existing
id), get, update, search with a bool query of term filters, sort and size, and the
scroll calls of opensearchpy.helpers.scan. An index created with a "dynamic":
"strict" mapping refuses unmapped top-level fields, like OpenSearch does.
"""

import copy
import itertools

from opensearchpy.exceptions import ConflictError, NotFoundError, RequestError

# OpenSearch answers 10 hits when a search sets no size
DEFAULT_SIZE = 10


class FakeIndices:
    """client.indices"""

    def __init__(self, client: "FakeOpenSearch"):
        self.client = client

    def exists(self, index: str, **_kwargs) -> bool:
        """True if the index exists"""
        self.client.check_available()
        return index in self.client.documents

    def create(self, index: str, body: dict = None, **_kwargs) -> dict:
        """create an index, with its mapping"""
        self.client.check_available()
        if index in self.client.documents:
            raise RequestError(400, "resource_already_exists_exception", index)
        self.client.documents[index] = {}
        self.client.mappings[index] = copy.deepcopy((body or {}).get("mappings", {}))
        return {"acknowledged": True, "index": index}


class FakeOpenSearch:
    """opensearch-py client keeping its documents in dictionaries"""

    def __init__(self):
        # index name -> document id -> source
        self.documents: dict = {}

        # index name -> mappings given at creation
        self.mappings: dict = {}

        # set to an exception instance to make every call raise it
        self.outage: Exception | None = None

        self.indices = FakeIndices(self)

        self._scroll_ids = itertools.count(1)

    def check_available(self):
        """raise the outage exception, if any"""
        if self.outage is not None:
            raise self.outage

    def _check_mapping(self, index: str, fields):
        mapping = self.mappings.get(index, {})
        if mapping.get("dynamic") != "strict":
            return
        unknown = set(fields) - set(mapping.get("properties", {}))
        if unknown:
            raise RequestError(400, "strict_dynamic_mapping_exception", sorted(unknown))

    def index(
        self, index: str, body: dict, id: str = None, op_type: str = None, **_kwargs
    ):
        """write a document. op_type=create refuses an existing id with a 409"""
        # pylint: disable=redefined-builtin
        self.check_available()
        self._check_mapping(index, body)
        documents = self.documents.setdefault(index, {})
        if op_type == "create" and id in documents:
            raise ConflictError(409, "version_conflict_engine_exception", id)
        documents[id] = copy.deepcopy(body)
        return {"_index": index, "_id": id, "result": "created"}

    def get(self, index: str, id: str, **_kwargs) -> dict:
        """read a document by id"""
        # pylint: disable=redefined-builtin
        self.check_available()
        if index not in self.documents:
            raise NotFoundError(404, "index_not_found_exception", index)
        if id not in self.documents[index]:
            raise NotFoundError(404, "not_found", id)
        return {
            "_index": index,
            "_id": id,
            "found": True,
            "_source": copy.deepcopy(self.documents[index][id]),
        }

    def update(self, index: str, id: str, body: dict, **_kwargs) -> dict:
        """merge body["doc"] into a document"""
        # pylint: disable=redefined-builtin
        self.check_available()
        self._check_mapping(index, body["doc"])
        if id not in self.documents.get(index, {}):
            raise NotFoundError(404, "document_missing_exception", id)
        self.documents[index][id].update(copy.deepcopy(body["doc"]))
        return {"_index": index, "_id": id, "result": "updated"}

    def search(
        self,
        index: str = None,
        body: dict = None,
        size: int = None,
        scroll: str = None,
        **_kwargs,
    ) -> dict:
        """bool / term / terms / match_all queries, sort, size. With scroll, every
        hit comes in the first page, and scroll() ends the iteration"""
        self.check_available()
        if index not in self.documents:
            raise NotFoundError(404, "index_not_found_exception", index)
        body = body or {}
        hits = [
            {"_index": index, "_id": doc_id, "_source": copy.deepcopy(source)}
            for doc_id, source in self.documents[index].items()
            if matches(source, body.get("query", {"match_all": {}}))
        ]
        hits = sort_hits(hits, body.get("sort", []))
        if scroll is None:
            hits = hits[: body.get("size", size if size is not None else DEFAULT_SIZE)]
        response = {
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {"total": {"value": len(hits)}, "hits": hits},
        }
        if scroll is not None:
            response["_scroll_id"] = f"scroll-{next(self._scroll_ids)}"
        return response

    def scroll(self, body: dict = None, **_kwargs) -> dict:
        """next page of a scroll: always empty, search() returned every hit"""
        self.check_available()
        return {
            "_scroll_id": body["scroll_id"],
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {"hits": []},
        }

    def clear_scroll(self, **_kwargs) -> dict:
        """release a scroll"""
        return {"succeeded": True}


def matches(source: dict, query: dict) -> bool:
    """evaluate the subset of the query DSL the configuration code uses"""
    if "match_all" in query:
        return True

    if "term" in query:
        ((field, value),) = query["term"].items()
        if isinstance(value, dict):
            value = value["value"]
        stored = source.get(field)
        if isinstance(stored, list):
            return value in stored
        return field in source and stored == value

    if "terms" in query:
        ((field, values),) = query["terms"].items()
        return any(matches(source, {"term": {field: value}}) for value in values)

    if "bool" in query:
        clauses = query["bool"]

        def as_list(name):
            value = clauses.get(name, [])
            return value if isinstance(value, list) else [value]

        return all(
            matches(source, clause) for clause in as_list("filter") + as_list("must")
        ) and not any(matches(source, clause) for clause in as_list("must_not"))

    raise NotImplementedError(f"FakeOpenSearch does not support the query {query}")


def sort_hits(hits: list, sort) -> list:
    """apply a sort clause: "_doc", {field: order} or {field: {"order": order}}"""
    if isinstance(sort, (str, dict)):
        sort = [sort]
    for clause in reversed(sort):
        if clause == "_doc":
            continue
        ((field, order),) = clause.items()
        if isinstance(order, dict):
            order = order.get("order", "asc")
        hits = sorted(
            hits, key=lambda hit: hit["_source"][field], reverse=order == "desc"
        )
    return hits


def add_config_document(
    client: FakeOpenSearch,
    path: str,
    version: int,
    services: list,
    content: dict,
    latest: bool = True,
    deleted: bool = False,
    index: str = "maas-collector-config",
):
    """write a configuration document as a publish would, without validation"""
    client.index(
        index=index,
        id=f"{path}@{version}",
        body={
            "path": path,
            "version": version,
            "latest": latest,
            "deleted": deleted,
            "services": services,
            "content": content,
        },
    )
