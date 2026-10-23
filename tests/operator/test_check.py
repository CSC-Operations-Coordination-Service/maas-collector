import base64
import json
from urllib.parse import unquote

import pytest
import requests

from fakeopensearch import add_config_document

from maas_collector.rawdata.collector.credentialprovider import (
    VaultArgs,
    VaultCredentialProvider,
)
from maas_collector.rawdata.collector.odatacollector import (
    ODataCollector,
    ODataCollectorConfiguration,
)
from maas_collector.rawdata.configuration import build_collector_configuration
from maas_collector.rawdata.operator.check import (
    ProductChecker,
    diff_documents,
    odata_quote,
    product_query_url,
)
from maas_collector.rawdata.operator.contract import CheckRequest

CONFIG_INDEX = "maas-collector-config"
PRODUCT_INDEX = "raw-data-check-product"
REMOTE_URL = "http://remote.example"
PRODUCT_ID = "3d5934fa-ca8a-4e74-a394-1eca3626e967"
PRODUCT_NAME = "S1A_X.SAFE"

MODEL = {
    "name": "CheckProduct",
    "index": PRODUCT_INDEX,
    "fields": [
        {"name": "product_id", "type": "Keyword"},
        {"name": "product_name", "type": "Keyword"},
        {"name": "content_length", "type": "Integer"},
        {"name": "publication_date", "type": "Date"},
    ],
}


def entry(name="IF", **extra):
    return {
        "class": "ODataCollectorConfiguration",
        "interface_name": name,
        "interface_credentials": "common_cred",
        "auth_method": "Basic",
        "id_field": "product_id",
        "routing_key": "new.raw.data.check-product",
        "custom_query_suffix": "&$expand=Attributes",
        "model": "CheckProduct",
        "extractor": {
            "class": "JSONExtractor",
            "args": {
                "attr_map": {
                    "product_id": "`this`.Id",
                    "product_name": "`this`.Name",
                    "content_length": "`this`.ContentLength",
                    "publication_date": "`this`.PublicationDate",
                },
                "iterate_nodes": "$.value",
            },
        },
        **extra,
    }


CFG = build_collector_configuration(
    entry(odata_product_url=REMOTE_URL, model=MODEL),
    ODataCollectorConfiguration,
    register=False,
)


def product(publication_date="2026-10-02T00:00:00.000Z", content_length=129873):
    return {
        "@odata.mediaContentType": "application/octet-stream",
        "Id": PRODUCT_ID,
        "Name": PRODUCT_NAME,
        "ContentLength": content_length,
        "PublicationDate": publication_date,
    }


class Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.content = b"payload"

    def json(self):
        return self.payload


class Remote:
    """stands for requests.Session.get: answers one response, records the calls"""

    def __init__(self, monkeypatch):
        self.response = Response({"value": []})
        self.calls = []
        remote = self

        def get(_session, url, **kwargs):
            remote.calls.append((url, kwargs))
            return remote.response

        monkeypatch.setattr(requests.Session, "get", get)

    def answer(self, payload, status_code=200):
        self.response = Response(payload, status_code)


@pytest.fixture
def remote(monkeypatch):
    return Remote(monkeypatch)


@pytest.fixture
def provider(fake_vault, tmp_path):
    secret_id_file = tmp_path / "secret_id"
    secret_id_file.write_text(fake_vault.secret_id + "\n", encoding="UTF-8")
    fake_vault.put(
        "secret",
        "maas-collector/common_cred",
        {
            "odata_product_url": REMOTE_URL,
            "client_username": "user",
            "client_password": "secret",
            "token_field_header": "Authorization",
        },
    )
    return VaultCredentialProvider(
        VaultArgs(
            url=fake_vault.url,
            role_id=fake_vault.role_id,
            secret_id_file=str(secret_id_file),
        )
    )


def add_entries(client, *collectors, services=("odata",)):
    add_config_document(
        client,
        "odata/check.json",
        1,
        list(services),
        {"collectors": list(collectors), "models": [MODEL]},
    )


@pytest.fixture
def checker(fake_opensearch, provider, tmp_path):
    add_entries(fake_opensearch, entry())
    return ProductChecker("odata", CONFIG_INDEX, provider, str(tmp_path / "work"))


def by_name(name=PRODUCT_NAME, interface="IF"):
    return CheckRequest("o", interface, product_name=name)


def by_id(product_id=PRODUCT_ID, interface="IF"):
    return CheckRequest("o", interface, product_id=product_id)


def store(client, source):
    client.index(index=PRODUCT_INDEX, id=PRODUCT_ID, body=source)


def stored_source(publication_date="2026-10-02T00:00:00.000Z", **extra):
    return {
        "product_id": PRODUCT_ID,
        "product_name": PRODUCT_NAME,
        "content_length": 129873,
        "publication_date": publication_date,
        "reportName": REMOTE_URL,
        "ingestionTime": "2026-10-03T10:00:00.000Z",
        **extra,
    }


def test_quote_is_doubled():
    assert odata_quote("a'b''c") == "a''b''''c"


def test_quote_in_a_name_is_doubled():
    url = product_query_url(CFG, CheckRequest("o", "IF", product_name="S1A_X'Y.SAFE"))
    assert "Name eq 'S1A_X''Y.SAFE'" in unquote(url)


def test_name_query_filters_on_the_products_of_the_interface():
    url = product_query_url(CFG, by_name("S1A_X&Y.SAFE"))
    assert url.startswith(f"{REMOTE_URL}/odata/v1/Products?$filter=")
    # an & in the name must not end the filter
    assert "S1A_X&Y" not in url
    assert url.endswith("&$expand=Attributes")


def test_id_uses_the_entity_url():
    url = product_query_url(CFG, by_id())
    assert url == (
        ODataCollector.build_get_an_entity_query(CFG, PRODUCT_ID)
        + "?$expand=Attributes"
    )


def test_diff_lists_the_differing_fields_sorted():
    assert diff_documents(
        {"b": 1, "a": 2, "same": 0, "reportName": "x", "ingestionTime": "t1"},
        {"b": 3, "c": 4, "same": 0, "reportName": "y", "ingestionTime": "t2"},
    ) == [
        {"field": "a", "remote": 2, "stored": None},
        {"field": "b", "remote": 1, "stored": 3},
        {"field": "c", "remote": None, "stored": 4},
    ]


def test_missing_on_remote_and_in_maas(checker, remote):
    remote.answer({"value": []})
    r = checker.check(by_name())
    assert r.status == "ok" and r.remote is None and r.stored is None
    assert r.diff == [] and r.error is None


def test_query_uses_the_credentials_of_the_interface(checker, remote):
    checker.check(by_name())
    ((url, kwargs),) = remote.calls
    assert url.startswith(REMOTE_URL)
    token = base64.b64encode(b"user:secret").decode()
    assert kwargs["headers"] == {"Authorization": f"Basic {token}"}
    # the backend gives up on a check well before the default auth_timeout
    assert kwargs["timeout"] == 20.0


def test_timeout_bounds_the_query_and_the_token(
    fake_opensearch, provider, remote, monkeypatch
):
    posts = []

    def post(_session, url, **kwargs):
        posts.append(kwargs)
        return Response(
            {"access_token": "tok", "token_type": "Bearer", "expires_in": 60}
        )

    monkeypatch.setattr(requests.Session, "post", post)
    add_entries(
        fake_opensearch,
        entry(auth_method="OAuth", token_url="http://token.example", auth_timeout=120),
    )
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp", timeout=5)
    r = checker.check(by_name())
    assert r.status == "ok"
    (token_kwargs,) = posts
    ((_url, kwargs),) = remote.calls
    assert token_kwargs["timeout"] == 5
    assert kwargs["timeout"] == 5
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}


def test_present_on_remote_only(checker, remote):
    remote.answer({"value": [product()]})
    r = checker.check(by_name())
    assert r.status == "ok" and r.stored is None
    assert r.remote["product_id"] == PRODUCT_ID
    assert r.remote["publication_date"] == "2026-10-02T00:00:00.000Z"
    assert r.remote["reportName"] == REMOTE_URL
    assert r.diff == []
    # the reply goes on the wire as JSON
    json.dumps(r.to_body())


def test_present_in_both_and_identical(checker, remote, fake_opensearch):
    remote.answer({"value": [product()]})
    store(fake_opensearch, checker.check(by_name()).remote)
    r = checker.check(by_name())
    assert r.status == "ok" and r.remote is not None and r.stored is not None
    assert r.diff == []


def test_present_in_both_with_a_different_publication_date(
    checker, remote, fake_opensearch
):
    store(fake_opensearch, stored_source("2026-10-01T00:00:00.000Z"))
    remote.answer({"value": [product("2026-10-02T00:00:00.000Z")]})
    r = checker.check(by_name())
    assert r.status == "ok"
    assert r.stored["publication_date"] == "2026-10-01T00:00:00.000Z"
    assert r.diff == [
        {
            "field": "publication_date",
            "remote": "2026-10-02T00:00:00.000Z",
            "stored": "2026-10-01T00:00:00.000Z",
        }
    ]


def test_report_name_and_ingestion_time_are_not_differences(
    checker, remote, fake_opensearch
):
    store(
        fake_opensearch,
        stored_source(reportName="other.json", ingestionTime="2020-01-01T00:00:00Z"),
    )
    remote.answer({"value": [product()]})
    r = checker.check(by_name())
    assert r.status == "ok" and r.stored["reportName"] == "other.json"
    assert r.diff == []


def test_fields_only_in_maas_are_kept_like_a_collect(checker, remote, fake_opensearch):
    store(fake_opensearch, stored_source(dhus_tag="added later"))
    remote.answer({"value": [product()]})
    r = checker.check(by_name())
    assert r.remote["dhus_tag"] == "added later"
    assert r.diff == []


def test_stored_date_without_milliseconds_is_not_a_difference(
    checker, remote, fake_opensearch
):
    store(fake_opensearch, stored_source("2026-10-02T00:00:00Z"))
    remote.answer({"value": [product("2026-10-02T00:00:00.000Z")]})
    r = checker.check(by_name())
    assert r.status == "ok"
    assert r.stored["publication_date"] == "2026-10-02T00:00:00.000Z"
    assert r.diff == []


def test_extraction_failure_is_an_error_reply(checker, remote):
    remote.answer({"value": [product(content_length="not-a-number")]})
    r = checker.check(by_name())
    assert r.status == "error"
    assert r.error.startswith("extraction failed: ")
    assert "not-a-number" in r.error


def test_extraction_without_document_is_an_error_reply(
    fake_opensearch, provider, remote
):
    interface = entry()
    interface["extractor"]["args"]["iterate_nodes"] = "$.value[0].Attributes"
    add_entries(fake_opensearch, interface)
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    remote.answer({"value": [{**product(), "Attributes": []}]})
    r = checker.check(by_name())
    assert r.status == "error" and r.error == "extraction produced no document"


def test_extractor_error_is_an_extraction_failure(fake_opensearch, provider, remote):
    interface = entry()
    interface["extractor"]["args"]["iterate_nodes"] = "$.nothing"
    add_entries(fake_opensearch, interface)
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    remote.answer({"value": [product()]})
    r = checker.check(by_name())
    assert r.status == "error" and r.error.startswith("extraction failed: ")


def test_missing_on_remote_but_in_maas(checker, remote, fake_opensearch):
    store(fake_opensearch, stored_source("2026-10-02T00:00:00Z"))
    remote.answer({"value": []})
    r = checker.check(by_id())
    assert r.status == "ok" and r.remote is None
    assert r.stored["product_id"] == PRODUCT_ID
    # normalised by the model like a remote document
    assert r.stored["publication_date"] == "2026-10-02T00:00:00.000Z"
    assert r.diff == []


def test_unknown_id_in_maas_and_on_remote(checker, remote, fake_opensearch):
    fake_opensearch.index(
        index=PRODUCT_INDEX, id="other", body=stored_source(product_id="other")
    )
    remote.answer({"error": {"code": "404"}}, 404)
    r = checker.check(by_id())
    assert r.status == "ok" and r.remote is None and r.stored is None


def test_single_entity_by_id(checker, remote, fake_opensearch):
    store(fake_opensearch, stored_source())
    remote.answer({"@odata.context": "$metadata#Products/$entity", **product()})
    r = checker.check(by_id())
    assert r.status == "ok" and r.remote["product_name"] == PRODUCT_NAME
    assert r.diff == []
    ((url, _kwargs),) = remote.calls
    assert f"Products({PRODUCT_ID})" in url


def test_unknown_id_is_missing_on_remote(checker, remote):
    remote.answer({"error": {"code": "404"}}, 404)
    r = checker.check(by_id())
    assert r.status == "ok" and r.remote is None and r.stored is None


def test_remote_http_error_is_an_error_reply(checker, remote):
    remote.answer({"error": "unavailable"}, 503)
    r = checker.check(by_name())
    assert r.status == "error" and "503" in r.error
    assert r.remote is None and r.stored is None


def test_remote_connection_error_is_an_error_reply(checker, monkeypatch):
    def refuse(*_args, **_kwargs):
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(requests.Session, "get", refuse)
    r = checker.check(by_name())
    assert r.status == "error" and "refused" in r.error


def test_work_folder_is_left_empty(checker, remote, tmp_path):
    remote.answer({"value": [product()]})
    checker.check(by_name())
    assert list((tmp_path / "work").iterdir()) == []


def test_non_odata_interface_is_an_error_reply(fake_opensearch, provider, remote):
    add_entries(
        fake_opensearch,
        entry("JIRA", **{"class": "JiraExtendedCollectorConfiguration"}),
    )
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    r = checker.check(by_name(interface="JIRA"))
    assert r.status == "error" and "JIRA" in r.error
    assert remote.calls == []


def test_odata_v3_interface_is_an_error_reply(fake_opensearch, provider, remote):
    add_entries(fake_opensearch, entry("V3", odata_version="v3"))
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    r = checker.check(by_name(interface="V3"))
    assert r.status == "error" and "v4" in r.error
    assert remote.calls == []


def test_disabled_interface_is_an_error_reply(fake_opensearch, provider, remote):
    add_entries(fake_opensearch, entry(enabled=False))
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    r = checker.check(by_name())
    assert r.status == "error" and "disabled" in r.error
    assert remote.calls == []


def test_unknown_interface_is_an_error_reply(checker, remote):
    r = checker.check(by_name(interface="NOPE"))
    assert r.status == "error" and "NOPE" in r.error
    assert remote.calls == []


def test_interface_of_another_service_is_an_error_reply(
    fake_opensearch, provider, remote
):
    add_entries(fake_opensearch, entry(), services=("other",))
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    r = checker.check(by_name())
    assert r.status == "error"
    assert remote.calls == []


def test_unreadable_configuration_index_is_an_error_reply(fake_opensearch, provider):
    fake_opensearch.outage = ConnectionError("opensearch down")
    checker = ProductChecker("odata", CONFIG_INDEX, provider, "/tmp")
    r = checker.check(by_name())
    assert r.status == "error" and "opensearch down" in r.error
