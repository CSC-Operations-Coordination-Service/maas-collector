"""Tests for the versioned configuration documents written by ConfigStore"""

import json
import os

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata import configuration
from maas_collector.rawdata.configprovider import (
    ConfigValidationError,
    OpenSearchConfigProvider,
)
from maas_collector.rawdata.configstore import (
    CONFIG_INDEX_BODY,
    ConfigConflictError,
    ConfigNotFoundError,
    ConfigStore,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

INDEX = "maas-collector-config"

PATH = "odata/prip/maas-collector-odata-prip.json"


def read_json(name):
    with open(os.path.join(CONF_DIR, name), encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def entry(model, interface_name="ITF_A", extractor_class="LogExtractor"):
    """a collector entry without class: FileCollectorConfiguration"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": model,
        "extractor": {"class": extractor_class, "args": {"pattern": ".*"}},
    }


def content(model_name="StoreModelA", **kwargs):
    return {
        "collectors": [entry({"name": model_name, "index": "", "fields": []}, **kwargs)]
    }


@pytest.fixture
def client(fake_opensearch):
    """the fake is also the default connection, read by the provider"""
    return fake_opensearch


@pytest.fixture
def store(client):
    return ConfigStore(client, INDEX)


def latest_flags(store, path=PATH):
    return {doc["version"]: doc["latest"] for doc in store.history(path)}


def test_init_creates_the_strict_index_once(store, client):
    assert store.init() is True
    assert store.init() is False

    assert client.mappings[INDEX]["dynamic"] == "strict"
    assert client.mappings[INDEX] == CONFIG_INDEX_BODY["mappings"]


def test_first_publish_writes_version_one(store, client):
    document = store.publish(PATH, content(), ["odata"], "alice", "first import")

    assert document["version"] == 1
    assert document["latest"] is True
    assert document["deleted"] is False
    assert document["based_on"] is None
    assert document["interface_names"] == ["ITF_A"]
    assert document["model_names"] == ["StoreModelA"]
    assert f"{PATH}@1" in client.documents[INDEX]


def test_second_publish_moves_the_latest_flag(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")

    document = store.publish(
        PATH, content("StoreModelB"), ["odata"], "bob", "v2", base_version=1
    )

    assert document["version"] == 2
    assert document["based_on"] == 1
    assert latest_flags(store) == {1: False, 2: True}
    assert store.get(PATH)["content"] == content("StoreModelB")


def test_publish_on_an_outdated_base_is_refused(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2", base_version=1)

    with pytest.raises(ConfigConflictError, match="version 2 was published after"):
        store.publish(
            PATH, content("StoreModelC"), ["odata"], "carol", "v3", base_version=1
        )

    assert [doc["version"] for doc in store.history(PATH)] == [1, 2]


def test_concurrent_publishes_on_the_same_base_conflict(store, client, monkeypatch):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    stale_history = store.history(PATH)

    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2")

    # carol read the history before bob wrote version 2
    racing = ConfigStore(client, INDEX)
    monkeypatch.setattr(racing, "history", lambda path: stale_history)

    with pytest.raises(ConfigConflictError, match="published concurrently"):
        racing.publish(PATH, content("StoreModelC"), ["odata"], "carol", "v2 too")

    assert store.get(PATH)["author"] == "bob"
    assert latest_flags(store) == {1: False, 2: True}


def test_publish_repairs_the_flags_of_a_crashed_publish(store, client):
    """a publish that died between its two steps left two latest versions"""
    add_config_document(client, PATH, 1, ["odata"], content())
    add_config_document(client, PATH, 2, ["odata"], content("StoreModelB"))
    store.init()

    store.publish(PATH, content("StoreModelC"), ["odata"], "alice", "v3")

    assert latest_flags(store) == {1: False, 2: False, 3: True}


def test_a_late_publish_leaves_a_newer_version_latest(store, client):
    """step 4 of a slow publish must not clear the flag of a later publish"""
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2")
    add_config_document(client, PATH, 3, ["odata"], content("StoreModelC"))

    store._clear_latest(PATH, 2)

    assert latest_flags(store)[3] is True


def test_history_lists_every_version_oldest_first(store):
    for number in range(1, 13):
        store.publish(PATH, content(f"StoreModel{number}"), ["odata"], "alice", "edit")

    history = store.history(PATH)

    assert [doc["version"] for doc in history] == list(range(1, 13))


def test_rollback_publishes_an_old_version_again(store):
    store.publish(PATH, content(), ["odata", "sftp"], "alice", "v1")
    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2")

    document = store.rollback(PATH, 1, "carol", "back to v1")

    assert document["version"] == 3
    assert document["based_on"] == 1
    assert document["content"] == content()
    assert document["services"] == ["odata", "sftp"]
    assert latest_flags(store) == {1: False, 2: False, 3: True}


def test_rollback_to_an_unknown_version_is_refused(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")

    with pytest.raises(ConfigNotFoundError):
        store.rollback(PATH, 7, "carol", "nope")


def test_delete_publishes_a_tombstone(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.publish("odata/other.json", content("StoreModelB"), ["odata"], "alice", "v1")

    document = store.delete(PATH, "bob", "interface decommissioned")

    assert document["version"] == 2
    assert document["deleted"] is True
    assert document["services"] == ["odata"]
    with pytest.raises(ConfigNotFoundError):
        store.delete(PATH, "bob", "twice")

    # collectors stop loading the path
    sources = OpenSearchConfigProvider(INDEX, "odata").load()
    assert [source.name for source in sources] == ["odata/other.json"]


def test_publish_after_a_delete_brings_the_path_back(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.delete(PATH, "bob", "removed")

    document = store.publish(PATH, content(), ["odata"], "carol", "restored")

    assert document["version"] == 3
    assert document["deleted"] is False


def test_credential_file_content_is_refused(store, client):
    credentials = read_json("test-maas-collector-credentials.json")

    with pytest.raises(ConfigValidationError, match="not a collector configuration"):
        store.publish("cred.json", credentials, ["odata"], "alice", "oops")

    assert client.documents.get(INDEX, {}) == {}


def test_unknown_extractor_class_is_refused(store):
    bad = content(extractor_class="NoSuchExtractor")

    with pytest.raises(ConfigValidationError, match=f"{PATH}: AttributeError"):
        store.publish(PATH, bad, ["odata"], "alice", "typo")


def test_unknown_configuration_class_is_refused(store):
    bad = content()
    bad["collectors"][0]["class"] = "NoSuchCollectorConfiguration"

    with pytest.raises(ConfigValidationError, match="unknown configuration class"):
        store.publish(PATH, bad, ["odata"], "alice", "typo")


@pytest.mark.parametrize(
    "name",
    [
        "maas-collector-interface-monitoring.json",
        "test-maas-collector-odata-basic.json",
        "maas-collector-conf-lta-deletion.json",
    ],
)
def test_real_configuration_files_are_accepted(store, name):
    store.publish(name, read_json(name), ["odata"], "alice", "import")

    assert store.get(name)["version"] == 1


def test_a_model_defined_in_another_file_is_accepted(store):
    """29 named models are shared: a file may use a model of another file"""
    uses_shared = {"collectors": [entry("SharedStoreModel")]}

    document = store.publish(PATH, uses_shared, ["odata"], "alice", "v1")

    assert document["model_names"] == ["SharedStoreModel"]


def test_validation_registers_no_model(store):
    before = dict(configuration.MODEL_DICT)

    store.publish(
        PATH,
        {
            **content("NeverRegisteredA"),
            "models": [{"name": "NeverRegisteredB", "index": "", "fields": []}],
        },
        ["odata"],
        "alice",
        "v1",
    )

    assert configuration.MODEL_DICT == before


def test_publish_needs_a_service(store):
    with pytest.raises(ValueError, match="at least one service"):
        store.publish(PATH, content(), [], "alice", "v1")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/etc/conf.json",
        "odata/../conf.json",
        "odata//conf.json",
        "./conf.json",
        "odata\\conf.json",
        "odata/conf.txt",
        "odata/",
    ],
)
def test_bad_paths_are_refused(store, client, path):
    with pytest.raises(ValueError, match="bad configuration path"):
        store.publish(path, content(), ["odata"], "alice", "v1")

    assert client.documents.get(INDEX, {}) == {}
