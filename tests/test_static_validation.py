"""Tests for the validation that never evaluates operator code"""

import builtins
import json
import os

import pytest

from backup_samples import S3, backups_file, entry
from maas_collector.rawdata.configprovider import (
    ConfigSource,
    ConfigValidationError,
    validate_config_sources,
)
from maas_collector.rawdata.configstore import ConfigStore, config_classes_for
from maas_collector.rawdata.extractor.static import (
    check_extractor_static,
    check_lambda_source,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

INDEX = "maas-collector-config"

PATH = "odata/entries.json"

LAMBDA_ENTRY = lambda python: {
    "id_field": "",
    "routing_key": "",
    "interface_name": "ITF_A",
    "model": {"name": "ModelA", "index": "", "fields": []},
    "extractor": {
        # builds its lambdas in its constructor, which the control test relies on
        "class": "JSONExtractor",
        "args": {"attr_map": {"a": "$.a", "b": {"python": python}}},
    },
}


def boom(*_args, **_kwargs):
    raise AssertionError("evaluated")


def test_a_lambda_is_accepted():
    check_lambda_source("lambda row: row[0].upper()")
    check_lambda_source("lambda x, *, y: x")


@pytest.mark.parametrize(
    "code",
    [
        "x",
        "",
        "lambda x, y=1: x",
        "lambda x, *, y=1: x",
        "lambda x: x)(",
        "(lambda x: x)(1)",
        "__import__('os').system('true')",
        "[lambda x: x]",
        5,
        None,
    ],
)
def test_check_lambda_source_refuses(code):
    with pytest.raises(ValueError):
        check_lambda_source(code)


def test_nothing_is_evaluated(monkeypatch, tmp_path):
    marker = tmp_path / "marker"
    monkeypatch.setattr(builtins, "eval", boom)
    monkeypatch.setattr(builtins, "exec", boom)
    # parsed, not run
    check_lambda_source(f"lambda x: open({str(marker)!r}, 'w')")
    # a default argument would run at definition
    with pytest.raises(ValueError):
        check_lambda_source(f"lambda x, y=open({str(marker)!r}, 'w'): x")
    assert not marker.exists()


@pytest.mark.parametrize(
    "name", ["Nope", "get_hash_func", "BaseExtractor", "os", "", None]
)
def test_the_extractor_class_must_be_a_concrete_extractor(name):
    with pytest.raises(ValueError):
        check_extractor_static({"class": name, "args": {}})


def test_arguments_are_checked_against_the_signature():
    with pytest.raises(ValueError, match="unknown_arg"):
        check_extractor_static(
            {"class": "LogExtractor", "args": {"pattern": ".*", "unknown_arg": 1}}
        )
    with pytest.raises(ValueError, match="attr_map"):
        check_extractor_static({"class": "XMLExtractor", "args": {}})


def test_lambdas_are_found_in_lists_dicts_and_both_python_keys():
    check_extractor_static(LAMBDA_ENTRY("lambda r: r[0]")["extractor"])
    check_extractor_static(
        {
            "class": "XMLExtractor",
            "args": {"attr_map": {"x": {"rpython": "lambda root: 1"}}},
        }
    )
    with pytest.raises(ValueError, match="rpython|x"):
        check_extractor_static(
            {
                "class": "XMLExtractor",
                "args": {"attr_map": {"x": {"rpython": "1 + 1"}}},
            }
        )


def test_python_converters_are_checked_at_both_levels():
    ok = {"f": {"type": "python", "python": "lambda v: v"}}
    check_extractor_static(
        {"class": "LogExtractor", "args": {"pattern": ".*"}, "converter_map": ok}
    )
    check_extractor_static(
        {"class": "LogExtractor", "args": {"pattern": ".*", "converter_map": ok}}
    )
    with pytest.raises(ValueError, match="f"):
        check_extractor_static(
            {
                "class": "LogExtractor",
                "args": {"pattern": ".*"},
                "converter_map": {"f": {"type": "python", "python": "oops"}},
            }
        )


def test_static_validation_never_evaluates(monkeypatch):
    monkeypatch.setattr(builtins, "eval", boom)
    validate_config_sources(
        [ConfigSource("a.json", {"collectors": [LAMBDA_ENTRY("lambda r: r[0]")]})],
        config_classes_for,
        resolve_models=False,
        evaluate=False,
    )


def test_the_default_mode_still_evaluates(monkeypatch):
    monkeypatch.setattr(builtins, "eval", boom)
    with pytest.raises(ConfigValidationError):
        validate_config_sources(
            [ConfigSource("a.json", {"collectors": [LAMBDA_ENTRY("lambda r: r[0]")]})],
            config_classes_for,
            resolve_models=False,
        )


def test_static_validation_refuses_a_bad_lambda_with_the_source_label():
    with pytest.raises(ConfigValidationError, match="a.json") as error:
        validate_config_sources(
            [ConfigSource("a.json", {"collectors": [LAMBDA_ENTRY("nope")]})],
            config_classes_for,
            resolve_models=False,
            evaluate=False,
        )
    assert "python" in str(error.value)


def test_static_validation_still_checks_the_rest():
    def check(*contents):
        validate_config_sources(
            [ConfigSource(f"f{i}.json", c) for i, c in enumerate(contents)],
            config_classes_for,
            evaluate=False,
        )

    with pytest.raises(ConfigValidationError, match="unknown configuration class"):
        check({"collectors": [entry(**{"class": "Nope"})]})
    bad_model = {"name": "M", "index": "", "fields": [{"name": "x", "type": "Nope"}]}
    with pytest.raises(ConfigValidationError):
        check({"models": [bad_model]})
    with pytest.raises(ConfigValidationError, match="s3_secret_key"):
        check(backups_file(S3 | {"s3_secret_key": "x"}))
    with pytest.raises(ConfigValidationError, match="@"):
        check({"collectors": [entry("ITF_A@custom")]})


def test_every_python_string_of_tests_conf_passes_statically():
    found = []

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("python", "rpython") and isinstance(value, str):
                    found.append(value)
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for folder, _dirs, names in os.walk(CONF_DIR):
        for name in names:
            if name.endswith(".json"):
                with open(os.path.join(folder, name), encoding="UTF-8") as conf_fd:
                    walk(json.load(conf_fd))

    for code in found:
        check_lambda_source(code)


def test_a_configstore_without_evaluation_publishes_and_rolls_back_statically(
    fake_opensearch, monkeypatch
):
    store = ConfigStore(fake_opensearch, INDEX, evaluate=False)
    monkeypatch.setattr(builtins, "eval", boom)
    store.publish(
        PATH, {"collectors": [LAMBDA_ENTRY("lambda r: r[0]")]}, ["odata"], "alice", "v1"
    )
    with pytest.raises(ConfigValidationError):
        store.publish(
            PATH, {"collectors": [LAMBDA_ENTRY("nope")]}, ["odata"], "alice", "v2"
        )
    store.rollback(PATH, 1, "alice", "back")
