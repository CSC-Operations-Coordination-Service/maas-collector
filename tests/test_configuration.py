import json
import os

from maas_collector.rawdata import configuration
from maas_collector.rawdata.extractor.base import BaseExtractor

from maas_collector.rawdata.collector.filecollector import (
    FileCollector,
    FileCollectorConfiguration,
    CollectorArgs,
)

CURRENT_DIR = os.path.dirname(__file__)

CONF_DIR = os.path.join(CURRENT_DIR, "conf", "collector.d")

CONF1 = os.path.join(CONF_DIR, "conf1.json")

RESOLVE_DIR = os.path.join(CURRENT_DIR, "conf", "model_resolve")


def test_configuration():
    class MockDocument:
        pass

    class DumbExtractor(BaseExtractor):
        def extract(self, path, report_folder: str = ""):
            yield {}

    colconf = FileCollectorConfiguration(
        model=MockDocument,
        id_field="id_field",
        extractor=DumbExtractor,
        routing_key="routing.key",
        model_meta={},
    )
    assert colconf.name == "MockDocumentConfiguration"


def test_load_configuration():
    c = FileCollector(CollectorArgs(rawdata_config=CONF1))

    c.load_config()

    assert len(c.configs) == 1

    assert c.configs[0].name == "EmptyModel1Configuration"


def test_load_configuration_dir():
    c = FileCollector(CollectorArgs(rawdata_config_dir=CONF_DIR))

    c.load_config()

    assert len(c.configs) == 5

    # the sort behavior of os.walk is NOT determinist
    assert set(config.name for config in c.configs) == set(
        [
            "EmptyModel1Configuration",
            "EmptyModel2Configuration",
            "EmptyModel3Configuration",
            "EmptyModel4Configuration",
            "EmptyModel5Configuration",
        ]
    )


def test_postponed_models():
    c = FileCollector(CollectorArgs(rawdata_config_dir=RESOLVE_DIR))

    c.load_config()

    assert len(c.configs) == 2

    assert set(config.name for config in c.configs) == set(
        ["PostPonedModel1Configuration", "PostPonedModel2Configuration"]
    )


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def test_load_json_dict_builds_the_same_configurations_as_load_json():
    from_file = list(configuration.load_json(CONF1, FileCollectorConfiguration))

    from_dict = list(
        configuration.load_json_dict(
            read_json(CONF1), FileCollectorConfiguration, "conf1.json"
        )
    )

    assert [config.name for config in from_dict] == [
        config.name for config in from_file
    ]
    assert from_dict[0].model_meta == from_file[0].model_meta


def test_load_json_dict_registers_the_models_of_a_model_only_content():
    content = {"models": [{"name": "DictOnlyModel", "index": "", "fields": []}]}

    configs = list(
        configuration.load_json_dict(content, FileCollectorConfiguration, "models")
    )

    assert configs == []
    assert configuration.MODEL_DICT["DictOnlyModel"].__name__ == "DictOnlyModel"


def test_unregistered_models_leave_model_dict_untouched():
    before = dict(configuration.MODEL_DICT)
    content = {
        "collectors": [
            {
                "id_field": "",
                "routing_key": "",
                "model": {"name": "InlineCheckedModel", "index": "", "fields": []},
                "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
            }
        ],
        "models": [{"name": "SharedCheckedModel", "index": "", "fields": []}],
    }

    configs = list(
        configuration.load_json_dict(
            content, FileCollectorConfiguration, "check", register=False
        )
    )

    assert configs[0].model.__name__ == "InlineCheckedModel"
    assert configuration.MODEL_DICT == before
