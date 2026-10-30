"""Base classes and factory functions for mass_collector"""

import os
import json
import logging

import opensearchpy

from maas_model import MAASRawDocument, ZuluDate

from maas_collector.rawdata import extractor
from maas_collector.rawdata.extractor.static import check_extractor_static

LOGGER = logging.getLogger(__name__)

# store all the declared module to allow name resolution after all files
# have been loaded. Would have been better in more OO way, but 'no Ferrari'
MODEL_DICT = {}


def build_model_class(meta: dict, register: bool = True) -> MAASRawDocument:
    """opensearch document class factory

    Args:
        meta (dict): meta-information from configuration file
        register (bool, optional): add the class to MODEL_DICT. False only checks
            that the class builds. Defaults to True.

    Returns:
        model.MAASRawDocument: Document class object
    """
    # populate document class attribute dictionnary using opensearchpy types only
    # maybe later add defaults and other raw database properties
    class_dict = {}
    for field_meta in meta["fields"]:
        args = {}
        # replace Date fields by ZuluDate
        if field_meta["type"] == "Date":
            class_obj = ZuluDate
        else:
            try:
                class_obj = getattr(opensearchpy, field_meta["type"])
            except AttributeError:
                LOGGER.critical(
                    "%s: '%s' is not a data type class name of opensearchpy module",
                    meta["name"],
                    field_meta["type"],
                )
                raise
        class_dict[field_meta["name"]] = class_obj(**args)

    # Inner class to declare opensearch index name
    class_dict["Index"] = type("Index", (object,), {"name": meta["index"]})

    # Partitionning optionnal settings
    if "partition_field" in meta:
        if meta["partition_field"] != "ingestionTime":
            # check if partition field is a date field
            assert isinstance(class_dict[meta["partition_field"]], ZuluDate)

        class_dict["_PARTITION_FIELD"] = meta["partition_field"]

    if "partition_format" in meta:
        # override yearly default
        class_dict["_PARTITION_FIELD_FORMAT"] = meta["partition_format"]

    # make search() works with partitions
    class_dict["_matches"] = classmethod(
        lambda cls, hit: hit["_index"].startswith(meta["index"])
    )

    # No configuration about partition ? Ok no partionning so
    if not "partition_field" in meta and not "partition_format" in meta:
        class_dict["_PARTITION_FIELD"] = None
        class_dict["_PARTITION_FIELD_FORMAT"] = None

    # return new document class
    model_class = type(meta["name"], (MAASRawDocument,), class_dict)

    if register:
        add_model(model_class)

    return model_class


def build_extractor(meta: dict) -> extractor.base.BaseExtractor:
    """build extractor instance from meta data

    Args:
        meta (dict): meta-information from configuration file

    Returns:
        extractor.BaseExtractor: Configured instance of an extrator
    """
    try:
        class_obj = getattr(extractor, meta["class"])
    except AttributeError:
        LOGGER.critical("Extractor class not found: %s", meta["class"])
        raise

    args = {}
    if "args" in meta:
        args.update(meta["args"])
    if "converter_map" in meta:
        args["converter_map"] = meta["converter_map"]
    return class_obj(**args)


def build_collector_configuration(
    meta: dict, configuration_class, register=True, evaluate=True
):
    """build CollectorConfiguration instance from json dict

    Args:
        meta (dict): configuration meta information
        configuration_class: configuration dataclass to instanciate
        register (bool, optional): register an inline model in MODEL_DICT.
            Defaults to True.
        evaluate (bool, optional): build the extractor, which evaluates its lambdas
            and converters. False only checks them statically and leaves the
            extractor of the configuration to None. Defaults to True.

    Returns:
        CollectorConfiguration: build configuration instance
    """

    if not isinstance(meta.get("enabled", True), bool):
        raise ValueError(
            f"enabled must be true or false, got {meta['enabled']!r}"
            f" in {meta.get('interface_name')}"
        )

    # copy meta args to leave argument untouched
    args = meta.copy()

    # store model meta for later usage like date convertion

    if "model" in meta:
        # create opensearchpy model class
        if isinstance(meta["model"], dict):
            args["model_meta"] = meta["model"].copy()
            args["model"] = build_model_class(meta["model"], register=register)
        else:
            args["model_meta"] = meta["model"]
            LOGGER.debug("Post-poned model resolution: %s", meta["model"])
    else:
        LOGGER.debug("No model declared in : %s", meta)
        args["model"] = None
        args["model_meta"] = None

    if "extractor" in meta:
        if evaluate:
            # instanciate the extractor
            args["extractor"] = build_extractor(args["extractor"])
        else:
            check_extractor_static(args["extractor"])
            args["extractor"] = None
    else:
        LOGGER.debug("No extractor declared in : %s", meta)
        args["extractor"] = None

    # instanciate the Configuration instance
    configuration_keys = set(configuration_class.__dataclass_fields__)

    configuration = configuration_class(
        **{name: value for name, value in args.items() if name in configuration_keys}
    )

    extra_keys = set(args) - configuration_keys

    if extra_keys:
        LOGGER.debug("Ignored configuration keys: %s", extra_keys)

    LOGGER.debug(
        "Configuration loaded: %s",
        configuration.name,
    )

    return configuration


def load_json_dict(
    json_dict: dict, configuration_class, source: str = "", register: bool = True
):
    """load a list of collector configuration from the content of a configuration
    file, and register its models

    Args:
        json_dict (dict): configuration content, {collectors?, models?, amqp?}
        configuration_class: configuration dataclass to instanciate
        source (str, optional): name of the content, for the logs. Defaults to "".
        register (bool, optional): register the models in MODEL_DICT. Defaults to
            True.

    Returns:
        typing.Iterator[CollectorConfiguration]: configuration instances
    """
    if "collectors" in json_dict:
        for conf_meta in json_dict["collectors"]:
            try:
                yield build_collector_configuration(
                    conf_meta, configuration_class, register=register
                )
            except Exception as error:
                LOGGER.critical(
                    "Cannot instanciate configuration from %s: %s", source, conf_meta
                )
                raise error

    if "models" in json_dict:
        for model_meta in json_dict["models"]:
            LOGGER.debug("Registering model %s", model_meta["name"])
            build_model_class(model_meta, register=register)


def load_json(path: str, configuration_class):
    """load a list of collector configuration from a json file

    Args:
        path (str): path a json file

    Returns:
        typing.List[CollectorConfiguration]: list of configuration instances
    """
    LOGGER.info("Loading configuration file: %s", path)
    with open(path, encoding="UTF-8") as conf_fd:
        json_dict = json.load(conf_fd)

    yield from load_json_dict(json_dict, configuration_class, path)


def get_model(name):
    """get a DAO class

    Args:
        name (str): the name of the class

    Returns:
        class: DAO class
    """
    if not name in MODEL_DICT:
        LOGGER.error("No model registed: %s", name)
    return MODEL_DICT[name]


def add_model(model_class):
    """register a new model class

    Args:
        name (str): class name
        model_class (class object): implementation

    Raises:
        ValueError: if class is already registered
    """
    if model_class.__name__ in MODEL_DICT:
        LOGGER.warning("Model %s is already defined", model_class.__name__)

    MODEL_DICT[model_class.__name__] = model_class


def find_configurations(directory_path: str, ext=".json") -> list[str]:
    """Look up a directory recursively for configuration files

    Args:
        directory_path (str): directory
        ext (str, optional): configuration extension. Defaults to ".json".

    Returns:
        list[str]: _description_
    """
    result = []
    for root, dirs, files in os.walk(directory_path):
        # filter out hidden files and dirs
        files = [name for name in files if not name[0] == "."]

        dirs[:] = [name for name in dirs if not name[0] == "."]

        result.extend(
            [
                os.path.join(root, path)
                for path in files
                if os.path.splitext(path)[1] == ext
            ]
        )

    return result
