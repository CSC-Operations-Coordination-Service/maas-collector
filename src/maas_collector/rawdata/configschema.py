"""The configuration of maas-collector, described by itself.

`configuration_schema()` reflects the configuration classes of the collectors, the
extractors and the shapes of the other parts of a file into one JSON-able structure,
for the configuration builder. Nothing is written by hand that the code can say: the
keys, types, defaults and required keys come from the dataclasses, the secret keys
from their `sensitive` marks. What only a person knows (the group and description of
a key, the keys no field marks) is in a reviewed catalog or set of this module.
"""

import dataclasses
import json
import types
import typing
from typing import Any, Iterable

from maas_collector.backup.named import SECRET_FIELDS
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitorCollectorConfiguration,
)
from maas_collector.rawdata.implementation import _CONFIG_TO_COLLECTOR_DICT
from maas_collector.rawdata.operator.overrides import OVERRIDABLE_KEYS
from maas_collector.rawdata.operator.replay import REPLAY_MODULES

UNMARKED_SECRET_KEYS = frozenset({"oauth_basic_credential"})
"""credentials that no field marks `sensitive`"""

NOT_SECRET_KEYS = frozenset(
    {
        "interface_credentials",
        "no_credential",
        "token_url",
        "token_field_header",
        "client_keyfile",
        "routing_key",
        "file_routing_key",
        "s3_max_keys",
        "credentials",
    }
)
"""reviewed names that look like credentials and hold none: `interface_credentials`
and `credentials` name a credential entry, `client_keyfile` is a path"""

TYPE_OVERRIDES = {"download_file_pattern": "string_list"}
"""types by key name, applied after the annotation mapping"""

GROUPS = (
    "identity",
    "remote_query",
    "download",
    "extraction",
    "backup",
    "archive",
    "monitoring",
    "other",
)

CLASS_KEY_DESCRIPTION = "configuration class of the entry"
DEFAULT_ENTRY_CLASS = "FileCollectorConfiguration"

# keys the dataclass annotation cannot tell the type of
_ANY_KEYS = frozenset({"model", "backup"})
_OBJECT_KEYS = frozenset({"extractor"})
# fields a file does not hold
_NOT_KEYS = frozenset({"model_meta"})
_CREDENTIAL_WORDS = ("password", "secret", "token", "credential", "key")


def config_classes() -> dict[str, type]:
    """The configuration classes an entry can build with, by class name"""
    classes = {
        name: collector.CONFIG_CLASS
        for name, collector in _CONFIG_TO_COLLECTOR_DICT.items()
    }
    classes[InterfaceMonitorCollectorConfiguration.__name__] = (
        InterfaceMonitorCollectorConfiguration
    )
    return classes


def _file_fields(configuration_class: type) -> list:
    """The fields a configuration file can set: `init=False` ones are runtime state
    (the resolved backup, its credentials) and `model_meta` is derived"""
    return [
        field
        for field in dataclasses.fields(configuration_class)
        if field.init and field.name not in _NOT_KEYS
    ]


def _sensitive_keys() -> frozenset:
    return (
        frozenset(
            field.name
            for configuration_class in config_classes().values()
            for field in _file_fields(configuration_class)
            if field.metadata.get("sensitive")
        )
        | UNMARKED_SECRET_KEYS
        | frozenset(SECRET_FIELDS)
    )


SECRET_KEYS: frozenset = _sensitive_keys()
"""every key that holds a secret, whatever the class: the name decides"""


def looks_like_credential(name: str) -> bool:
    """Whether a field name suggests a credential"""
    lowered = name.lower()
    return any(word in lowered for word in _CREDENTIAL_WORDS)


def unclassified_credential_fields(classes: Iterable[type] | None = None) -> list[str]:
    """`<Class>.<field>` of every field that looks like a credential and is in neither
    `SECRET_KEYS` nor `NOT_SECRET_KEYS`: a new one must be classified by a person"""
    if classes is None:
        classes = config_classes().values()
    found = set()
    for configuration_class in classes:
        for field in _file_fields(configuration_class):
            if (
                looks_like_credential(field.name)
                and field.name not in SECRET_KEYS
                and field.name not in NOT_SECRET_KEYS
            ):
                found.add(f"{configuration_class.__name__}.{field.name}")
    return sorted(found)


def entry_class_name(entry: dict, service_class: str | None) -> str:
    """The class a collector entry builds with: its service's class, else its own
    `class`, else the file collector's (the collector of a service builds every entry
    of the service with its own class)"""
    return service_class or entry.get("class") or DEFAULT_ENTRY_CLASS


def service_class_of(entries: Iterable[dict]) -> str | None:
    """The one class the entries of a service name, None when none does

    Raises:
        ValueError: several classes are named
    """
    named = {entry["class"] for entry in entries if entry.get("class")}
    if len(named) > 1:
        raise ValueError(f"several collector classes in one service: {sorted(named)}")
    return next(iter(named), None)


def json_type(annotation: Any) -> str:
    """The schema type of a dataclass annotation"""
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin in (typing.Union, types.UnionType):
        options = [arg for arg in args if arg is not type(None)]
        return json_type(options[0]) if len(options) == 1 else "any"
    if annotation is bool:
        return "boolean"
    if annotation is int:
        return "integer"
    if annotation is float:
        return "number"
    if annotation is str:
        return "string"
    if annotation in (list, typing.List) or origin is list:
        return "string_list" if args == (str,) else "list"
    if annotation in (dict, typing.Dict) or origin is dict:
        return "object"
    return "any"


def json_default(field: dataclasses.Field) -> Any:
    """The default of a field when it is JSON, else None"""
    if field.default is not dataclasses.MISSING:
        value = field.default
    elif field.default_factory is not dataclasses.MISSING:
        value = field.default_factory()
    else:
        return None
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return None
    return value


def key_descriptor(field: dataclasses.Field, class_name: str, hints: dict) -> dict:
    """The descriptor of one key of a configuration class"""
    if field.name in _ANY_KEYS:
        kind = "any"
    elif field.name in _OBJECT_KEYS:
        kind = "object"
    else:
        kind = TYPE_OVERRIDES.get(field.name) or json_type(
            hints.get(field.name, field.type)
        )
    has_default = (
        field.default is not dataclasses.MISSING
        or field.default_factory is not dataclasses.MISSING
    )
    return {
        "type": kind,
        "default": json_default(field),
        "required": not has_default and field.name not in (_ANY_KEYS | _OBJECT_KEYS),
        "sensitive": field.name in SECRET_KEYS,
        "overridable": field.name in OVERRIDABLE_KEYS.get(class_name, ()),
        "group": None,
        "description": None,
    }


def class_key(group: str | None = None, description: str | None = None) -> dict:
    """The pseudo-key `class`: not a field, yet every entry names its class"""
    return {
        "type": "string",
        "default": None,
        "required": False,
        "sensitive": False,
        "overridable": False,
        "group": group,
        "description": description,
    }


def collector_classes() -> dict[str, dict]:
    """The `collector_classes` part of the schema"""
    described = {}
    for name, configuration_class in sorted(config_classes().items()):
        try:
            hints = typing.get_type_hints(configuration_class)
        except (NameError, TypeError):
            hints = {}
        keys = {
            field.name: key_descriptor(field, name, hints)
            for field in _file_fields(configuration_class)
        }
        keys["class"] = class_key("identity", CLASS_KEY_DESCRIPTION)
        described[name] = {
            "replayable": name in REPLAY_MODULES,
            "archivable": bool(getattr(configuration_class, "ARCHIVABLE", False)),
            "keys": keys,
        }
    return described
