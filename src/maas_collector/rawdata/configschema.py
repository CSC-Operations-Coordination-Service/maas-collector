"""The configuration of maas-collector, described by itself.

`configuration_schema()` reflects the configuration classes of the collectors, the
extractors and the shapes of the other parts of a file into one JSON-able structure,
for the configuration builder. Nothing is written by hand that the code can say: the
keys, types, defaults and required keys come from the dataclasses, the secret keys
from their `sensitive` marks. What only a person knows (the group and description of
a key, the keys no field marks) is in a reviewed catalog or set of this module.
"""

import dataclasses
import inspect
import json
import types
import typing
from typing import Any, Iterable

import opensearchpy

import maas_collector
from maas_collector.backup import factory_type_dict
from maas_collector.backup.named import SECRET_FIELDS
from maas_collector.rawdata import extractor as extractors
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitorCollectorConfiguration,
)
from maas_collector.rawdata.implementation import _CONFIG_TO_COLLECTOR_DICT
from maas_collector.rawdata.extractor.base import BaseExtractor
from maas_collector.rawdata.operator.overrides import (
    DUPLICATES,
    JIRA_CLAUSE,
    OVERRIDABLE_KEYS,
    ODATA_PLACEHOLDERS,
)
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


def _entry(group: str, description: str) -> dict:
    return {"group": group, "description": description}


CATALOG: dict[str, dict] = {
    # identity
    "class": _entry("identity", CLASS_KEY_DESCRIPTION),
    "interface_name": _entry(
        "identity", "name of the interface, unique in a service (no @)"
    ),
    "interface_credentials": _entry(
        "identity", "name of the credential entry of the interface"
    ),
    "enabled": _entry(
        "identity", "false keeps the entry in the configuration but out of every run"
    ),
    # remote query: where and how the remote service is queried
    "odata_product_url": _entry(
        "remote_query", "base URL of the OData service (legacy name of product_url)"
    ),
    "product_url": _entry("remote_query", "base URL of the remote service"),
    "odata_version": _entry(
        "remote_query", "OData protocol version (legacy name of protocol_version)"
    ),
    "protocol_version": _entry("remote_query", "protocol version of the service"),
    "odata_entities": _entry("remote_query", "OData entity set that is queried"),
    "odata_entity_location": _entry(
        "remote_query", "path of the OData service below the URL"
    ),
    "odata_query_filter": _entry(
        "remote_query",
        "OData $filter with the {publication_start_date} and {publication_end_date}"
        " placeholders of the collected window",
    ),
    "odata_query_order_by": _entry("remote_query", "OData $orderby of the query"),
    "custom_query_suffix": _entry(
        "remote_query", "text added at the end of the query as it is"
    ),
    "product_per_page": _entry("remote_query", "number of products asked per page"),
    "refresh_interval": _entry(
        "remote_query", "minutes between two collections of the interface"
    ),
    "odata_start_offset": _entry(
        "remote_query", "minutes the start of a live window is moved back"
    ),
    "end_date_time_offset": _entry(
        "remote_query", "minutes the end of a live window stays behind the clock"
    ),
    "max_time_window": _entry(
        "remote_query", "longest window, in minutes, asked in one query"
    ),
    "date_attr": _entry(
        "remote_query", "attribute of the extracted product that holds its date"
    ),
    "filter_date_v3": _entry("remote_query", "date field of the window with OData v3"),
    "filter_date_v4": _entry(
        "remote_query", "date field of the window with OData v4 (PublicationDate)"
    ),
    "auth_method": _entry("remote_query", "authentication method of the service"),
    "client_username": _entry("remote_query", "user name for the service"),
    "client_password": _entry("remote_query", "password for the service (secret)"),
    "token": _entry("remote_query", "access token for the service (secret)"),
    "token_url": _entry("remote_query", "URL where the access token is requested"),
    "token_field_header": _entry(
        "remote_query", "header that carries the access token"
    ),
    "client_id": _entry("remote_query", "OAuth client identifier"),
    "client_secret": _entry("remote_query", "OAuth client secret (secret)"),
    "scope": _entry("remote_query", "OAuth scope of the token request"),
    "grant_type": _entry("remote_query", "OAuth grant type of the token request"),
    "oauth_basic_credential": _entry(
        "remote_query", "basic credential of the token request (secret)"
    ),
    "auth_timeout": _entry("remote_query", "seconds the token request may take"),
    "no_credential": _entry(
        "remote_query", "true when the service needs no credential"
    ),
    "http_query_params": _entry(
        "remote_query", "extra parameters added to every request"
    ),
    "disable_insecure_request_warning": _entry(
        "remote_query", "true to silence the warning about unverified TLS"
    ),
    "end_point": _entry("remote_query", "URL of the Jira service"),
    "jql_query": _entry(
        "remote_query",
        "JQL query with the {date_criteria} placeholder of the collected window",
    ),
    "username": _entry("remote_query", "user name for Jira"),
    "password": _entry("remote_query", "password for Jira (secret)"),
    "proxy_login": _entry("remote_query", "user name of the proxy"),
    "proxy_password": _entry("remote_query", "password of the proxy (secret)"),
    # download: which files are fetched and kept
    "download_file_pattern": _entry(
        "download",
        "patterns of the files downloaded for each product, none: metadata only",
    ),
    "parent_file_pattern": _entry(
        "download", "pattern of the files of the parent product"
    ),
    "list_of_files_to_retrieve": _entry(
        "download", "names of the files retrieved instead of querying by date"
    ),
    "list_of_files_to_retrieve_per_query": _entry(
        "download", "number of those files asked per query"
    ),
    "list_of_files_to_retrieve_query": _entry(
        "download", "query template of those files, with one {} for the name"
    ),
    "file_pattern": _entry("download", "pattern of the files the entry takes"),
    "file_routing_key": _entry(
        "download", "routing key of the messages about downloaded files"
    ),
    "ingest_attachements": _entry("download", "true to ingest the attachments"),
    "attachement_prefix": _entry(
        "download", "true to prefix the attachment name with its issue"
    ),
    "attachment_extra_fields": _entry(
        "download", "record field -> attachment attribute stamped on each record"
    ),
    "attachment_date_fields": _entry(
        "download", "of those fields, the ones that are dates, normalised to ZULU"
    ),
    # extraction: what a collected file becomes
    "model": _entry(
        "extraction", "name of the model of the documents, or its definition"
    ),
    "extractor": _entry(
        "extraction", "extractor class and arguments that read the files"
    ),
    "id_field": _entry(
        "extraction", "field, or list of fields, that make the id of a document"
    ),
    "force_update": _entry(
        "extraction", "true to overwrite a document that exists (else: duplicate)"
    ),
    "store_meta": _entry("extraction", "metadata fields stored with each document"),
    "routing_key": _entry(
        "extraction", "routing key of the message sent for each document"
    ),
    "exchange_name": _entry(
        "extraction", "exchange of those messages, empty for the collector's"
    ),
    # backup, archive, monitoring
    "backup": _entry(
        "backup", "name of a backup, or an inline backup block, for the files"
    ),
    "archive": _entry(
        "archive", "where and how the products of the interface are archived"
    ),
    "no_probe": _entry("monitoring", "true to leave the interface out of the probes"),
}
"""group and description of a key, whatever the class"""

CLASS_CATALOG: dict[str, dict[str, dict]] = {
    "JiraExtendedCollectorConfiguration": {
        "refresh_interval": _entry(
            "remote_query", "minutes between two collections of the interface"
        ),
        "token": _entry("remote_query", "access token of Jira (secret)"),
    },
}
"""a description that differs for one class wins over the catalog"""


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
        keys = {}
        for field in _file_fields(configuration_class):
            descriptor = key_descriptor(field, name, hints)
            descriptor.update(
                CLASS_CATALOG.get(name, {}).get(field.name)
                or CATALOG.get(field.name)
                or {}
            )
            keys[field.name] = descriptor
        keys["class"] = class_key(**CATALOG["class"])
        described[name] = {
            "replayable": name in REPLAY_MODULES,
            "archivable": bool(getattr(configuration_class, "ARCHIVABLE", False)),
            "keys": keys,
        }
    return described


BACKUP_CATALOG = {
    "name": "name of the backup, [a-z0-9][a-z0-9-]*, at most 64 characters",
    "description": "what the backup is for",
    "type": "BackupS3 or BackupLocal",
    "directory": "folder, or prefix in the bucket, the files are written to",
    "calendar_tree": "true to put the files in a year/month/day tree",
    "enable_gzip": "true to compress the files",
    "report_model": "model of the reports of the transfers, none: no reports",
    "buckets": "S3 buckets the files are written to",
    "s3_endpoint_url": "URL of the S3 service",
    "s3_region": "region of the S3 service",
    "s3_signature_version": "signature version of the S3 requests",
    "credentials": "name of the credential entry that holds the S3 keys (backup-...)",
}
_BACKUP_REQUIRED = {"name", "type", "directory"}
_BACKUP_REQUIRED_S3 = {"buckets", "credentials"}
# the definition keys that are not fields of the backup configuration
_BACKUP_DEFINITION_ONLY = {
    "BackupS3": ("name", "description", "credentials"),
    "BackupLocal": ("name", "description"),
}
_BACKUP_NOT_KEYS = {
    "interface_name",
    "interface_credentials",
    "backup_name",
    "type",
} | set(SECRET_FIELDS)


def _backup_key(kind: str, default: Any, required: bool, name: str) -> dict:
    return {
        "type": kind,
        "default": default,
        "required": required,
        "sensitive": False,
        "overridable": False,
        "group": "backup",
        "description": BACKUP_CATALOG[name],
    }


def backup_types() -> dict:
    """The `backup_types` part of the schema: the definitions a person can write

    Only `BackupS3` and `BackupLocal`: `BackupSFTP` exists in the factory, and the
    builder does not offer it.
    """
    described = {}
    for type_name in ("BackupS3", "BackupLocal"):
        configuration_class = factory_type_dict[type_name].CONFIGURATION_CLASS
        hints = typing.get_type_hints(configuration_class)
        keys = {
            "name": _backup_key("string", None, True, "name"),
            "description": _backup_key("string", None, False, "description"),
            "type": _backup_key("string", type_name, True, "type"),
        }
        keys["type"]["choices"] = [type_name]
        for field in dataclasses.fields(configuration_class):
            if field.name in _BACKUP_NOT_KEYS:
                continue
            keys[field.name] = _backup_key(
                json_type(hints.get(field.name, field.type)),
                json_default(field),
                field.name in _BACKUP_REQUIRED
                or (type_name == "BackupS3" and field.name in _BACKUP_REQUIRED_S3),
                field.name,
            )
        if type_name == "BackupS3":
            keys["credentials"] = _backup_key("string", None, True, "credentials")
        described[type_name] = {"keys": keys}
    return described


ARCHIVE_RULE_FIELDS = {
    "product_id": "product_id",
    "product_name": "product_name",
    "size": "content_length",
    "checksum": "checksum",
    "checksum_algorithm": "MD5",
    "publication_date": "publication_date",
}


def _rule_key(kind: str, default: Any, description: str, **more) -> dict:
    return {
        "type": kind,
        "default": default,
        "required": False,
        "sensitive": False,
        "overridable": False,
        "group": "archive",
        "description": description,
        **more,
    }


def archive_rule() -> dict:
    """The `archive_rule` part of the schema: the four keys of an entry's `archive`

    `backup_version` and `force` are written into the copy of a recollect by the
    operator worker from the archive choice: they are never edited, so not described.
    """
    keys = {
        "backup": _rule_key("string", None, "name of the backup that archives"),
        "duplicates": _rule_key(
            "string",
            "skip",
            "what to do with a product the backup already holds: skip it, or replace"
            " it when its checksum changed",
            choices=list(DUPLICATES),
        ),
        "fields": _rule_key(
            "object",
            dict(ARCHIVE_RULE_FIELDS),
            "name of the document field that holds each value of the rule",
        ),
        "download": _rule_key(
            "object",
            {"url": None, "quote_id": True},
            "url: download template with {id}, none: the collector's own form;"
            " quote_id: quote the id in it",
        ),
    }
    keys["backup"]["required"] = True
    return {"keys": keys}


def _arg_type(parameter: inspect.Parameter) -> str:
    """The schema type of a constructor parameter: its annotation, else its default"""
    if parameter.name in ("attr_map", "converter_map"):
        return "object"
    if parameter.annotation is not inspect.Parameter.empty:
        return json_type(parameter.annotation)
    default = parameter.default
    if isinstance(default, bool):
        return "boolean"
    if isinstance(default, int):
        return "integer"
    if isinstance(default, str):
        return "string"
    return "any"


def _arg_default(parameter: inspect.Parameter) -> Any:
    if parameter.default is inspect.Parameter.empty:
        return None
    try:
        json.dumps(parameter.default)
    except (TypeError, ValueError):
        return None
    return parameter.default


def extractor_classes() -> dict:
    """The `extractor_classes` part of the schema: the arguments of each extractor,
    read from its constructor. `extra_args` is true when the constructor takes
    `**kwargs`."""
    described = {}
    for name in sorted(dir(extractors)):
        extractor_class = getattr(extractors, name)
        if (
            not inspect.isclass(extractor_class)
            or not issubclass(extractor_class, BaseExtractor)
            or extractor_class is BaseExtractor
        ):
            continue
        args, extra = {}, False
        for parameter in inspect.signature(
            extractor_class.__init__
        ).parameters.values():
            if parameter.name == "self":
                continue
            if parameter.kind is inspect.Parameter.VAR_KEYWORD:
                extra = True
                continue
            if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
                continue
            args[parameter.name] = {
                "type": _arg_type(parameter),
                "default": _arg_default(parameter),
                "required": parameter.default is inspect.Parameter.empty,
                "description": None,
            }
        described[name] = {"args": args}
        if extra:
            described[name]["extra_args"] = True
    return described


def _shape_key(kind: str, required: bool, description: str, group: str) -> dict:
    return {
        "type": kind,
        "default": None,
        "required": required,
        "sensitive": False,
        "overridable": False,
        "group": group,
        "description": description,
    }


def shapes() -> dict:
    """The `shapes` part of the schema: the parts of a file that are not collector
    entries. The `models` list of a file is described by `model.definition`."""
    field_types = {"Date"} | {
        name
        for name, value in vars(opensearchpy).items()
        if inspect.isclass(value) and issubclass(value, opensearchpy.Field)
    }
    return {
        "extractor": {
            "keys": {
                "class": _shape_key(
                    "string",
                    True,
                    "extractor class, see extractor_classes",
                    "extraction",
                ),
                "args": _shape_key(
                    "object",
                    False,
                    "arguments of the extractor, see extractor_classes",
                    "extraction",
                ),
                "converter_map": _shape_key(
                    "object",
                    False,
                    "converters by field name, merged into the arguments",
                    "extraction",
                ),
            },
            "attr_map_kinds": [
                "<path string>",
                "{attr, path?}",
                "{python}",
                "{rpython}",
                "{root_path}",
            ],
            "converter_kinds": [
                '{type: "regex", expression, format?}',
                '{type: "python", python}',
            ],
        },
        "model": {
            "forms": ["name", "definition"],
            "field_types": sorted(field_types),
            "definition": {
                "keys": {
                    "name": _shape_key("string", True, "name of the model", "model"),
                    "index": _shape_key(
                        "string", True, "index the documents are written to", "model"
                    ),
                    "fields": _shape_key(
                        "list",
                        True,
                        "list of {name, type}, type being one of field_types",
                        "model",
                    ),
                    "partition_field": _shape_key(
                        "string",
                        False,
                        "date field that partitions the index, or ingestionTime",
                        "model",
                    ),
                    "partition_format": _shape_key(
                        "string",
                        False,
                        "date format of the partition, yearly by default",
                        "model",
                    ),
                }
            },
        },
        "amqp": {
            "keys": {
                "chunk_size": _shape_key(
                    "integer",
                    False,
                    "positive number of messages sent together, by routing key",
                    "other",
                )
            }
        },
    }


def configuration_schema() -> dict:
    """The whole schema, JSON-able: what a configuration file can hold"""
    return {
        "version": maas_collector.__version__,
        "collector_classes": collector_classes(),
        "extractor_classes": extractor_classes(),
        "backup_types": backup_types(),
        "archive_rule": archive_rule(),
        "secret_keys": sorted(SECRET_KEYS),
        "window_rules": {
            "odata": {
                "date_field_key": "filter_date_v4",
                "placeholders": list(ODATA_PLACEHOLDERS),
            },
            "jira": {"clause": JIRA_CLAUSE},
        },
        "shapes": shapes(),
    }
