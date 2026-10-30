# Configuration schema

`maas_collector.rawdata.configschema.configuration_schema()` describes the configuration
of maas-collector by itself, as one JSON-able dictionary, for the configuration builder
of the operator application. It reflects the code (the configuration dataclasses, the
extractor constructors) instead of repeating it, so it cannot drift from what the
collectors accept: a test builds every file of `tests/conf` against it.

## Shape

| key | content |
| --- | --- |
| `version` | the maas-collector version that produced the schema |
| `collector_classes` | by configuration class name: `replayable`, `archivable` and `keys` |
| `extractor_classes` | by extractor class name: its `args`, and `extra_args: true` when its constructor takes `**kwargs` |
| `backup_types` | `BackupS3` and `BackupLocal`: the `keys` of a backup definition (`backups/<name>.json`) |
| `archive_rule` | the four editable `keys` of an entry's `archive` (`backup`, `duplicates`, `fields`, `download`) |
| `secret_keys` | the sorted names of every key that holds a secret |
| `window_rules` | the placeholders and the clause a custom query must keep (see [custom recollects](custom_recollect)) |
| `shapes` | the parts of a file that are not collector entries: `extractor` (keys, kinds of `attr_map` and converter), `model` (forms, field types, definition keys) and `amqp` (`chunk_size`) |

A key descriptor has `type` (`string`, `integer`, `number`, `boolean`, `string_list`,
`list`, `object` or `any`), `default` (`null` when the default is not JSON), `required`,
`sensitive`, `overridable` (a custom recollect may change it), `group`, `description`
and, for an enumerated key, `choices`. Every class of `collector_classes` has the
pseudo-key `class`, the configuration class of the entry, which is not a dataclass field.
Fields a file cannot set (the resolved backup and its credentials) are not keys.

## The class an entry builds with

A collector builds every entry of its service with its own configuration class, whatever
the `class` of the entry says: `entry_class_name(entry, service_class)` is the service's
class, else the entry's `class`, else `FileCollectorConfiguration`. `service_class_of`
finds the class a service's entries name.

## Secret keys

`SECRET_KEYS` is the union of the dataclass fields marked `sensitive`, of the credentials
no field marks (`UNMARKED_SECRET_KEYS`, today `oauth_basic_credential`) and of the secret
fields of a backup. The **name** decides: a name that is secret in one class is sensitive
in every class that has it, so the builder never shows or sends back such a value.

A new field whose name looks like a credential (it contains `password`, `secret`,
`token`, `credential` or `key`) fails the test `unclassified_credential_fields() == []` until a person classifies it: mark the field `sensitive` (or add the
name to `UNMARKED_SECRET_KEYS`) when it holds a secret, or add the name to
`NOT_SECRET_KEYS` when it does not (a path, the name of a credential entry, a routing
key).

## Groups and descriptions

`CATALOG` gives each key of the odata, jira and file entries a group (`identity`,
`remote_query`, `download`, `extraction`, `backup`, `archive`, `monitoring`, `other`) and a
description; `CLASS_CATALOG` overrides it for one class. A key of another class has them
only once someone writes them: the descriptor keeps `null`.
