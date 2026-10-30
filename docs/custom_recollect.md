# Custom recollects

A custom recollect replays an interface over a window of dates with a bounded set of
overridden keys, and optionally with an archive choice. The operator worker runs it
with the interface's live entry, copied at run time, with the overrides applied to the
copy only: the entry in the index is never changed. The live entry is resolved when the
replay runs, not when the request is made, so a change of the live entry between the two
is picked up (every key that is not overridden stays live).

This page describes the rules enforced by `maas_collector.rawdata.operator.overrides`.

## Overridable keys

| collector class | key | rule |
| --- | --- | --- |
| `ODataCollectorConfiguration` | `odata_query_filter` | a string that keeps the replay window (below) |
| | `download_file_pattern` | a list of non-empty strings (`[]` replays the metadata only) |
| | `product_per_page` | an integer from 1 to the live value or 1000, whichever is higher |
| | `interface_credentials` | the name of a credential entry that exists for the service; not a backup's entry (`backup-...`) |
| `JiraExtendedCollectorConfiguration` | `jql_query` | a string that keeps the replay window (below) |
| | `interface_credentials` | as above |

Every other key stays live: identity, model, routing, exchange, extractor, URLs and
hosts, `custom_query_suffix`, offsets, backups. A key outside the table is refused,
naming it. A class that is not in the table has no overridable key.

## The replay window stays

A custom query is a string with placeholders that the collector fills in with the start
and end of the replay. Some structure is checked, whatever the whitespace and the case of
the keywords (`AND`, `Or`):

- **odata:** the top-level `and` terms must include `<date field> ge
  {publication_start_date}` and `<date field> le {publication_end_date}`. The date field
  is the one of the entry (`filter_date_v4`, `PublicationDate` by default). There must be
  no `or` at parenthesis depth 0 outside quoted strings. `&`, `#` and `?` are refused
  anywhere: they would end the `$filter` in the request URL.
- **jira:** the clause `updated > {date_criteria}` must be one top-level `AND` term, and
  there must be no `OR` at depth 0 outside quotes before any `ORDER BY`.
- **both:** the only replacement fields are the placeholders of the language, with no
  attribute, index, conversion or format spec; a literal brace is written `{{` or `}}`.
  Quotes and parentheses must be balanced.

Accepted forms:

```text
PublicationDate ge {publication_start_date} and PublicationDate le {publication_end_date} and contains(Name,'TYPE_A')
(Collection/Name eq 'S1' or Collection/Name eq 'S2') and PublicationDate ge {publication_start_date} and PublicationDate le {publication_end_date}

project = X AND updated > {date_criteria} ORDER BY updated ASC
```

## The archive choice

A recollect of an odata entry may choose where the products are archived:

| key | meaning |
| --- | --- |
| `backup` | the name of a backup (mandatory) |
| `backup_version` | the version of the backup definition to use; `null` pins the latest when the replay starts |
| `duplicates` | `skip` (default) or `replace_if_changed` |
| `force` | `true` to archive what the backup already holds (default `false`) |

Any other key is refused: the rule's fields and download are those of the entry.
An archive choice on another class of entry is refused.
