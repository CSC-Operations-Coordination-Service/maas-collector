# Continuous archive

An odata collector entry may ask for every product it indexes to be archived, once,
into a [named backup](backup). The archive works in two stages:

1. **The collector** indexes the documents as usual, then publishes **transfer
   requests** naming the documents of an archived entry.
2. **The transfer stage** takes the requests, downloads each product, checks it,
   decides whether it is already archived, uploads it to the backup and writes one
   report per destination.

The collector's own download of files and its live collection without `archive` are
unchanged.

## Archive rules

An odata entry declares `archive`:

```json
{
    "interface_name": "ITF_A",
    "archive": {
        "backup": "archive-a",
        "duplicates": "skip",
        "fields": {
            "product_id": "product_id",
            "product_name": "product_name",
            "size": "content_length",
            "checksum": "checksum",
            "checksum_algorithm": "MD5",
            "publication_date": "publication_date"
        },
        "download": {"url": "https://bytes.example/odata/v1/Products({id})/$value", "quote_id": false}
    }
}
```

| key | default | description |
| --- | --- | --- |
| `backup` | - | mandatory name of the backup (`backups/<name>.json`) |
| `duplicates` | `skip` | `skip` or `replace_if_changed` |
| `fields` | see above | the field of the raw document holding each value of the product; only the keys you change are needed |
| `download.url` | the collector's own form | template with `{id}` |
| `download.quote_id` | `true` | quote the id in the template: `'<id>'` |

`backup_version` and `force` are also read from the rule. They are written by a
[custom recollect](custom_recollect) into its copy of the entry from the archive choice
of the operation, and are not meant to be edited.

### Duplicates

- `skip`: a product already archived (the last archive report of its backup, interface,
  product name and destination is `OK` or `REPLACED`) is not transferred again. When its
  checksum differs from the archived one, a conflict is recorded.
- `replace_if_changed`: the same checksum is skipped, another checksum is transferred
  again and replaces the archived object.
- A forced transfer always transfers and replaces.
- A product without a checksum can only be compared by name: `replace_if_changed`
  behaves like `skip`, and no conflict can be detected.

### Fields

`fields` names where the raw document holds the id, name, size, checksum and
publication date of the product, and the algorithm of the checksum. A missing size or
checksum (or `0`) is not checked. `checksum_algorithm` is any name `hashlib` knows
(`MD5`, `SHA-256`, `SHA3-256`...); a name it does not know is refused when the
configuration loads.

### Download URL

Without `download.url`, the product is fetched from
`<odata_product_url>/odata/v1/Products('<id>')/$value`, as the collector builds its own
URLs. A template replaces `{id}` with the id, quoted (`'<id>'`, a quote doubled) unless
`quote_id` is false. Use it when the bytes are served by another host than the catalogue.

### Rules

- Only odata entries can archive: another class with an `archive` key is refused.
- The archive backup is never the entry's own `backup`.
- The collector needs its service (`CONFIG_SERVICE`) to publish requests.

## Transfer requests, wire format

- Exchange `maas-transfer-exchange` (topic, durable), routing key
  `maas.transfer.<service>`, queue `maas-transfer-<service>`, declared by the transfer
  stage.
- Requests are published mandatory, persistent and confirmed: a request no queue
  accepts is an error, not a silent loss.

```json
{"service": "svc", "interface_name": "ITF_A",
 "documents": [{"id": "d1", "index": "raw-data-a-2026"}],
 "backup": "archive-a", "backup_version": null, "duplicates": "skip", "force": false,
 "operation_id": "", "requested_by": "", "origin": "live"}
```

`documents` holds several documents (the id and the index of each). `origin` is `live`,
`recollect` or `download`; `operation_id` is empty for the live flow. Unknown fields are
ignored on reading, and a malformed body is dropped by the stage.

## Transfer requests

After a run indexes documents through an entry with an `archive` rule, the collector
hands the documents to its requester, which publishes requests of at most 10 documents:

- **Live**: the documents created or updated by the run. A request the broker refuses
  is kept (10 000 at most, then the oldest is dropped with a `CRITICAL` log: its
  products show as gaps) and published again at the next loop.
- **Replay** (a recollect): every document the run extracted, created, updated or
  unchanged, because a recollect over products already indexed must still archive them.
  The ordinary product messages are not sent again. A request that cannot be sent is
  counted, never retried, and fails the replay.

Requests are published mandatory and confirmed, so a request no queue accepts (no
transfer stage of the service has declared its queue) is an error, not a silent loss.
For an operation (a replay with an operation id), the collector first writes one
`REQUESTED` report per product, a document that is not in the index included, then
publishes the request: the operation knows what it sent.

The collector needs its service to publish: set `CONFIG_SERVICE` (`--config-service`).
An archive rule without it makes the configuration fail to load.

A replay exits with status 69 (`TRANSFER_FAILED_EXIT_CODE`) when requests could not be
sent, and prints `transfer requests failed: <count> documents` as its last line: see
[replay](replay) for the order of the exit statuses.
