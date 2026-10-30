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

## The transfer stage

```bash
python -m maas_collector.rawdata.cli.transfer --config-service <service>
```

One transfer stage per collector service, started before the archive is used: it
declares the exchange `maas-transfer-exchange` and the queue `maas-transfer-<service>`
when it starts, so the requests published while it is down are routed and wait. A
deployment without a stage shows up as an unroutable publish (a failed replay, a live
request kept for the next loop), never as a silent loss. The stage is a collector: it
loads the configuration and the credentials of its service the same way, restarts on a
new configuration, and stops on SIGTERM.

It serves one product at a time per replica: scale it with replicas, mind the rate
limit of the catalogue. Its options are the collectors' (`CONFIG_*`, `CREDENTIAL_*`,
`VAULT_*`, `ES_*`, `AMQP_*`, `WORKING_DIR`, `WATCH_PERIOD`, the idle poll period)
and:

| variable | option | default | meaning |
| --- | --- | --- | --- |
| `TRANSFER_MAX_ATTEMPTS` | `--transfer-max-attempts` | 3 | attempts of a download, of an upload per destination, of a report write |
| `TRANSFER_RETRY_DELAY` | `--transfer-retry-delay` | 5.0 | seconds before the second attempt, doubled after, at most 60 |
| `TRANSFER_MAX_DOWNLOAD_SECONDS` | `--transfer-max-download-seconds` | 900 | seconds a download may take |

A request is **acknowledged when it is taken**, and no connection is held while it is
served. A stop (a deploy, SIGTERM) reports nothing for the product in flight and
publishes the documents it left again as one request, so nothing is lost; a crash
leaves gaps or pending products (see "Gaps"). A malformed request is logged and
dropped.

### What it does for a request

For each product of a request, in order:

1. It reads the raw document, resolves the entry and its rule from the live
   configuration (URLs and credentials are never overridden), and the backup at the
   version the request names (the latest for the live flow).
2. Unless forced, it applies `duplicates` to the archive reports of the product.
3. It downloads the product, streamed in chunks to a working directory made with
   `mkdtemp` (never built from the id or the name of the product), and checks its size
   and checksum. The checksums are compared in lower case, blanks ignored.
4. It uploads the product to every destination that still misses it, a failed
   destination being tried again alone, and writes one report per destination.

The object key is `<directory>/<interface_name>/[YYYY/MM/DD/]<product file name>`, the
date being the UTC day of the publication date of the product: a replacement
overwrites the object the consumers read. With `calendar_tree`, a product without a
usable publication date is a `KO`, never filed under today's date. A product name that
is not a bare file name (`../P1.zip`, `a/P1.zip`) is a `KO`.

### Download rules

- The token is sent to the configured host only. A redirect is followed (3 hops at
  most) **without** any authentication header. A `401` or `403` after a redirect is a
  permanent `KO` naming the host: set `download.url` of the archive rule to that host
  when the bytes are served by another host than the catalogue.
- A `401` or `403` from the configured host refreshes the token once.
- Bounded retries everywhere: a network error, a `5xx`, a `408` or `429`, a size or
  checksum mismatch are tried again with a growing delay, then end as a `KO` report.
  Another `4xx` is permanent at once. The request is acknowledged either way: nothing
  requeues forever.

### Reports and statuses

Reports go to the index of the backup reports with `origin` `archive`.

| status | meaning |
| --- | --- |
| `REQUESTED` | an operation asked for the product (written by the publisher of the operation) |
| `OK` | written to the destination |
| `REPLACED` | written over an object already archived there |
| `SKIPPED` | not written: already archived, or a conflict (`error_type` `ArchiveConflict`) |
| `KO` | not written, with `error_type` and `details` |

An operation's skips are reported, live skips are not. A document that is not in the
index, an unknown interface or backup end every product of the request as `KO`.

### Conflicts

A product archived in a backup and seen again under the same name with another
checksum (`duplicates: skip`), or whose downloaded bytes are not the archived ones when
a missing mirror was to be filled, is a conflict: nothing is written. It is recorded in
the index `maas-archive-conflict`, one document per `(backup, interface, product
name)`, `OPEN` until a later `OK` or `REPLACED` transfer of the product resolves it
(`RESOLVED`). For an operation, every destination that was not written has a `SKIPPED`
report with the error type `ArchiveConflict`, so that an operation never stays pending.

### Triggering consumers (not implemented)

Nothing yet tells a consumer that a product has arrived. Three ways, none coded:

- **An event per archived product**: after an `OK` or `REPLACED` report, the stage could
  publish on a topic exchange (for example `maas-archive-exchange`, routing key
  `archived.<backup>.<interface>`) the backup, the interface, the product id and name,
  the object keys, size, MD5, `replaced`, `operation_id` and the time. Each consumer
  binds its own queue. The report is written when the object is known complete, which
  makes it the natural hook.
- **The report index**: a consumer can poll the index of the backup reports for the new
  `OK` and `REPLACED` archive documents of a backup (by `ingestionTime`), with no change
  to the stage.
- **Storage notifications**: bucket notifications, if the storage offers them (to be
  checked for the actual endpoints).

Whichever is chosen: emit only once the object is complete, and make a replacement
distinguishable from a first arrival.
