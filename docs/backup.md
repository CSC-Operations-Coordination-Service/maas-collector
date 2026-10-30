# Backup

A collector configuration may copy every file it ingests to a backup space. The
copy is driven by the `backup` key of the configuration, and it happens **after**
the extracted documents have been written to the database.

There are currently three backup types: `BackupLocal`, `BackupS3` and `BackupSFTP`
(the latter is not implemented yet).

## Usage

```json
{
    "other_collector_config": "...",
    "backup": {
        "type": "BackupLocal",
        "directory": "/tmp/backup"
    }
}
```

Keys common to every type:

| key | default | description |
| --- | --- | --- |
| `type` | - | `BackupLocal`, `BackupS3` or `BackupSFTP` |
| `directory` | - | mandatory root of the destination tree |
| `calendar_tree` | `false` | insert a `YYYY/MM/DD` tree under `directory` |
| `enable_gzip` | `false` | gzip the file before transfer, except for `.xlsx`, `.gzip` and `.zip` |
| `interface_name` | `null` | reported interface, defaults to the one of the collector configuration |
| `report_model` | `null` | model storing the transfer reports, see below |

The destination path is `directory[/YYYY/MM/DD]/<interface name>/<file name>`.

## FileSystem

```json
{
    "...": "...",
    "backup": {
        "type": "BackupLocal",
        "directory": "/tmp/backup"
    }
}
```

## S3

A single file is uploaded to **every** bucket of `buckets`.

```json
{
    "...": "...",
    "backup": {
        "type": "BackupS3",
        "directory": "cdse/sentinel-1/ocn",
        "calendar_tree": true,
        "enable_gzip": false,
        "s3_endpoint_url": "http://minio:9000",
        "s3_access_key": "minioadmin",
        "s3_secret_key": "minioadmin",
        "s3_signature_version": "s3v4",
        "s3_region": "us-east-1",
        "buckets": ["cdse-backup"]
    }
}
```

## SFTP

Not implemented: instanciating it raises `NotImplementedError`.

```json
{
    "...": "...",
    "backup": {
        "type": "BackupSFTP",
        "directory": "/backup",
        "host": "sftp.local",
        "port": 22,
        "username": "...",
        "password": "..."
    }
}
```

## Named backups

A backup can be defined once, by a name, and shared by several entries. A configuration
file holds a `backups` key, **one file per backup**: `backups/<name>.json`, whose content
is `{"backups": [definition]}`, tagged with every service that may use it.

```json
{
    "backups": [
        {
            "name": "archive-a",
            "description": "the local archive",
            "type": "BackupLocal",
            "directory": "/data/archive-a",
            "calendar_tree": true
        }
    ]
}
```

| key | type | description |
| --- | --- | --- |
| `name` | string | `[a-z0-9][a-z0-9-]*`, at most 64 characters, unique among the files a service loads |
| `description` | string | optional |
| `type` | string | `BackupS3` or `BackupLocal` |
| `directory` | string | mandatory; for `BackupLocal` an absolute path in the container, without `..` |
| `calendar_tree` | boolean | default `false` |
| `enable_gzip` | boolean | default `false` |
| `report_model` | string or null | collector file backups only, see "Backup reports" |
| `buckets` | list of strings | S3 only, mandatory for S3; several buckets are mirrors |
| `s3_endpoint_url`, `s3_region`, `s3_signature_version` | string or null | S3 only |
| `credentials` | string | S3 only, mandatory for S3: the name of a credential entry, which starts with `backup-` |

- **Several buckets are mirrors.** Every file is written to every bucket: adding a
  bucket duplicates every new write, it does not add capacity.
- **A filesystem `directory` is an absolute path in the container.**
- **No secret in a backup block**, named or inline: `s3_access_key` and
  `s3_secret_key` are refused. They live in the credential entry named by
  `credentials`.

The rules are checked by `maas_collector.backup.named`.

## Backup reports

A transfer that is not reported is invisible: a failed upload is logged as
`critical` but it does not stop the ingestion journal from advancing, so an
archive gap cannot be detected afterwards.

Setting `report_model` makes the collector store **one document per transfer**,
which answers what was backed up, where to, when, with what status and how long it
took. Leaving it out keeps the previous behaviour and writes nothing.

```json
{
    "...": "...",
    "backup": {
        "type": "BackupS3",
        "directory": "cdse/sentinel-1/ocn",
        "buckets": ["cdse-backup", "cdse-backup-cold"],
        "report_model": "Backup"
    }
}
```

`report_model` names a model declared in the `models` array of any configuration
file, exactly like the `model` key of a collector. An unknown name raises at
startup rather than on the first backed up file.

The model shall declare these fields:

| field | type | description |
| --- | --- | --- |
| `filename` | `Keyword` | name of the file as transferred, ends with `.gz` when compressed |
| `source_filename` | `Keyword` | name of the ingested file, before any compression |
| `source_path` | `Keyword` | local path of the file actually transferred |
| `size_bytes` | `Long` | size of the transferred file |
| `compressed` | `Boolean` | whether the file was gzipped before transfer |
| `backup_type` | `Keyword` | `BackupS3`, `BackupLocal` or `BackupSFTP` |
| `destination` | `Keyword` | s3 bucket, local root directory or sftp host |
| `destination_path` | `Keyword` | full path of the file on the destination |
| `interface_name` | `Keyword` | interface of the collector configuration |
| `model_name` | `Keyword` | model of the ingested file |
| `start_time` | `Date` | when the transfer started |
| `end_time` | `Date` | when the transfer ended |
| `duration` | `Float` | transfer duration in seconds |
| `status` | `Keyword` | `OK` or `KO` |
| `error_type` | `Keyword` | exception class name when the status is `KO` |
| `details` | `Text` | error message when the status is `KO` |

Notes:

- **One document per transfer.** A file sent to three buckets produces three
  documents, so a partial failure is visible. A sidecar metadata file is reported
  like any other file.
- **The index is an event log, not a state record.** The identifier is the md5 of
  the file name, the destination, the destination path, the interface and the
  start instant, and documents are written with the `create` operation: replaying
  an ingestion appends new documents instead of overwriting the previous ones.
- **Reporting never breaks an ingestion.** A transport error is turned into a `KO`
  document, and a failure to store the reports themselves is logged and swallowed.

## Improvments

- Support multiple backups strategy

```json
{
    "...": "...",
    "backup": [
        {
            "type": "BackupLocal",
            "directory": "/tmp/backup_on_system_a"
        },
        {
            "type": "BackupLocal",
            "directory": "/tmp/backup_on_system_b"
        }
    ]
}
```

- Support Mail backup

```json
{
    "...": "...",
    "backup": {
        "type": "BackupMail",
        "email_adress": ["maas@local", "backup@local"],
        "server_adress": "...",
        "...": "..."
    }
}
```

- Implement the SFTP backup
- Retention of the backup report index: nothing purges old yearly partitions
