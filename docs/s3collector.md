# S3 Collector to collect data from s3 bucket

S3 collector is used to ingest data from s3.

## Quick start

```bash
TZ=UTC python3.11 -m maas_collector.rawdata.cli.s3 -d "<path to s3 collector folder>/s3/" --healthcheck-port 31400 -p 0 -v --credential-file "<path to credential-file.json>"
```

## Command Line Options

common Options of a collector (es_url, working_dir ....) see common parser.md and specific hereatfer

| CLI option               | Environment variable       | Description                                              |
| ------------------------ | -------------------------- | -------------------------------------------------------- |
| --s3-timeout             | S3_TIMEOUT                 | Timeout in seconds defaulted to 120                      |

## Collector Configuration Options

| Configuration key | Description                              |
| ----------------- | ---------------------------------------- |
| class             | shall be S3CollectorConfiguration------- |

## Prefix and move after ingest

Two optional keys of the collector credential configuration turn a bucket into a
drop folder:

| Key | Default | Description |
| --- | --- | --- |
| `s3_prefix` | `""` | List only the objects under this key prefix, for example `to_ingest/`. Empty lists the whole bucket. |
| `s3_move_to` | `""` | Key prefix an object is moved to once it is ingested. `{date}` is the UTC day (`YYYY-MM-DD`), for example `ingested/{date}/`. Empty leaves the object in place. |

With `s3_move_to` set:

- the object is **moved** (copy, then delete) after its documents are written. If the
  copy fails the object stays where it was and is ingested again on the next loop;
- the journal date filter is not applied: everything under `s3_prefix` is still to be
  ingested, whatever its `LastModified`;
- the first configuration that matches the file name ingests it, the other ones are not
  tried (the object is gone);
- the part of the key under `s3_prefix` is kept, so `to_ingest/AUXIP/a.tgz` goes to
  `ingested/2026-10-28/AUXIP/a.tgz`.

A prefix that is not scanned (for example `buffer/`) holds files that wait for an
operator to choose them.

## Sample collector credential configuration

```json
        {
            // Nom de la configuration (obligatoire)
            "name": "data-from-s3-test-interface-name",
             // URL de l'endpoint S3, généralement pour la région spécifiée (obligatoire)
            //  ATTENTION si vous utilisez le s3 bucket de amazon l'url est https://s3.<region>.amazonaws.com
            //  pour les autres à verifier mais normalement pas besoin
            "s3_endpoint_url": "https://s3.eu-west-3.amazonaws.com", 
            // Liste des buckets S3 à interroger ou surveiller (obligatoire)
            "buckets": [
                "sample-bucket-name"
            ],
            // Clé d'accès S3 (AWS Access Key ID) (obligatoire)
            "s3_access_key": "xxxxxxxxxxxxx",
            // Clé secrète S3 (AWS Secret Access Key) (obligatoire)
            "s3_secret_key": "xxxxxxxxxxxxxx",
            // Version de la signature utilisée pour l'authentification S3 (optionnelle) (par défaut : v4)
            "s3_signature_version": "s3v4",
            // Région AWS où se trouve le bucket S3 (optionnelle)
            "s3_region": "eu-west-3", 
            // Nombre maximum d'objets (ou "keys") à récupérer lors d'une requête S3 (optionnelle) (par défaut : 1000)
            "s3_max_keys": 1000,
             // Timestamp en secondes initial pour commencer à récupérer les objets S3 optionnelle) (par défaut : 0) qui correspond à 1970-01-01 00:00:00 (Unix epoch)
            "s3_initial_timestamp": 1609459200
        }
```

## Sample collector configuration

```json
{
    "collectors": [
        {
            "class": "S3CollectorConfiguration",
            "interface_credentials": "data-from-s3-test",
            "interface_name": "data-from-s3-test-interface-name",
            "id_field": [
                "link_session_id",
                "ground_station"
            ],
            "file_pattern": "EDRS*S2*DOR*.xlsx",
            "routing_key": "new.raw.data.aps.edrs",
            "expected_collect_interval": 1800,
            "model": "ApsEdrs",
            "extractor": {
                "class": "EDRSApsExtractor"
            }
        }
    ],
    "amqp": {
        "new.raw.data.aps.edrs": {
            "chunk_size": 128
        }
    }
}
```
