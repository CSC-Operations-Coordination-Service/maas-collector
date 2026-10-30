"""Samples shared by the tests of the named backups"""

LOCAL = {"name": "archive-a", "type": "BackupLocal", "directory": "/data/archive-a"}

S3 = {
    "name": "archive-b",
    "type": "BackupS3",
    "directory": "archive",
    "buckets": ["bucket-a", "bucket-b"],
    "s3_endpoint_url": "https://storage.example",
    "s3_region": "region-1",
    "s3_signature_version": "s3v4",
    "credentials": "backup-archive-b",
}

KEYS = {"s3_access_key": "test-access-key", "s3_secret_key": "test-secret-key"}

ODATA = "ODataCollectorConfiguration"
"""class of every test entry that carries an `archive` rule"""


def entry(interface_name="ITF_A", **extra) -> dict:
    """a minimal collector entry (LogExtractor requires `pattern`,
    FileCollectorConfiguration requires `id_field` and `routing_key`)"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": {"name": "ModelA", "index": "", "fields": []},
        "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
        **extra,
    }


def backups_file(*definitions) -> dict:
    """the content of a backups file"""
    return {"backups": list(definitions)}


def models_file(*names) -> dict:
    """the content of a models file"""
    return {"models": [{"name": n, "index": "", "fields": []} for n in names]}
