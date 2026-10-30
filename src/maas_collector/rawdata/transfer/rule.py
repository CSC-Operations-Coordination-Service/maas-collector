"""Archive rules: which products of an odata entry are archived, and how.

This module imports only ``backup/named.py`` and ``operator/overrides.py``, so
that the odata collector can import it at module level.
"""

import datetime
import hashlib
from dataclasses import dataclass
from typing import Iterable

from maas_collector.backup.named import BACKUP_NAME, MAX_NAME_LENGTH
from maas_collector.rawdata.operator.overrides import DUPLICATES

DEFAULT_FIELDS = {
    "product_id": "product_id",
    "product_name": "product_name",
    "size": "content_length",
    "checksum": "checksum",
    "checksum_algorithm": "MD5",
    "publication_date": "publication_date",
}

RULE_KEYS = ("backup", "duplicates", "fields", "download", "backup_version", "force")
DOWNLOAD_KEYS = ("url", "quote_id")


def new_hash(algorithm: str):
    """A hashlib object for an algorithm name

    The lookup tries the lower-cased name, then with ``-`` replaced by ``_``, then
    with ``-`` removed.

    Raises:
        ValueError: the algorithm is unknown to hashlib, or has no fixed length
    """
    lowered = str(algorithm).lower()
    for name in (lowered, lowered.replace("-", "_"), lowered.replace("-", "")):
        try:
            digest = hashlib.new(name)
            digest.hexdigest()
            return digest
        except (ValueError, TypeError):
            continue
    raise ValueError(f"unknown checksum algorithm {algorithm!r}")


def _is_version(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


@dataclass(frozen=True)
class ArchiveRule:
    """The archive rule of a collector entry (``archive``)"""

    backup: str
    duplicates: str
    fields: dict
    download_url: str | None
    quote_id: bool
    backup_version: int | None = None
    force: bool = False

    @classmethod
    def default(cls, backup: str) -> "ArchiveRule":
        """The rule of a backup name alone"""
        return cls(backup, "skip", dict(DEFAULT_FIELDS), None, True)

    @classmethod
    def from_entry(cls, entry: dict) -> "ArchiveRule | None":
        """Read the rule of a collector entry

        Returns:
            the rule, None without ``archive`` (or with null)

        Raises:
            ValueError: a bad key; the message starts with the interface name and
                names the key
        """
        archive = entry.get("archive")
        if archive is None:
            return None
        where = f"{entry.get('interface_name')}: archive"
        if not isinstance(archive, dict):
            raise ValueError(f"{where} must be an object, got {archive!r}")
        unknown = sorted(set(archive) - set(RULE_KEYS))
        if unknown:
            raise ValueError(f"{where}: unknown keys {unknown}, expected {RULE_KEYS}")

        backup = archive.get("backup")
        if (
            not isinstance(backup, str)
            or len(backup) > MAX_NAME_LENGTH
            or not BACKUP_NAME.fullmatch(backup)
        ):
            raise ValueError(
                f"{where}.backup must be a backup name matching"
                f" [a-z0-9][a-z0-9-]* of at most {MAX_NAME_LENGTH} characters"
            )

        duplicates = archive.get("duplicates", "skip")
        if duplicates not in DUPLICATES:
            raise ValueError(f"{where}.duplicates must be one of {DUPLICATES}")

        fields = dict(DEFAULT_FIELDS)
        given = archive.get("fields", {})
        if not isinstance(given, dict):
            raise ValueError(f"{where}.fields must be an object")
        for name, value in given.items():
            if name not in DEFAULT_FIELDS:
                raise ValueError(
                    f"{where}.fields: unknown key {name!r}, expected {sorted(DEFAULT_FIELDS)}"
                )
            if not isinstance(value, str) or not value:
                raise ValueError(f"{where}.fields.{name} must be a non-empty string")
            fields[name] = value

        download = archive.get("download", {})
        if not isinstance(download, dict):
            raise ValueError(f"{where}.download must be an object")
        unknown = sorted(set(download) - set(DOWNLOAD_KEYS))
        if unknown:
            raise ValueError(
                f"{where}.download: unknown keys {unknown}, expected {DOWNLOAD_KEYS}"
            )
        url = download.get("url")
        if url is not None:
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                raise ValueError(f"{where}.download.url must start with http:// or https://")
            if "{id}" not in url:
                raise ValueError(f"{where}.download.url must contain {{id}}")
        quote_id = download.get("quote_id", True)
        if not isinstance(quote_id, bool):
            raise ValueError(f"{where}.download.quote_id must be true or false")

        version = archive.get("backup_version")
        if version is not None and not _is_version(version):
            raise ValueError(f"{where}.backup_version must be null or an integer >= 1")
        force = archive.get("force", False)
        if not isinstance(force, bool):
            raise ValueError(f"{where}.force must be true or false")

        rule = cls(backup, duplicates, fields, url, quote_id, version, force)
        try:
            rule.new_checksum()
        except ValueError as error:
            raise ValueError(f"{where}.fields.checksum_algorithm: {error}") from error
        return rule

    def new_checksum(self):
        """A new hashlib object of the checksum algorithm of the rule"""
        return new_hash(self.fields["checksum_algorithm"])


def check_archive_rule(entry: dict, configuration_classes: Iterable[type]) -> None:
    """Check the ``archive`` key of a collector entry

    Raises:
        ValueError: a class cannot archive, or the rule is bad
    """
    if entry.get("archive") is None:
        return
    for configuration_class in configuration_classes:
        if not getattr(configuration_class, "ARCHIVABLE", False):
            raise ValueError(
                f"{entry.get('interface_name')}: archive rules are for odata entries,"
                f" {configuration_class.__name__} cannot archive"
            )
    ArchiveRule.from_entry(entry)


def _blank(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _size(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value.strip().isdigit():
        value = int(value.strip())
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return value if isinstance(value, int) and value > 0 else None


def product_of(source: dict, rule: ArchiveRule, document_id: str, document_index: str) -> dict:
    """The product a raw document describes, read through the fields of the rule

    Values the document does not give are None: no size or checksum is checked
    then, and the product is compared by name only.
    """
    checksum = source.get(rule.fields["checksum"])
    values = {}
    for name in ("product_id", "product_name", "publication_date"):
        value = source.get(rule.fields[name])
        values[name] = None if value == "" else value
    return {
        "document_id": document_id,
        "document_index": document_index,
        "product_id": values["product_id"],
        "product_name": values["product_name"],
        "size": _size(source.get(rule.fields["size"])),
        "checksum": None if _blank(checksum) else str(checksum).strip(),
        "publication_date": values["publication_date"],
    }


def utc_datetime(value) -> datetime.datetime | None:
    """A date as a timezone-aware UTC datetime

    Returns:
        None when absent; a naive date is read as UTC

    Raises:
        ValueError: the value is not a date
    """
    if value is None or value == "":
        return None
    if isinstance(value, str):
        try:
            value = datetime.datetime.fromisoformat(value.strip())
        except ValueError as error:
            raise ValueError(f"invalid date {value!r}") from error
    if not isinstance(value, datetime.datetime):
        raise ValueError(f"invalid date {value!r}")
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC)
