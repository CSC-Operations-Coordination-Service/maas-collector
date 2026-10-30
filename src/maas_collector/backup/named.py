"""Named backups: what a backup definition is, and how a name is resolved.

Pure functions that import nothing from the extractors, so that the file collector,
the configuration validation and the configuration store share them.
"""

import copy
import re
import typing

BACKUP_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")
"""the name of a backup, always used with fullmatch"""

MAX_NAME_LENGTH = 64

CREDENTIAL_PREFIX = "backup-"

CREDENTIAL_NAME = re.compile(r"backup-[A-Za-z0-9._-]+")
"""the name of the credential entry holding the S3 keys of a backup"""

SECRET_FIELDS = ("s3_access_key", "s3_secret_key")
"""keys refused in any backup block: they live in a credential entry"""

DESTINATION_KEYS = (
    "type",
    "s3_endpoint_url",
    "directory",
    "calendar_tree",
    "enable_gzip",
    "buckets",
)
"""keys that cannot change once the backup has reports"""

DEFINITION_KEYS = frozenset(
    {
        "name",
        "description",
        "type",
        "directory",
        "calendar_tree",
        "enable_gzip",
        "report_model",
        "buckets",
        "s3_endpoint_url",
        "s3_region",
        "s3_signature_version",
        "credentials",
    }
)

BACKUP_TYPES = ("BackupS3", "BackupLocal")

S3_ONLY_KEYS = ("buckets", "s3_endpoint_url", "s3_region", "s3_signature_version")

NOT_IN_A_BLOCK = ("name", "description", "credentials")
"""keys of a definition that the backup object does not know"""

DESTINATION_DEFAULTS = {"calendar_tree": False, "enable_gzip": False}


def check_backup_block(block: dict, where: str) -> None:
    """check what every backup block must satisfy, named or inline

    Args:
        block (dict): the backup block or definition
        where (str): what the block belongs to, starts every message

    Raises:
        ValueError: a secret field, a bad name or a bad credentials name. A message
            names the field and never echoes a value.
    """
    if not isinstance(block, dict):
        raise ValueError(f"{where}: a backup must be a name or a block")

    for field in SECRET_FIELDS:
        if field in block:
            raise ValueError(
                f"{where}: {field} is not allowed in a backup block, "
                "put it in the credential entry named by `credentials`"
            )

    if "name" in block:
        name = block["name"]
        if (
            not isinstance(name, str)
            or len(name) > MAX_NAME_LENGTH
            or not BACKUP_NAME.fullmatch(name)
        ):
            raise ValueError(
                f"{where}: backup name must match [a-z0-9][a-z0-9-]* "
                f"and be at most {MAX_NAME_LENGTH} characters"
            )

    if "credentials" in block:
        credentials = block["credentials"]
        if not isinstance(credentials, str) or not CREDENTIAL_NAME.fullmatch(
            credentials
        ):
            raise ValueError(
                f"{where}: credentials must name a credential entry "
                f"starting with {CREDENTIAL_PREFIX}"
            )

    if block.get("type") == "BackupS3":
        if not block.get("credentials"):
            raise ValueError(
                f"{where}: an S3 backup must name its credentials entry in `credentials`"
            )
    elif "credentials" in block:
        raise ValueError(f"{where}: credentials only apply to a BackupS3")


def check_definition(definition: dict, where: str) -> None:
    """check a backup definition, the content of a `backups` item

    Args:
        definition (dict): the definition
        where (str): what the definition belongs to, starts every message

    Raises:
        ValueError: the definition breaks a rule
    """
    check_backup_block(definition, where)

    unknown = sorted(set(definition) - DEFINITION_KEYS)
    if unknown:
        raise ValueError(f"{where}: unknown backup keys {', '.join(unknown)}")

    if "name" not in definition:
        raise ValueError(f"{where}: a backup definition needs a name")

    backup_type = definition.get("type")
    if backup_type not in BACKUP_TYPES:
        raise ValueError(f"{where}: type must be one of {', '.join(BACKUP_TYPES)}")

    directory = definition.get("directory")
    if not isinstance(directory, str) or not directory:
        raise ValueError(f"{where}: directory must be a non-empty string")

    if backup_type == "BackupLocal":
        if not directory.startswith("/") or ".." in directory.split("/"):
            raise ValueError(
                f"{where}: directory of a BackupLocal must be an absolute path "
                "without .. segment"
            )

    for key in ("calendar_tree", "enable_gzip"):
        if key in definition and not isinstance(definition[key], bool):
            raise ValueError(f"{where}: {key} must be a boolean")

    if "description" in definition and not isinstance(definition["description"], str):
        raise ValueError(f"{where}: description must be a string")

    report_model = definition.get("report_model")
    if report_model is not None and not isinstance(report_model, str):
        raise ValueError(f"{where}: report_model must be a string or null")

    if backup_type == "BackupS3":
        buckets = definition.get("buckets")
        if (
            not isinstance(buckets, list)
            or not buckets
            or not all(isinstance(bucket, str) and bucket for bucket in buckets)
        ):
            raise ValueError(f"{where}: buckets must be a non-empty list of names")
    else:
        for key in S3_ONLY_KEYS:
            if key in definition:
                raise ValueError(f"{where}: {key} only applies to a BackupS3")


def backup_definitions(contents: typing.Iterable[dict]) -> typing.Dict[str, dict]:
    """index the backup definitions of configuration contents by name

    Args:
        contents (Iterable[dict]): configuration file contents

    Raises:
        ValueError: a definition breaks a rule, or a name is defined twice

    Returns:
        dict[str, dict]: name -> definition
    """
    definitions: typing.Dict[str, dict] = {}

    for content in contents:
        if not isinstance(content, dict):
            continue

        for definition in content.get("backups") or []:
            name = definition.get("name") if isinstance(definition, dict) else None
            check_definition(definition, f"backup {name!r}")

            if name in definitions:
                raise ValueError(f"backup {name!r} is defined twice")

            definitions[name] = definition

    return definitions


def resolve_backup(
    reference: typing.Union[str, dict], definitions: typing.Dict[str, dict]
) -> typing.Tuple[dict, typing.Optional[str]]:
    """resolve the `backup` of an entry

    Args:
        reference (str | dict): a backup name or an inline block
        definitions (dict[str, dict]): the definitions the service loads

    Raises:
        KeyError: unknown name
        ValueError: neither a name nor a block, or a block with a secret field

    Returns:
        tuple[dict, str | None]: the block for instanciate_collector_backup, without
            name, description and credentials, and the name of the credential entry
    """
    if isinstance(reference, str):
        block = definitions[reference]
    elif isinstance(reference, dict):
        check_backup_block(reference, "inline backup block")
        block = reference
    else:
        raise ValueError("a backup must be a name or a block")

    resolved = {
        key: copy.deepcopy(value)
        for key, value in block.items()
        if key not in NOT_IN_A_BLOCK
    }

    return resolved, block.get("credentials") or None


def destination_changes(old: dict, new: dict) -> typing.List[str]:
    """list the destination keys that differ between two definitions

    Args:
        old (dict): the previous definition
        new (dict): the new definition

    Returns:
        list[str]: the keys of DESTINATION_KEYS that changed, `buckets` when a
            bucket of old is missing from new
    """
    changes = []

    for key in DESTINATION_KEYS:
        if key == "buckets":
            continue

        default = DESTINATION_DEFAULTS.get(key)
        if old.get(key, default) != new.get(key, default):
            changes.append(key)

    if set(old.get("buckets") or []) - set(new.get("buckets") or []):
        changes.append("buckets")

    return changes
