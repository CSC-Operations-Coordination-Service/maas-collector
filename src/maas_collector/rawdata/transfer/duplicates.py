"""What to do with a product the archive may already hold."""

from dataclasses import dataclass
from typing import Sequence

from maas_collector.backup.backup import OK, REPLACED


@dataclass(frozen=True)
class Decision:
    """The outcome of the duplicate check of one product"""

    transfer: tuple[str, ...] = ()
    """destinations to write"""

    skip: tuple[str, ...] = ()
    """destinations left as they are"""

    conflict: bool = False
    """a conflict: nothing is written"""

    same_bytes_as: str = ""
    """MD5 the downloaded bytes must have before a missing mirror is filled"""

    archived: frozenset = frozenset()
    """destinations whose last report is OK or REPLACED: writing there is REPLACED"""

    reference: dict | None = None
    """the archived report compared with"""


def same_checksum(first: str | None, second: str | None) -> bool | None:
    """Whether two checksums are the same, None when either is unknown

    The case and the blanks around are ignored.
    """
    first = (first or "").strip().lower()
    second = (second or "").strip().lower()
    if not first or not second:
        return None
    return first == second


def decide(
    destinations: Sequence[str],
    state: dict[str, dict],
    duplicates: str,
    force: bool,
    checksum: str | None,
) -> Decision:
    """Decide which destinations a product is written to

    Args:
        destinations: the destinations of the backup
        state: destination -> its last archive report (see ``archived_state``);
            destinations that are not in ``destinations`` are ignored
        duplicates: ``skip`` or ``replace_if_changed``
        force: a forced transfer always transfers and replaces
        checksum: the checksum the catalogue announces now, None when unknown
    """
    destinations = tuple(destinations)
    archived = tuple(
        destination
        for destination in destinations
        if state.get(destination, {}).get("status") in (OK, REPLACED)
    )
    missing = tuple(
        destination for destination in destinations if destination not in archived
    )
    reference = (
        max((state[destination] for destination in archived), key=_started)
        if archived
        else None
    )

    if force or not archived:
        return Decision(
            transfer=destinations, archived=frozenset(archived), reference=reference
        )

    changed = same_checksum(checksum, reference.get("catalogue_checksum")) is False

    if changed and duplicates == "replace_if_changed":
        return Decision(
            transfer=destinations, archived=frozenset(archived), reference=reference
        )
    if changed:
        return Decision(
            conflict=True, archived=frozenset(archived), reference=reference
        )
    return Decision(
        transfer=missing,
        skip=archived,
        same_bytes_as=reference.get("written_md5") or "",
        archived=frozenset(archived),
        reference=reference,
    )


def _started(report: dict) -> str:
    return report.get("start_time") or ""
