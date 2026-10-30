"""What a custom recollect may change in a collector entry, and what it may not.

Pure module: it imports only the standard library (and, from Task 2 on, the
names of ``backup/named.py``), because the odata collector reaches it through
the archive rule and must not create an import cycle.

The window checks are structural: they do not parse OData or JQL, they only
make sure that the query a person pastes still contains the replay window as
a top-level ``and`` term, and has no top-level ``or`` that would let the query
escape it.
"""

import copy
import re
import string
from typing import Dict, FrozenSet, Iterable, List, Optional, Tuple

from maas_collector.backup.named import BACKUP_NAME, CREDENTIAL_PREFIX, MAX_NAME_LENGTH

DEFAULT_ODATA_DATE_FIELD = "PublicationDate"
ODATA_PLACEHOLDERS = ("publication_start_date", "publication_end_date")
JIRA_PLACEHOLDERS = ("date_criteria",)
JIRA_CLAUSE = "updated > {date_criteria}"

ODATA_CLASS = "ODataCollectorConfiguration"
JIRA_CLASS = "JiraExtendedCollectorConfiguration"

OVERRIDABLE_KEYS: Dict[str, FrozenSet[str]] = {
    ODATA_CLASS: frozenset(
        {
            "odata_query_filter",
            "download_file_pattern",
            "product_per_page",
            "interface_credentials",
        }
    ),
    JIRA_CLASS: frozenset({"jql_query", "interface_credentials"}),
}
"""the keys of a collector entry a custom recollect may replace, by class name;
every other key stays live"""

DUPLICATES = ("skip", "replace_if_changed")
ARCHIVE_CHOICE_KEYS = ("backup", "backup_version", "duplicates", "force")
MAX_PRODUCT_PER_PAGE = 1000

INTERFACE_CREDENTIALS = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

# placed in the masked text instead of every character that is not at depth 0
# outside quotes
_MASK = "\x00"
_QUERY_STRING_DELIMITERS = "&#?"
_AND = re.compile(r"\band\b", re.IGNORECASE)
_OR = re.compile(r"\bor\b", re.IGNORECASE)
_ORDER_BY = re.compile(r"\border\s+by\b", re.IGNORECASE)


class OverrideRefused(ValueError):
    """A custom recollect asks for something it may not do."""


def _collapse(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _check_replacement_fields(text: str, allowed: Tuple[str, ...]) -> None:
    """Refuse a lone brace and any replacement field that is not a placeholder."""
    try:
        parsed = list(string.Formatter().parse(text))
    except ValueError as error:
        raise OverrideRefused(f"unbalanced brace in the query ({error})") from error
    for _literal, name, spec, conversion in parsed:
        if name is None:
            continue
        if name not in allowed or spec or conversion:
            shown = "{" + name + ("!" + conversion if conversion else "")
            shown += (":" + spec if spec else "") + "}"
            raise OverrideRefused(
                f"replacement field {shown} is not allowed, "
                f"only {', '.join('{' + a + '}' for a in allowed)}"
            )


def _mask(text: str, quotes: str, backslash_escape: bool) -> str:
    """Return ``text`` with every character not at depth 0, outside quotes, masked.

    Quote characters and parentheses are masked too. A quote that is not closed
    or a parenthesis that is not balanced raises ``OverrideRefused``.
    """
    masked: List[str] = []
    quote = ""
    depth = 0
    escaped = False
    for char in text:
        if quote:
            masked.append(_MASK)
            if escaped:
                escaped = False
            elif backslash_escape and char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
        elif char in quotes:
            quote = char
            masked.append(_MASK)
        elif char == "(":
            depth += 1
            masked.append(_MASK)
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise OverrideRefused("unbalanced parenthesis in the query")
            masked.append(_MASK)
        else:
            masked.append(char if depth == 0 else _MASK)
    if quote:
        raise OverrideRefused("unclosed quote in the query")
    if depth:
        raise OverrideRefused("unbalanced parenthesis in the query")
    return "".join(masked)


def _split_terms(text: str, masked: str) -> List[str]:
    """Split ``text`` at the top-level ``and`` keywords, whitespace collapsed."""
    terms, start = [], 0
    for match in _AND.finditer(masked):
        terms.append(_collapse(text[start : match.start()]))
        start = match.end()
    terms.append(_collapse(text[start:]))
    return terms


def _require_terms(
    text: str, terms: Iterable[str], patterns: Iterable[Tuple[str, "re.Pattern"]]
) -> None:
    terms = list(terms)
    collapsed = _collapse(text)
    for shown, pattern in patterns:
        if any(pattern.fullmatch(term) for term in terms):
            continue
        where = (
            " (it is inside a group or a quote)" if pattern.search(collapsed) else ""
        )
        raise OverrideRefused(
            f"the query must include the top-level term '{shown}'{where}"
        )


def check_odata_window(odata_filter: str, date_field: str) -> None:
    """Refuse an OData ``$filter`` that does not keep the replay window."""
    if any(char in odata_filter for char in _QUERY_STRING_DELIMITERS):
        raise OverrideRefused(
            "the filter carries a query string delimiter (& # ?): "
            "it would end the $filter in the request URL"
        )
    _check_replacement_fields(odata_filter, ODATA_PLACEHOLDERS)
    masked = _mask(odata_filter, "'", backslash_escape=False)
    if _OR.search(masked):
        raise OverrideRefused(
            "a top-level or would let the filter escape the replay window: "
            "put it in parentheses"
        )
    start, end = ODATA_PLACEHOLDERS
    _require_terms(
        odata_filter,
        _split_terms(odata_filter, masked),
        [
            (
                f"{date_field} ge {{{start}}}",
                re.compile(
                    re.escape(date_field) + r" (?i:ge) " + re.escape("{%s}" % start)
                ),
            ),
            (
                f"{date_field} le {{{end}}}",
                re.compile(
                    re.escape(date_field) + r" (?i:le) " + re.escape("{%s}" % end)
                ),
            ),
        ],
    )


def check_jql_window(jql: str) -> None:
    """Refuse a JQL query that does not keep the replay window."""
    _check_replacement_fields(jql, JIRA_PLACEHOLDERS)
    masked = _mask(jql, "\"'", backslash_escape=True)
    order_by = _ORDER_BY.search(masked)
    cut = order_by.start() if order_by else len(jql)
    text, masked = jql[:cut], masked[:cut]
    if _OR.search(masked):
        raise OverrideRefused(
            "a top-level or would let the query escape the replay window: "
            "put it in parentheses"
        )
    head, tail = JIRA_CLAUSE.split(" > ")
    _require_terms(
        text,
        _split_terms(text, masked),
        [
            (
                JIRA_CLAUSE,
                re.compile(r"(?i:" + re.escape(head) + r") > " + re.escape(tail)),
            )
        ],
    )


def _check_odata_query_filter(value, live_entry: dict) -> None:
    if not isinstance(value, str):
        raise OverrideRefused("odata_query_filter must be a string")
    date_field = live_entry.get("filter_date_v4") or DEFAULT_ODATA_DATE_FIELD
    check_odata_window(value, date_field)


def _check_jql_query(value, _live_entry: dict) -> None:
    if not isinstance(value, str):
        raise OverrideRefused("jql_query must be a string")
    check_jql_window(value)


def _check_download_file_pattern(value, _live_entry: dict) -> None:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise OverrideRefused(
            "download_file_pattern must be a list of non-empty strings"
        )


def _check_product_per_page(value, live_entry: dict) -> None:
    live = live_entry.get("product_per_page", MAX_PRODUCT_PER_PAGE)
    if not isinstance(live, int) or isinstance(live, bool):
        live = MAX_PRODUCT_PER_PAGE
    highest = max(live, MAX_PRODUCT_PER_PAGE)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= highest
    ):
        raise OverrideRefused(
            f"product_per_page must be an integer between 1 and {highest}"
        )


def _check_interface_credentials(value, _live_entry: dict) -> None:
    if (
        not isinstance(value, str)
        or not INTERFACE_CREDENTIALS.fullmatch(value)
        or value.startswith(CREDENTIAL_PREFIX)
    ):
        raise OverrideRefused(
            "interface_credentials must be the name of an interface credential "
            f"entry (not one starting with {CREDENTIAL_PREFIX})"
        )


_VALUE_CHECKS = {
    "odata_query_filter": _check_odata_query_filter,
    "jql_query": _check_jql_query,
    "download_file_pattern": _check_download_file_pattern,
    "product_per_page": _check_product_per_page,
    "interface_credentials": _check_interface_credentials,
}


def merge_overrides(collector_class: str, live_entry: dict, overrides: dict) -> dict:
    """Return a copy of ``live_entry`` with the ``overrides`` applied.

    Raises ``OverrideRefused`` for a class that has no overridable keys, a key
    outside its allowlist, or a value that is not acceptable.
    """
    allowed = OVERRIDABLE_KEYS.get(collector_class)
    if allowed is None:
        raise OverrideRefused(
            f"{collector_class} has no overridable keys: "
            "a custom recollect needs an odata or a jira entry"
        )
    merged = copy.deepcopy(live_entry)
    for key, value in overrides.items():
        if key not in allowed:
            raise OverrideRefused(
                f"{key} cannot be overridden on {collector_class} "
                f"(allowed: {', '.join(sorted(allowed))})"
            )
        try:
            _VALUE_CHECKS[key](value, live_entry)
        except OverrideRefused as error:
            if key in str(error):
                raise
            raise OverrideRefused(f"{key}: {error}") from error
        merged[key] = copy.deepcopy(value)
    return merged


def normalize_archive_choice(archive: dict) -> dict:
    """Return the archive choice with its defaults, or raise ``OverrideRefused``."""
    if not isinstance(archive, dict):
        raise OverrideRefused("archive must be an object")
    for key in archive:
        if key not in ARCHIVE_CHOICE_KEYS:
            raise OverrideRefused(
                f"archive.{key} is not part of an archive choice "
                f"(allowed: {', '.join(ARCHIVE_CHOICE_KEYS)})"
            )
    backup = archive.get("backup")
    if (
        not isinstance(backup, str)
        or len(backup) > MAX_NAME_LENGTH
        or not BACKUP_NAME.fullmatch(backup)
    ):
        raise OverrideRefused(
            "archive.backup must be the name of a backup "
            f"([a-z0-9][a-z0-9-]*, at most {MAX_NAME_LENGTH} characters)"
        )
    version: Optional[int] = archive.get("backup_version")
    if version is not None and (
        not isinstance(version, int) or isinstance(version, bool) or version < 1
    ):
        raise OverrideRefused("archive.backup_version must be a positive integer")
    duplicates = archive.get("duplicates", "skip")
    if duplicates not in DUPLICATES:
        raise OverrideRefused(
            f"archive.duplicates must be one of {', '.join(DUPLICATES)}"
        )
    force = archive.get("force", False)
    if not isinstance(force, bool):
        raise OverrideRefused("archive.force must be true or false")
    return {
        "backup": backup,
        "backup_version": version,
        "duplicates": duplicates,
        "force": force,
    }


def check_archive_choice(collector_class: str, archive: Optional[dict]) -> None:
    """Refuse an archive choice a collector class cannot serve."""
    if archive is None:
        return
    if collector_class != ODATA_CLASS:
        raise OverrideRefused(
            f"{collector_class} cannot archive: an archive choice needs an odata entry"
        )
    normalize_archive_choice(archive)
