"""What a custom recollect may change in a collector entry, and what it may not.

Pure module: it imports only the standard library (and, from Task 2 on, the
names of ``backup/named.py``), because the odata collector reaches it through
the archive rule and must not create an import cycle.

The window checks are structural: they do not parse OData or JQL, they only
make sure that the query a person pastes still contains the replay window as
a top-level ``and`` term, and has no top-level ``or`` that would let the query
escape it.
"""

import re
import string
from typing import Iterable, List, Tuple

DEFAULT_ODATA_DATE_FIELD = "PublicationDate"
ODATA_PLACEHOLDERS = ("publication_start_date", "publication_end_date")
JIRA_PLACEHOLDERS = ("date_criteria",)
JIRA_CLAUSE = "updated > {date_criteria}"

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
