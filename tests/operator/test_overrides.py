"""Tests of the window checks, the allowlist and the merge of a custom recollect."""

import pytest

from maas_collector.rawdata.operator.overrides import (
    OverrideRefused,
    check_jql_window,
    check_odata_window,
)

WINDOW = (
    "PublicationDate ge {publication_start_date} and "
    "PublicationDate le {publication_end_date}"
)


@pytest.mark.parametrize(
    "flt",
    [
        WINDOW,
        WINDOW + " and contains(Name,'TYPE_A')",
        "contains(Name,'TYPE_A') and " + WINDOW,
        "(Collection/Name eq 'S1' or Collection/Name eq 'S2') and " + WINDOW,
        "contains(Name,'a or b (') and " + WINDOW,
        "contains(Name,'it''s') and " + WINDOW,
    ],
)
def test_odata_windows_that_stay(flt):
    check_odata_window(flt, "PublicationDate")


@pytest.mark.parametrize(
    "flt, word",
    [
        ("PublicationDate le {publication_end_date}", "ge"),
        ("PublicationDate ge {publication_start_date}", "le"),
        (
            "PublicationDate gt {publication_start_date} and "
            "PublicationDate le {publication_end_date}",
            "ge",
        ),
        (
            "CreationDate ge {publication_start_date} and "
            "CreationDate le {publication_end_date}",
            "PublicationDate",
        ),
        ("(" + WINDOW + ")", "top-level"),
        (WINDOW + " or Name eq 'x'", r"\bor\b"),
        ("Name eq 'x' or " + WINDOW, r"\bor\b"),
        (WINDOW + " and (Name eq 'a' or Name eq 'b') or Name eq 'c'", r"\bor\b"),
        (WINDOW + " and Name eq 'x", "quote"),
        (WINDOW + " and (Name eq 'x'", "parenthes"),
        (WINDOW + ")", "parenthes"),
        ("", "ge"),
    ],
)
def test_odata_windows_that_escape(flt, word):
    with pytest.raises(OverrideRefused, match=word):
        check_odata_window(flt, "PublicationDate")


def test_the_date_field_is_the_entrys():
    flt = (
        "CreationDate ge {publication_start_date} and "
        "CreationDate le {publication_end_date}"
    )
    check_odata_window(flt, "CreationDate")


def test_window_checks_ignore_whitespace_and_keyword_case():
    check_odata_window(
        "PublicationDate  ge\n{publication_start_date}\nAND\t"
        "PublicationDate le {publication_end_date}\n",
        "PublicationDate",
    )
    with pytest.raises(OverrideRefused, match=r"(?i)\bor\b"):
        check_odata_window(WINDOW + " Or Name eq 'x'", "PublicationDate")
    check_jql_window("UPDATED  >\n{date_criteria} and project = X order  by updated")


@pytest.mark.parametrize("tail", ["&$top=1000000", "#", "?x=1", "\n&$filter=true"])
def test_odata_filter_cannot_carry_query_string_delimiters(tail):
    for flt in (WINDOW + " and Name eq 'a" + tail + "'", WINDOW + tail):
        with pytest.raises(OverrideRefused, match="query string"):
            check_odata_window(flt, "PublicationDate")


@pytest.mark.parametrize(
    "field",
    [
        "{x}",
        "{}",
        "{0}",
        "{publication_start_date.__class__}",
        "{publication_start_date[0]}",
        "{publication_start_date!r}",
        "{publication_start_date:>20}",
        "{",
        "}",
    ],
)
def test_replacement_fields_are_the_window_placeholders_only(field):
    with pytest.raises(OverrideRefused, match="replacement field|brace"):
        check_odata_window(WINDOW + f" and Name eq '{field}'", "PublicationDate")
    with pytest.raises(OverrideRefused, match="replacement field|brace"):
        check_jql_window(f'updated > {{date_criteria}} AND summary ~ "{field}"')


def test_doubled_braces_are_literal():
    check_odata_window(WINDOW + " and Name eq '{{'", "PublicationDate")


@pytest.mark.parametrize(
    "jql",
    [
        "project = X AND updated > {date_criteria} ORDER BY updated ASC",
        "updated > {date_criteria}",
        "(project = A OR project = B) AND updated > {date_criteria} ORDER BY created",
        'summary ~ "a OR b" AND updated > {date_criteria}',
        "summary ~ 'a \\' OR b' AND updated > {date_criteria}",
        'updated > {date_criteria} AND status in (Closed, "TO BE VERIFIED") '
        "ORDER BY updated ASC",
    ],
)
def test_jql_windows_that_stay(jql):
    check_jql_window(jql)


@pytest.mark.parametrize(
    "jql, word",
    [
        ("project = X", "updated"),
        ("updated >= {date_criteria}", "updated"),
        ("(updated > {date_criteria})", "top-level"),
        ("project = X OR updated > {date_criteria}", r"(?i)\bor\b"),
        ("updated > {date_criteria} or project = X ORDER BY updated", r"(?i)\bor\b"),
        ('updated > {date_criteria} AND summary ~ "x', "quote"),
        ("updated > {date_criteria} AND (a = 1", "parenthes"),
    ],
)
def test_jql_windows_that_escape(jql, word):
    with pytest.raises(OverrideRefused, match=word):
        check_jql_window(jql)
