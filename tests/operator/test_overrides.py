"""Tests of the window checks, the allowlist and the merge of a custom recollect."""

import copy

import pytest

from maas_collector.rawdata.collector.odatacollector import (
    ODataCollectorConfiguration,
)
from maas_collector.rawdata.operator.overrides import (
    ARCHIVE_CHOICE_KEYS,
    DEFAULT_ODATA_DATE_FIELD,
    DUPLICATES,
    OVERRIDABLE_KEYS,
    OverrideRefused,
    check_archive_choice,
    check_jql_window,
    check_odata_window,
    merge_overrides,
    normalize_archive_choice,
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


ODATA = "ODataCollectorConfiguration"
JIRA = "JiraExtendedCollectorConfiguration"
LIVE = {
    "class": ODATA,
    "interface_name": "ITF_A",
    "odata_product_url": "https://remote.example",
    "product_per_page": 500,
    "odata_query_filter": WINDOW,
    "download_file_pattern": ["*"],
    "interface_credentials": "cred-a",
}


def test_the_contract_names():
    assert OVERRIDABLE_KEYS == {
        ODATA: frozenset(
            {
                "odata_query_filter",
                "download_file_pattern",
                "product_per_page",
                "interface_credentials",
            }
        ),
        JIRA: frozenset({"jql_query", "interface_credentials"}),
    }
    assert DUPLICATES == ("skip", "replace_if_changed")
    assert ARCHIVE_CHOICE_KEYS == ("backup", "backup_version", "duplicates", "force")


def test_merge_replaces_the_overridden_keys_and_leaves_the_live_entry_alone():
    before = copy.deepcopy(LIVE)
    merged = merge_overrides(
        ODATA, LIVE, {"download_file_pattern": ["*.EOF*"], "product_per_page": 100}
    )
    assert (
        merged == {**LIVE, "download_file_pattern": ["*.EOF*"], "product_per_page": 100}
        and LIVE == before
    )


@pytest.mark.parametrize(
    "key",
    [
        "odata_product_url",
        "exchange_name",
        "routing_key",
        "interface_name",
        "model",
        "extractor",
        "custom_query_suffix",
        "odata_start_offset",
        "backup",
        "archive",
        "token",
        "client_password",
        "class",
        "enabled",
    ],
)
def test_every_other_odata_key_is_refused_naming_it(key):
    with pytest.raises(OverrideRefused, match=key):
        merge_overrides(ODATA, LIVE, {key: "x"})


def test_jira_allows_two_keys_only():
    jira = {
        "class": JIRA,
        "interface_name": "J",
        "end_point": "https://j.example",
        "jql_query": "updated > {date_criteria}",
    }
    merged = merge_overrides(
        JIRA, jira, {"jql_query": "project = X AND updated > {date_criteria}"}
    )
    assert merged["jql_query"].startswith("project")
    with pytest.raises(OverrideRefused, match="end_point"):
        merge_overrides(JIRA, jira, {"end_point": "https://other.example"})
    with pytest.raises(OverrideRefused, match="jql_query"):
        merge_overrides(JIRA, jira, {"jql_query": "project = X"})


def test_a_class_without_overridable_keys_is_refused():
    with pytest.raises(OverrideRefused, match="FileCollectorConfiguration"):
        merge_overrides("FileCollectorConfiguration", {}, {"x": 1})


@pytest.mark.parametrize(
    "value, live, ok",
    [
        (1, 500, True),
        (1000, 500, True),
        (1001, 500, False),
        (2000, 2000, True),
        (2001, 2000, False),
        (0, 500, False),
        (-1, 500, False),
        (True, 500, False),
        ("5", 500, False),
        (1.5, 500, False),
    ],
)
def test_product_per_page_is_between_one_and_the_larger_of_live_and_1000(
    value, live, ok
):
    entry = {**LIVE, "product_per_page": live}
    if ok:
        merged = merge_overrides(ODATA, entry, {"product_per_page": value})
        assert merged["product_per_page"] == value
    else:
        with pytest.raises(OverrideRefused, match="product_per_page"):
            merge_overrides(ODATA, entry, {"product_per_page": value})


def test_a_live_entry_without_product_per_page_counts_as_1000():
    live = {k: v for k, v in LIVE.items() if k != "product_per_page"}
    merge_overrides(ODATA, live, {"product_per_page": 1000})
    with pytest.raises(OverrideRefused, match="product_per_page"):
        merge_overrides(ODATA, live, {"product_per_page": 1001})


@pytest.mark.parametrize("value", ["*.EOF", [1], [""], None])
def test_download_file_pattern_is_a_list_of_non_empty_strings(value):
    with pytest.raises(OverrideRefused, match="download_file_pattern"):
        merge_overrides(ODATA, LIVE, {"download_file_pattern": value})


def test_an_empty_download_file_pattern_means_metadata_only():
    merged = merge_overrides(ODATA, LIVE, {"download_file_pattern": []})
    assert merged["download_file_pattern"] == []


@pytest.mark.parametrize(
    "name", ["", "backup-archive-a", "../other", "a/b", "a b", "x" * 129]
)
def test_interface_credentials_is_a_plain_entry_name(name):
    with pytest.raises(OverrideRefused, match="interface_credentials"):
        merge_overrides(ODATA, LIVE, {"interface_credentials": name})


@pytest.mark.parametrize("name", ["cred-b", "Cred_B.1"])
def test_a_plain_interface_credentials_name_is_accepted(name):
    merged = merge_overrides(ODATA, LIVE, {"interface_credentials": name})
    assert merged["interface_credentials"] == name


def test_the_odata_window_uses_the_live_entrys_date_field():
    live = {**LIVE, "filter_date_v4": "CreationDate"}
    with pytest.raises(OverrideRefused, match="CreationDate"):
        merge_overrides(ODATA, live, {"odata_query_filter": WINDOW})


def test_the_default_date_field_is_the_collectors():
    assert (
        DEFAULT_ODATA_DATE_FIELD
        == ODataCollectorConfiguration.__dataclass_fields__["filter_date_v4"].default
    )


def test_archive_choice():
    check_archive_choice(ODATA, None)
    check_archive_choice(JIRA, None)
    check_archive_choice(
        ODATA,
        {
            "backup": "archive-a",
            "backup_version": 3,
            "duplicates": "replace_if_changed",
            "force": True,
        },
    )
    assert normalize_archive_choice({"backup": "archive-a"}) == {
        "backup": "archive-a",
        "backup_version": None,
        "duplicates": "skip",
        "force": False,
    }


@pytest.mark.parametrize(
    "collector_class, choice, word",
    [
        (JIRA, {"backup": "archive-a"}, "archive"),
        (ODATA, {}, "backup"),
        (ODATA, {"backup": "Archive"}, "backup"),
        (ODATA, {"backup": "a" * 65}, "backup"),
        (ODATA, {"backup": "archive-a", "duplicates": "keep"}, "duplicates"),
        (ODATA, {"backup": "archive-a", "force": "yes"}, "force"),
        (ODATA, {"backup": "archive-a", "backup_version": 0}, "backup_version"),
        (ODATA, {"backup": "archive-a", "backup_version": True}, "backup_version"),
        (ODATA, {"backup": "archive-a", "fields": {}}, "fields"),
    ],
)
def test_archive_choice_refusals(collector_class, choice, word):
    with pytest.raises(OverrideRefused, match=word):
        check_archive_choice(collector_class, choice)
