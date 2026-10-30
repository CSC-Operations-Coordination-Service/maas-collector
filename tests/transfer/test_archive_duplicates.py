import pytest

from maas_collector.rawdata.transfer.duplicates import decide, same_checksum

OK1 = {
    "status": "OK",
    "catalogue_checksum": "aa",
    "written_md5": "m1",
    "start_time": "2026-10-01T00:00:00.000Z",
    "end_time": "2026-10-01T00:01:00.000Z",
}
KO1 = OK1 | {"status": "KO"}


@pytest.mark.parametrize(
    "destinations, state, duplicates, force, checksum, expected",
    [
        pytest.param(["b1", "b2"], {}, "skip", False, "aa", dict(transfer=("b1", "b2")), id="new"),
        pytest.param(["b1"], {"b1": OK1}, "skip", False, "aa", dict(skip=("b1",), same_bytes_as="m1"), id="same-skip"),
        pytest.param(["b1"], {"b1": OK1}, "skip", False, " AA ", dict(skip=("b1",), same_bytes_as="m1"), id="same-upper"),
        pytest.param(["b1"], {"b1": OK1}, "skip", False, "bb", dict(conflict=True), id="changed-skip"),
        pytest.param(["b1"], {"b1": OK1}, "replace_if_changed", False, "bb", dict(transfer=("b1",)), id="changed-replace"),
        pytest.param(["b1"], {"b1": OK1}, "replace_if_changed", False, None, dict(skip=("b1",), same_bytes_as="m1"), id="no-checksum-replace"),
        pytest.param(["b1"], {"b1": OK1 | {"catalogue_checksum": ""}}, "skip", False, "bb", dict(skip=("b1",), same_bytes_as="m1"), id="no-checksum-archived"),
        pytest.param(["b1"], {"b1": OK1}, "skip", True, "bb", dict(transfer=("b1",)), id="forced"),
        pytest.param(["b1", "b2"], {"b1": OK1}, "skip", False, "aa", dict(transfer=("b2",), skip=("b1",), same_bytes_as="m1"), id="mirror-same"),
        pytest.param(["b1", "b2"], {"b1": OK1}, "skip", False, None, dict(transfer=("b2",), skip=("b1",), same_bytes_as="m1"), id="no-checksum-mirror"),
        pytest.param(["b1", "b2"], {"b1": OK1}, "skip", False, "bb", dict(conflict=True), id="mirror-changed"),
        pytest.param(["b1"], {"b1": KO1}, "skip", False, "aa", dict(transfer=("b1",)), id="last-ko"),
        pytest.param(["b1", "b2"], {"b1": OK1}, "replace_if_changed", False, "bb", dict(transfer=("b1", "b2")), id="changed-replace-mirror"),
        pytest.param(["b1"], {"b9": OK1}, "skip", False, "bb", dict(transfer=("b1",)), id="other-destination-ignored"),
    ],
)
def test_decide(destinations, state, duplicates, force, checksum, expected):
    decision = decide(destinations, state, duplicates, force, checksum)
    defaults = dict(transfer=(), skip=(), conflict=False, same_bytes_as="")
    assert {key: getattr(decision, key) for key in defaults} == defaults | expected


def test_writing_an_archived_destination_is_a_replacement():
    assert decide(["b1", "b2"], {"b1": OK1}, "skip", True, "aa").archived == {"b1"}


def test_the_latest_archived_report_is_the_reference():
    late = OK1 | {"catalogue_checksum": "cc", "start_time": "2026-10-03T00:00:00.000Z"}
    assert decide(["b1", "b2"], {"b1": OK1, "b2": late}, "skip", False, "cc").reference == late


@pytest.mark.parametrize(
    "first, second, expected",
    [("aa", "AA", True), (" aa ", "aa", True), ("aa", "bb", False), (None, "aa", None), ("aa", " ", None)],
)
def test_same_checksum(first, second, expected):
    assert same_checksum(first, second) is expected
