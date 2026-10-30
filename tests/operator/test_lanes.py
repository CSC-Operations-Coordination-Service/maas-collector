import pytest

from maas_collector.rawdata.operator.lanes import (
    SINGLE_LANE,
    accepts_custom,
    custom_lane_name,
    slot_of,
)


def test_names():
    assert SINGLE_LANE == "single"
    assert custom_lane_name("short") == "custom-short"
    assert custom_lane_name("long") == "custom-long"


@pytest.mark.parametrize(
    "lane, slot",
    [
        ("short", "short"),
        ("long", "long"),
        ("custom-short", "short"),
        ("custom-long", "long"),
        ("single", "single"),
    ],
)
def test_a_custom_lane_shares_the_slot_of_its_plain_lane(lane, slot):
    assert slot_of(lane) == slot


@pytest.mark.parametrize(
    "lane, accepted",
    [
        ("custom-short", True),
        ("custom-long", True),
        ("single", True),
        ("short", False),
        ("long", False),
    ],
)
def test_only_custom_and_single_lanes_accept_a_custom_request(lane, accepted):
    assert accepts_custom(lane) is accepted
