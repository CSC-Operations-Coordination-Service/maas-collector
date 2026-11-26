"""Parsing of SAR-MPC Quality Disclaimer text.

The texts below are cut from what pdfplumber extracts from real disclaimers
(SAR-MPC, 2026-11-26). They keep the shapes that the first version of the
parser missed: the title block garbled by the logo, the number on its own line,
and product names listed without ``.SAFE``.
"""

import pytest

from maas_collector.rawdata.collector.disclaimer.pdf_extract import (
    parse_quality_disclaimer,
    parse_sentinel1_product,
)

BODY = """Title:
Invalid burst ID annotations (S1A IW SLC in cycle 322)
Description:
Since version 003.40 for the SAR processor (IPF), the IW SLC products contain an
annotation of burst ID.
Degradation types:
Impacted products:
S1A_IW_SLC__1SDV_20240510T233753_20240510T233829_053815_068A3C_5826.SAFE
S1A_IW_SLC__1SDV_20240510T233818_20240510T233845_053815_068A3C_0D1B.SAFE
"""


@pytest.mark.parametrize(
    "head, number",
    [
        # the layout the parser was written for
        ("SENTINEL-1 Quality Disclaimer\nNo. 33\n", 33),
        # #1: a space after the S, number on the same line
        ("S ENTINEL-1 Quality Disclaimer No. 1\n", 1),
        # #215: a digit in the logo, number on the next line
        ("S9E NTINEL-1 Quality Disclaimer\nNo. 215\n", 215),
        # #300: two digits in the logo, "No" without a dot
        ("S9E2N TINEL-1 Quality Disclaimer No 300\n", 300),
        # #236: the number before the title
        ("No. 236\nS9E NTINEL-1 Quality Disclaimer\n", 236),
        # #418
        ("S8E NTINEL-1 Quality Disclaimer\nNo. 418\n", 418),
    ],
)
def test_header_variants(head, number):
    out = parse_quality_disclaimer(head + BODY)
    assert out is not None
    assert out["mission"] == "Sentinel-1"
    assert out["disclaimer_number"] == number
    assert out["title"] == "Invalid burst ID annotations (S1A IW SLC in cycle 322)"
    assert out["impacted_products_count"] == 2


def test_not_a_disclaimer():
    assert parse_quality_disclaimer("Some other document\nNo. 3\n") is None


def test_header_without_number():
    assert parse_quality_disclaimer("SENTINEL-1 Quality Disclaimer\n" + BODY) is None


def test_names_without_safe_are_kept_as_written():
    # #33 lists its products without the suffix
    text = (
        "SENTINEL-1 Quality Disclaimer\nNo. 33\n"
        "Title:\nS-1A products processed without Restituted Orbit Files\n"
        "Impacted products:\n"
        "S1A_EW_SLC__1SDH_20180322T062254_20180322T062400_021124_0244D5_846E\n"
        "S1A_IW_GRDH_1SDV_20180321T214252_20180321T214321_021119_0244AD_983B\n"
    )
    out = parse_quality_disclaimer(text)
    assert out["impacted_product_names"] == [
        "S1A_EW_SLC__1SDH_20180322T062254_20180322T062400_021124_0244D5_846E",
        "S1A_IW_GRDH_1SDV_20180321T214252_20180321T214321_021119_0244AD_983B",
    ]
    assert out["impacted_products_summary"]["product_types"] == ["GRDH", "SLC"]


# Names are built from their fields to keep the lines short.
_GRD = "S1C_IW_GRDH_1SDV_20260625T152305_20260625T152336_008264_01056B_F534_COG"
_SLC = "S1A_EW_SLC__1SDH_20180322T062254_20180322T062400_021124_0244D5_846E"
_RAW = "S1A_IW_RAW__0{}SH_20221129T154420_20221129T154420_046111_05850A_FA64.SAFE"
_ETA = "S1A_IW_ETA__AXDV_20230724T173232_20230724T173251_049568_05F5D9_1DEF.SAFE"


@pytest.mark.parametrize(
    "name, ptype, level, cog",
    [
        (_GRD + ".SAFE", "GRDH", 1, True),
        (_GRD, "GRDH", 1, True),
        (_SLC, "SLC", 1, False),
        # #343: Level-0 classes C and N, besides S and A
        (_RAW.format("C"), "RAW", 0, False),
        (_RAW.format("N"), "RAW", 0, False),
        (_RAW.format("A"), "RAW", 0, False),
        (_RAW.format("S"), "RAW", 0, False),
        # #348: ETA products, level A and class X
        (_ETA, "ETA", None, False),
        # #46: polarisation VV
        (_RAW.format("S").replace("0SSH", "0SVV"), "RAW", 0, False),
    ],
)
def test_product_names(name, ptype, level, cog):
    out = parse_sentinel1_product(name)
    assert out is not None
    assert out["name"] == name
    assert out["product_type"] == ptype
    assert out["cog"] is cog
    assert out["processing_level"] == level


@pytest.mark.parametrize(
    "line",
    [
        # prose and figure captions are not product lines
        "Figure 1: S1A_IW_GRDH_1ADV_20221114T054021_20221114T054046_045886_057D6E",
        # a wildcard pattern, not a product
        "S1A_EW_*SDH_20221103T191346_20221103T191450_045734_057848_*",
        # ARD products are not cds-product names
        "S1C_IW_GRDH_1SDV_20251021T152916_20251021T152941_004662_009372_A800"
        "_CARD_BS",
        # a name cut at the end of a line
        _SLC + ".",
    ],
)
def test_not_product_names(line):
    assert parse_sentinel1_product(line) is None


def test_names_in_table_rows():
    # #59 lists each product with its datatake, orbit and times on the same line
    text = (
        "SENTINEL-1 Quality Disclaimer\nNo. 59\n"
        "Title:\nSome title\n"
        "Impacted products:\n"
        "S1A_IW_SLC__1SDV_20150323T171452_20150323T171518_005162_00681A_25C0 00681A"
        " 005162 15 2015-03-23 17:14:52\n"
        "S1A_IW_SLC__1SDV_20150323T171427_20150323T171454_005162_00681A_757F 00681A"
        " 005162 15 2015-03-23 17:14:27\n"
        "AUX_WAV S1__AUX_WAV_V20240708T210000_G20240703T043437.SAFE\n"
    )
    out = parse_quality_disclaimer(text)
    assert out["impacted_product_names"] == [
        "S1A_IW_SLC__1SDV_20150323T171452_20150323T171518_005162_00681A_25C0",
        "S1A_IW_SLC__1SDV_20150323T171427_20150323T171454_005162_00681A_757F",
    ]
