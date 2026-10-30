"""Satellite unavailability reports (REP__SUP__) extracted with the XMLExtractor"""

import os

from opensearchpy import (
    Text,
    Integer,
    Keyword,
)

from maas_model import MAASRawDocument, ZuluDate
from maas_collector.rawdata.extractor.xml import XMLExtractor


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

NEW_FORMAT_FILE = os.path.join(
    DATA_DIR,
    "Satu/S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001-NEW.EOF.xml",
)

# Extractor args of the Satellite-Unavailability collector. Header and report
# fields are anchored on the root, subsystem fields on each iterated node.
# Both report formats are read: the subsystem text is <Comment> in the first one,
# <Description> in the "Unavailability Tool" one, which also adds the identifier,
# the status and the category.
SATU_EXTRACTOR_ARGS = {
    "iterate_nodes": "Data_Block/List_Of_Subsystem_Unavailabilities/Subsystem_Unavailability",
    "allow_partial": True,
    "attr_map": {
        "file_name": {"root_path": "Earth_Explorer_Header/Fixed_Header/File_Name"},
        "description": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/File_Description"
        },
        "notes": {"root_path": "Earth_Explorer_Header/Fixed_Header/Notes"},
        "mission": {"root_path": "Earth_Explorer_Header/Fixed_Header/Mission"},
        "file_class": {"root_path": "Earth_Explorer_Header/Fixed_Header/File_Class"},
        "file_type": {"root_path": "Earth_Explorer_Header/Fixed_Header/File_Type"},
        "validity_start": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start"
        },
        "validity_stop": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop"
        },
        "file_version": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/File_Version"
        },
        "source_system": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/Source/System"
        },
        "source_creator": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/Source/Creator"
        },
        "source_creation_date": {
            "root_path": "Earth_Explorer_Header/Fixed_Header/Source/Creation_Date"
        },
        "unique_identifier": {"root_path": "Data_Block/Unique_Identifier"},
        "unavailability_reference": {
            "root_path": "Data_Block/Unavailability_Reference"
        },
        "unavailability_type": {"root_path": "Data_Block/Unavailability_Type"},
        "unavailability_status": {"root_path": "Data_Block/Unavailability_Status"},
        "subsystem": "Subsystem",
        "start_time": "StartTime",
        "start_doy": "StartDOY",
        "start_orbit": "StartOrbit",
        "start_anx_offset": "StartAnxOffset",
        "end_time": "EndTime",
        "end_doy": "EndDOY",
        "end_orbit": "EndOrbit",
        "end_anx_offset": "EndAnxOffset",
        "type": "Type",
        "category": "Category",
        "comment": {
            "python": "lambda e: e.findtext('Comment') or e.findtext('Description')"
        },
        "interface_name": {"python": "lambda c: 'Satellite-Unavailability'"},
        "production_service_type": {"python": "lambda c: 'AUXIP'"},
        "production_service_name": {"python": "lambda c: 'Exprivia'"},
    },
    # start_time and end_time keep their "UTC=" prefix: they are part of the
    # document id and the CDS consolidation strips it.
    "converter_map": {
        "validity_start": {
            "type": "python",
            "python": "lambda v: v.removeprefix('UTC=') + 'Z' if v else None",
        },
        "validity_stop": {
            "type": "python",
            "python": "lambda v: v.removeprefix('UTC=') + 'Z' if v else None",
        },
        "source_creation_date": {
            "type": "python",
            "python": "lambda v: v.removeprefix('UTC=') + 'Z' if v else None",
        },
        "start_orbit": {
            "type": "python",
            "python": "lambda v: str(int(v)) if v.strip().isdigit() else None",
        },
        "end_orbit": {
            "type": "python",
            "python": "lambda v: str(int(v)) if v.strip().isdigit() else None",
        },
        "start_anx_offset": {
            "type": "python",
            "python": "lambda v: int(v) if v.strip().isdigit() else None",
        },
        "end_anx_offset": {
            "type": "python",
            "python": "lambda v: int(v) if v.strip().isdigit() else None",
        },
        "start_doy": {
            "type": "python",
            "python": "lambda v: int(v) if v.strip().isdigit() else None",
        },
        "end_doy": {
            "type": "python",
            "python": "lambda v: int(v) if v.strip().isdigit() else None",
        },
    },
}


class SatUnavailabilityProduct(MAASRawDocument):
    class Index:
        name = "raw-data-sat-unavailability-product"

    _PARTITION_FIELD = "ingestionTime"
    _PARTITION_FIELD_FORMAT = "static"

    category = Keyword()

    comment = Keyword()

    description = Text()

    end_anx_offset = Integer()

    end_doy = Integer()

    end_orbit = Keyword()

    end_time = Keyword()

    file_class = Keyword()

    file_name = Keyword()

    file_type = Keyword()

    file_version = Keyword()

    interface_name = Keyword()

    mission = Keyword()

    notes = Text()

    production_service_name = Keyword()

    production_service_type = Keyword()

    source_creation_date = ZuluDate()

    source_creator = Keyword()

    source_system = Keyword()

    start_anx_offset = Integer()

    start_doy = Integer()

    start_orbit = Keyword()

    start_time = Keyword()

    subsystem = Keyword()

    type = Keyword()

    unavailability_reference = Keyword()

    unavailability_status = Keyword()

    unavailability_type = Keyword()

    unique_identifier = Keyword()

    validity_start = ZuluDate()

    validity_stop = ZuluDate()


def extract_products(path):
    """extract and validate every subsystem unavailability of a report"""
    products = []
    for extract in XMLExtractor(**SATU_EXTRACTOR_ARGS).extract(path):
        product = SatUnavailabilityProduct(**extract)
        product.full_clean()
        products.append(product.to_dict())
    return products


def test_satu_extractor():
    products = extract_products(
        os.path.join(
            DATA_DIR,
            "S5P_OPER_REP__SUP___20221007T012935_20221007T035455_0001.EOF.xml",
        )
    )

    assert products == [
        {
            "comment": "Due to a collision risk identified for Sentinel-5P on 07/10/2022 "
            "a Collision Avoidance Manoeuvre (CAM) has been scheduled. Refer "
            "to event #27468 in SCARF for further details.",
            "description": "FOS and Spacecraft Unavailability Report",
            "end_doy": 280,
            "end_orbit": "25818",
            "end_time": "UTC=2022-10-07T03:54:55",
            "file_class": "OPER",
            "file_name": "S5P_OPER_REP__SUP___20221007T012935_20221007T035455_0001",
            "file_type": "REP__SUP__",
            "file_version": "0001",
            "interface_name": "Satellite-Unavailability",
            "mission": "Sentinel-5P",
            "notes": "",
            "production_service_name": "Exprivia",
            "production_service_type": "AUXIP",
            "reportName": "S5P_OPER_REP__SUP___20221007T012935_20221007T035455_0001.EOF.xml",
            "source_creation_date": "2022-10-06T16:54:04.000Z",
            "source_creator": "MCS",
            "source_system": "FOS",
            "start_doy": 280,
            "start_orbit": "25817",
            "start_time": "UTC=2022-10-07T01:29:35",
            "subsystem": "S/C Manoeuvre",
            "type": "Planned",
            "unavailability_reference": "S5P-UNA-2022/0009",
            "unavailability_type": "Return to Operations",
            "validity_start": "2022-10-07T01:29:35.000Z",
            "validity_stop": "2022-10-07T03:54:55.000Z",
        }
    ]


def test_satu_extractor_format_v1():
    products = extract_products(
        os.path.join(
            DATA_DIR,
            "Satu/S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001-OLD.EOF.xml",
        )
    )

    assert [product["subsystem"] for product in products] == ["OCP", "PDHT", "SAR"]

    assert products[0] == {
        "comment": "The PDHT was unavailable due to a Passthrough Blockage anomaly re-occurrence (GS1_SC-283) on 02/12/2025 at 15:23:25 UTC. The OCP link with T0 = 16:43:58 UTC was lost.",
        "description": "FOS and Spacecraft Unavailability Report",
        "end_anx_offset": 4297,
        "end_doy": 336,
        "end_orbit": "62139",
        "end_time": "UTC=2025-12-02T17:15:37",
        "file_class": "OPER",
        "file_name": "S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001",
        "file_type": "REP__SUP__",
        "file_version": "0001",
        "interface_name": "Satellite-Unavailability",
        "mission": "Sentinel-1A",
        "notes": "",
        "production_service_name": "Exprivia",
        "production_service_type": "AUXIP",
        "reportName": "S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001-OLD.EOF.xml",
        "source_creation_date": "2025-12-02T00:00:00.000Z",
        "source_creator": "MCS",
        "source_system": "FOS",
        "start_anx_offset": 3489,
        "start_doy": 336,
        "start_orbit": "62138",
        "start_time": "UTC=2025-12-02T15:23:25",
        "subsystem": "OCP",
        "type": "UNPLANNED",
        "unavailability_reference": "S1A-UNA-2025/0039",
        "unavailability_type": "Return to Operations",
        "validity_start": "2025-12-02T15:23:25.000Z",
        "validity_stop": "2025-12-02T17:15:37.000Z",
    }


def test_satu_extractor_format_v2():
    products = extract_products(NEW_FORMAT_FILE)

    assert [product["category"] for product in products] == [
        "Secondary",
        "Primary",
        "Secondary",
    ]

    assert products[0] == {
        "category": "Secondary",
        "comment": "The PDHT was unavailable due to a Passthrough Blockage anomaly re-occurrence (GS1_SC-283) on 02/12/2025 at 15:23:25 UTC. The OCP link with T0 = 16:43:58 UTC was lost.",
        "description": "FOS and Spacecraft Unavailability Report",
        "end_anx_offset": 4297,
        "end_doy": 336,
        "end_orbit": "62139",
        "end_time": "UTC=2025-12-02T17:15:37",
        "file_class": "OPER",
        "file_name": "S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001",
        "file_type": "REP__SUP__",
        "file_version": "0001",
        "interface_name": "Satellite-Unavailability",
        "mission": "Sentinel-1A",
        "notes": "",
        "production_service_name": "Exprivia",
        "production_service_type": "AUXIP",
        "reportName": "S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001-NEW.EOF.xml",
        "source_creation_date": "2025-12-02T00:00:00.000Z",
        "source_creator": "Unavailability Tool",
        "source_system": "FOS",
        "start_anx_offset": 3489,
        "start_doy": 336,
        "start_orbit": "62138",
        "start_time": "UTC=2025-12-02T15:23:25",
        "subsystem": "OCP",
        "type": "Unplanned",
        "unavailability_reference": "S1A-UNA-2025/0039",
        "unavailability_status": "Closed",
        "unavailability_type": "Unplanned",
        "unique_identifier": "3e3370bb-e9c6-40b1-b582-3d2f2d1c6584",
        "validity_start": "2025-12-02T15:23:25.000Z",
        "validity_stop": "2025-12-02T17:15:37.000Z",
    }


def test_satu_extractor_ground_segment_without_orbit(tmp_path):
    """ground segment unavailabilities come with empty orbits and anx offsets"""
    with open(NEW_FORMAT_FILE, encoding="utf-8") as report:
        content = report.read()
    for tag, value in (
        ("StartOrbit", "62138"),
        ("EndOrbit", "62139"),
        ("StartAnxOffset", "3489"),
        ("EndAnxOffset", "4297"),
    ):
        content = content.replace(f"<{tag}>{value}</{tag}>", f"<{tag}/>")
    path = tmp_path / "S1A_OPER_REP__SUP___20251202T152325_20251202T171537_0001.EOF.xml"
    path.write_text(content, encoding="utf-8")

    product = extract_products(str(path))[0]

    for field in ("start_orbit", "end_orbit", "start_anx_offset", "end_anx_offset"):
        assert field not in product
    assert product["comment"].startswith("The PDHT was unavailable")
