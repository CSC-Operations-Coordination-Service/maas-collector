from maas_collector.rawdata.configschema import configuration_schema


def test_schema_describes_the_archive_key():
    schema = configuration_schema()
    odata = schema["collector_classes"]["ODataCollectorConfiguration"]
    assert odata["archivable"] is True
    assert (
        odata["keys"]["archive"]["group"] == "archive"
        and odata["keys"]["archive"]["description"]
    )
    assert "archive_rule" not in odata["keys"]
    # not backup_version, not force
    assert set(schema["archive_rule"]["keys"]) == {
        "backup",
        "duplicates",
        "fields",
        "download",
    }
    assert [
        name
        for name, cls in schema["collector_classes"].items()
        if cls["archivable"]
    ] == ["ODataCollectorConfiguration"]
