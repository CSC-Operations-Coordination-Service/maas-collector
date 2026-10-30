"""Tests for the backups of configuration files: definitions, references, `@`"""

import pytest

from backup_samples import LOCAL, ODATA, S3, backups_file, entry, models_file
from maas_collector.rawdata.configprovider import (
    CONFIG_KEYS,
    ConfigIntegrityError,
    ConfigSource,
    ConfigValidationError,
    integrity_problems,
    is_config_content,
    validate_config_sources,
)
from maas_collector.rawdata.configstore import config_classes_for


def sources_of(*contents):
    return [ConfigSource(f"f{i}.json", c) for i, c in enumerate(contents)]


def validate(*contents, **kwargs):
    validate_config_sources(sources_of(*contents), config_classes_for, **kwargs)


def test_backups_is_a_configuration_key():
    assert CONFIG_KEYS == {"collectors", "models", "amqp", "backups"}
    assert is_config_content({"backups": []})


def test_a_backups_only_file_validates():
    validate(backups_file(LOCAL, S3))


def test_an_entry_naming_a_backup_of_another_file_validates():
    validate(backups_file(LOCAL), {"collectors": [entry(backup="archive-a")]})


@pytest.mark.parametrize("enabled", [True, False])
def test_an_unknown_name_is_refused_even_on_a_disabled_entry(enabled):
    with pytest.raises(ConfigIntegrityError) as error:
        validate({"collectors": [entry(backup="nope", enabled=enabled)]})
    assert "ITF_A" in error.value.problems[0] and "nope" in error.value.problems[0]


def test_a_single_file_check_does_not_resolve_names():
    validate({"collectors": [entry(backup="nope")]}, resolve_models=False)


def test_every_dangling_reference_is_listed():
    with pytest.raises(ConfigIntegrityError) as error:
        validate(
            {
                "collectors": [
                    entry("ITF_A", backup="nope"),
                    entry("ITF_B", archive={"backup": "gone"}, **{"class": ODATA}),
                ]
            }
        )
    assert len(error.value.problems) == 2


@pytest.mark.parametrize("resolve", [True, False])
def test_a_secret_in_an_inline_block_or_a_definition_is_refused(resolve):
    inline = {
        "type": "BackupS3",
        "directory": "d",
        "buckets": ["b"],
        "credentials": "backup-x",
        "s3_secret_key": "x",
    }
    with pytest.raises(ConfigValidationError, match="s3_secret_key"):
        validate({"collectors": [entry(backup=inline)]}, resolve_models=resolve)
    with pytest.raises(ConfigValidationError, match="s3_access_key"):
        validate(backups_file(S3 | {"s3_access_key": "x"}), resolve_models=resolve)


@pytest.mark.parametrize("resolve", [True, False])
def test_an_inline_s3_block_naming_credentials_validates(resolve):
    inline = {
        "type": "BackupS3",
        "directory": "d",
        "buckets": ["b"],
        "credentials": "backup-x",
    }
    validate({"collectors": [entry(backup=inline)]}, resolve_models=resolve)


def test_a_local_inline_block_still_validates():
    validate({"collectors": [entry(backup={"type": "BackupLocal", "directory": "/d"})]})


@pytest.mark.parametrize("backup", [["a"], 3, True])
def test_a_backup_that_is_neither_a_name_nor_a_block_is_refused(backup):
    with pytest.raises(ConfigValidationError, match="backup"):
        validate({"collectors": [entry(backup=backup)]}, resolve_models=False)


@pytest.mark.parametrize("name", ["Bad_Name", "a" * 65])
def test_a_backup_name_that_breaks_the_name_rule_is_refused(name):
    with pytest.raises(ConfigValidationError):
        validate({"collectors": [entry(backup=name)]}, resolve_models=False)


@pytest.mark.parametrize("resolve", [True, False])
def test_the_same_backup_for_the_file_backup_and_the_archive_is_refused_in_both_modes(
    resolve,
):
    content = {
        "collectors": [
            entry(backup="archive-a", archive={"backup": "archive-a"}, **{"class": ODATA})
        ]
    }
    with pytest.raises(ConfigValidationError, match="archive-a"):
        validate(backups_file(LOCAL), content, resolve_models=resolve)


def test_different_backups_for_the_file_backup_and_the_archive_validate():
    validate(
        backups_file(LOCAL, S3),
        {
            "collectors": [
                entry(
                    backup="archive-a",
                    archive={"backup": "archive-b"},
                    **{"class": ODATA},
                )
            ]
        },
    )


@pytest.mark.parametrize("enabled", [True, False])
def test_an_interface_name_with_at_is_refused(enabled):
    with pytest.raises(ConfigValidationError, match="@"):
        validate(
            {"collectors": [entry("ITF_A@custom", enabled=enabled)]},
            resolve_models=False,
        )


def test_a_report_model_of_a_definition_must_be_defined():
    with pytest.raises(ConfigIntegrityError, match="ReportM"):
        validate(backups_file(LOCAL | {"report_model": "ReportM"}))
    # in another file
    validate(backups_file(LOCAL | {"report_model": "ReportM"}), models_file("ReportM"))
    # an inline model
    validate(
        backups_file(LOCAL | {"report_model": "ModelA"}), {"collectors": [entry()]}
    )


def test_a_name_defined_in_two_files_is_refused_and_in_one_file_even_alone():
    with pytest.raises(ConfigIntegrityError, match="both"):
        validate(backups_file(LOCAL), backups_file(LOCAL))
    with pytest.raises(ConfigValidationError):
        validate(backups_file(LOCAL, LOCAL), resolve_models=False)


def test_integrity_problems_is_empty_for_a_consistent_set():
    sources = sources_of(
        backups_file(LOCAL | {"report_model": "ModelA"}),
        {"collectors": [entry(backup="archive-a")]},
    )
    assert integrity_problems(sources) == []


def test_integrity_problems_start_with_the_file_path():
    sources = sources_of({"collectors": [entry(backup="nope")]})
    assert integrity_problems(sources) == [
        "f0.json: interface ITF_A uses backup nope, which is not defined"
    ]
