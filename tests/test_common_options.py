"""The environment variables and defaults described in docs/common_options.md"""

import pytest

from maas_collector.rawdata.cli.lib.args import common_parser, es_parser

CONNECTION_OPTIONS = (
    "es_url",
    "es_username",
    "es_password",
    "amqp_url",
    "amqp_username",
    "amqp_password",
)


@pytest.fixture
def clean_env(monkeypatch):
    """no option set from the environment of the test run"""
    for name in (
        "IGNORE_CERTS_VERIFICATION",
        "ES_IGNORE_CERTS_VERIFICATION",
        "WORKING_DIR",
        "WORKING_DIRECTORY",
        "HEALTHCHECK_PORT",
        *(option.upper() for option in CONNECTION_OPTIONS),
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


@pytest.mark.parametrize(
    "value,ignored",
    [(None, False), ("", False), ("False", True), ("0", True), ("true", True)],
)
def test_any_ignore_certs_verification_value_skips_the_verification(
    clean_env, value, ignored
):
    """the option is a Python bool: any non-empty value is true, even False or 0"""
    if value is not None:
        clean_env.setenv("IGNORE_CERTS_VERIFICATION", value)

    assert es_parser().parse_args([]).es_ignore_certs_verification is ignored


def test_es_prefixed_ignore_certs_verification_is_not_read(clean_env):
    clean_env.setenv("ES_IGNORE_CERTS_VERIFICATION", "true")

    assert es_parser().parse_args([]).es_ignore_certs_verification is False


def test_ignore_certs_verification_option_reads_its_value_the_same_way(clean_env):
    namespace = es_parser().parse_args(["--es-ignore-certs-verification", "False"])

    assert namespace.es_ignore_certs_verification is True


def test_working_directory_comes_from_working_dir(clean_env):
    clean_env.setenv("WORKING_DIRECTORY", "/not/read")
    assert common_parser().parse_args([]).working_directory == "/tmp"

    clean_env.setenv("WORKING_DIR", "/data/work")
    assert common_parser().parse_args([]).working_directory == "/data/work"


def test_defaults(clean_env):
    namespace = common_parser().parse_args([])

    assert namespace.healthcheck_port == 80
    for option in CONNECTION_OPTIONS:
        assert getattr(namespace, option) is None
