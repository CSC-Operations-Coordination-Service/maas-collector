"""The maas-engine floor of the operator extra: it must refuse the release that has no
replay worker, and accept the develop builds that have it"""

import configparser
import pathlib

import pytest
from packaging.requirements import Requirement

SETUP_CFG = pathlib.Path(__file__).parent.parent.parent / "setup.cfg"


def engine_requirements():
    config = configparser.ConfigParser(interpolation=None)
    config.read(SETUP_CFG, encoding="utf-8")
    found = {}
    for section, key in (
        ("options.extras_require", "operator"),
        ("options.extras_require", "testing"),
    ):
        lines = [
            line.strip()
            for line in config[section][key].splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        found[key] = [
            Requirement(l) for l in lines if Requirement(l).name == "maas-engine"
        ]
    return found


@pytest.mark.parametrize("extra", ["operator", "testing"])
def test_the_engine_floor_accepts_develop_builds_and_refuses_the_release_without_replay(
    extra,
):
    [engine] = engine_requirements()[extra]
    spec = engine.specifier
    # the develop builds of maas-engine (no-guess-dev after the 3.1.0 tag)
    assert spec.contains("3.1.0.post1.dev1", prereleases=True)
    assert spec.contains("3.2.0", prereleases=True)
    # 3.1.0 has no maas_engine.replay
    assert not spec.contains("3.1.0", prereleases=True)
    assert not spec.contains("3.0.1", prereleases=True)
    assert not spec.contains("4.0.0", prereleases=True)
