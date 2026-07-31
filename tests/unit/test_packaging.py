# coding=utf-8
"""Guards the pyproject.toml declarations whose consumers live elsewhere: the
console-script names, and the package-data globs that put runtime resources in
the wheel."""

import pathlib
import sys

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from simplyblock_core import constants


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def pyproject():
    with open(REPO_ROOT / "pyproject.toml", "rb") as fh:
        return tomllib.load(fh)


def test_cli_name_has_a_console_script(pyproject):
    """tasks_cluster_status.py shells out to `SIMPLY_BLOCK_CLI_NAME`."""
    assert constants.SIMPLY_BLOCK_CLI_NAME in pyproject["project"]["scripts"]


def test_release_command_name_is_declared(pyproject):
    """release.yml seds env_var to `sbctl`; the script has to survive that."""
    assert "sbctl" in pyproject["project"]["scripts"]


@pytest.mark.parametrize("resource", [
    # constants.get_config_var() opens this next to constants.py.
    "simplyblock_core/env_var",
    "simplyblock_core/scripts/docker-compose-swarm.yml",
    "simplyblock_core/scripts/deploy_stack.sh",
    "simplyblock_core/scripts/dashboards/cluster.json",
    "simplyblock_core/scripts/alerting/alert_rules.yaml",
    "simplyblock_core/services/service_template.service",
    # Run as `python3 simplyblock_core/workers/...` by docker-compose-swarm.yml.
    "simplyblock_core/workers/cleanup_foundationdb.py",
    "simplyblock_web/templates/storage_deploy_spdk.yaml.j2",
    "simplyblock_web/static/openapi.json",
    "simplyblock_web/api/v1/static/swagger.yaml",
])
def test_runtime_resource_is_shipped(pyproject, resource):
    package, _, relative = resource.partition("/")
    package_dir = REPO_ROOT / package
    patterns = pyproject["tool"]["setuptools"]["package-data"].get(package, [])

    shipped = {
        path.relative_to(package_dir).as_posix()
        for pattern in patterns
        for path in package_dir.glob(pattern)
        if path.is_file()
    }

    assert (package_dir / relative).is_file(), f"{resource} does not exist"
    assert relative in shipped, \
        f"{resource} is not shipped: no package-data glob for it under '{package}'"
