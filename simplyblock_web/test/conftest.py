from pathlib import Path

from simplyblock_web.test import util


def pytest_addoption(parser):
    for opt in util.OPTIONS:
        parser.addoption(f"--{opt}", action="store")


def pytest_generate_tests(metafunc):
    for opt in util.OPTIONS:
        if opt in metafunc.fixturenames:
            metafunc.parametrize(
                opt,
                [metafunc.config.getoption(opt)] if hasattr(metafunc.config.option, opt) else [],
                scope='session',
            )


def pytest_ignore_collect(collection_path: Path, config):
    return len({opt for opt in util.OPTIONS if getattr(config.option, opt) is None}) > 0
