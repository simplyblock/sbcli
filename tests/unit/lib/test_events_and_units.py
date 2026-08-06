# coding=utf-8
"""Unit tests for simplyblock_lib.events and simplyblock_lib.units."""
import logging

import pytest

from simplyblock_lib import events, units


# --------------------------------------------------------------------- events

@pytest.mark.parametrize("event_level,logging_level", [
    (events.LEVEL_DEBUG, logging.DEBUG),
    (events.LEVEL_INFO, logging.INFO),
    (events.LEVEL_WARN, logging.WARNING),
    (events.LEVEL_ERROR, logging.ERROR),
    (events.LEVEL_CRITICAL, logging.CRITICAL),
])
def test_log_at_level_maps_severity(caplog, event_level, logging_level):
    logger = logging.getLogger("test.events")
    with caplog.at_level(logging.DEBUG, logger="test.events"):
        events.log_at_level(logger, event_level, "hello")
    assert caplog.records[-1].levelno == logging_level
    assert caplog.records[-1].message == "hello"


def test_log_at_level_unknown_severity_defaults_to_info(caplog):
    logger = logging.getLogger("test.events")
    with caplog.at_level(logging.DEBUG, logger="test.events"):
        events.log_at_level(logger, "Bogus", "hello")
    assert caplog.records[-1].levelno == logging.INFO


def test_level_names_match_event_model():
    """The lib severity names must stay identical to EventObj's."""
    from simplyblock_core.models.events import EventObj
    assert events.LEVEL_DEBUG == EventObj.LEVEL_DEBUG
    assert events.LEVEL_INFO == EventObj.LEVEL_INFO
    assert events.LEVEL_WARN == EventObj.LEVEL_WARN
    assert events.LEVEL_ERROR == EventObj.LEVEL_ERROR
    assert events.LEVEL_CRITICAL == EventObj.LEVEL_CRITICAL


# ---------------------------------------------------------------------- units

@pytest.mark.parametrize("value,expected", [
    ("4096", 4096),
    ("1kB", 1000),
    ("1KiB", 1024),
    ("2 MiB", 2 * 1024 ** 2),
    ("1GB", 10 ** 9),
    ("1GiB", 2 ** 30),
    (512, 512),
])
def test_parse_size(value, expected):
    assert units.parse_size(value) == expected


def test_parse_size_uppercase_decimal_kilo_is_invalid():
    # Long-standing quirk kept for parity: in si/iec mode the decimal kilo
    # prefix must be lowercase ('1kB'); '1KB' is rejected.
    assert units.parse_size("1KB") == -1


def test_parse_size_assume_unit():
    assert units.parse_size(1, assume_unit='GiB') == 2 ** 30


def test_parse_size_invalid_returns_minus_one():
    assert units.parse_size("garbage") == -1
    assert units.parse_size("12XB") == -1


def test_core_utils_reexport_is_same_function():
    from simplyblock_core import utils as core_utils
    assert core_utils.parse_size is units.parse_size
    assert core_utils._parse_unit is units._parse_unit
