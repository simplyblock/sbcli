# coding=utf-8
"""
conftest.py – shared fixtures for all tests in tests/.

Clears the DBController Singleton cache before each test to prevent
state leakage between test modules.
"""

import pytest


@pytest.fixture(autouse=True)
def _clear_singleton_cache():
    """Clear DBController Singleton cache before and after each test."""
    from simplyblock_core.db_controller import Singleton
    Singleton._instances.clear()
    yield
    Singleton._instances.clear()
