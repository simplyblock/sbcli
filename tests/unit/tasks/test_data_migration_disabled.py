# coding=utf-8
"""Regression tests for the distrib data-migration kill switch.

Background: the 2026-08-27 soak reproduced silent data corruption in six
iterations. It was traced to a concurrent ``balancing_on_restart`` rebalance
relocating individual stripes -- single-stripe ``[267,268)`` ranges on
``res_loc=2`` devices -- while writes were in flight, with the lvstore primary
and secondary disagreeing on which device held the stripe. Reads afterwards
returned another location's bytes: high entropy, no fio 0xacca header.

This branch disables distrib data migration outright so the corruption can be
isolated. "Disabled" has to mean *cannot run*, not "is not scheduled", so the
switch is enforced at four independent layers and each is asserted here:

  1. no migration-family task can be created  (tasks_controller._add_task)
  2. the distr_migration_*_start RPCs refuse   (rpc_client)
  3. each migration runner main() exits at once
  4. the runner services are absent from the swarm stack

Layers 1-3 are asserted behaviourally by calling the real functions; layer 4
by parsing the compose file. Together they mean a stray runner, a queued task
or a hand-rolled RPC all fail closed.
"""
import io
import os

import pytest
import yaml
from pydantic import SecretStr

from simplyblock_core import constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.rpc_client import RPCClient


DATA_MIGRATION_FUNCTIONS = [
    JobSchedule.FN_DEV_MIG,
    JobSchedule.FN_NEW_DEV_MIG,
    JobSchedule.FN_FAILED_DEV_MIG,
    JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
    JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
    JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION,
]

#: Deliberately unroutable: if a guard ever stops short-circuiting, the call
#: blocks on the network instead of returning, so the test fails loudly.
BLACKHOLE_HOST = "203.0.113.1"

COMPOSE = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                       "simplyblock_core", "scripts", "docker-compose-swarm.yml")

REMOVED_SERVICES = [
    "TasksRunnerMigration",
    "TasksRunnerFailedMigration",
    "TasksRunnerNewDeviceMigration",
]

RUNNER_MODULES = [
    "simplyblock_core.services.tasks_runner_migration",
    "simplyblock_core.services.tasks_runner_failed_migration",
    "simplyblock_core.services.tasks_runner_new_dev_migration",
]


def test_switch_is_off():
    assert constants.DATA_MIGRATION_ENABLED is False


@pytest.mark.parametrize("function_name", DATA_MIGRATION_FUNCTIONS)
def test_migration_task_cannot_be_created(function_name):
    """The guard must precede any DB access, so no cluster/node need exist."""
    assert tasks_controller._add_task(
        function_name, "no-such-cluster", "no-such-node", "no-such-device",
        function_params={"distr_name": "distrib_18"}) is False


def test_migration_rpcs_refuse_without_touching_the_network():
    client = RPCClient(BLACKHOLE_HOST, 5260, "u", SecretStr("p"), timeout=1)
    assert client.distr_migration_expansion_start("distrib_18") is False
    assert client.distr_migration_failure_start("distrib_18", 3) is False
    assert client.distr_migration_to_primary_start(3, "distrib_18") is False


@pytest.mark.parametrize("module_name", RUNNER_MODULES)
def test_runner_main_exits_immediately(module_name):
    """main() is an unbounded ``while True``; returning proves it never entered."""
    import importlib
    module = importlib.import_module(module_name)
    assert module.main() is None


@pytest.mark.parametrize("service", REMOVED_SERVICES)
def test_runner_service_absent_from_swarm_stack(service):
    with io.open(COMPOSE, encoding="utf-8") as handle:
        stack = yaml.safe_load(handle.read())
    assert service not in stack["services"]

