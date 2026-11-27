"""Shared fixtures: run ArrayClient against a vendor fake (in-process) or,
with ``SB_ARRAY_LIVE_CONFIG=<file.json>``, against a real array/simulator.

Live config file::

    {"vendor": "netapp-ontap", "endpoint": "https://cluster1.lab:443",
     "credential": {"username": "...", "password": "..."},
     "ca_bundle": "-----BEGIN CERTIFICATE-----...", "verify_tls": true,
     "options": {"svm": "svm1"},
     "initiators": {"iscsi": [["iqn...a"], ["iqn...b"]]}}

The live tier creates and deletes only ``sb-`` volumes and ``sbtest-`` hosts.
"""
from __future__ import annotations

import importlib
import json
import os

import pytest
from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import ArrayConfig, Initiator, Protocol
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.registry import ADAPTERS
from tests.unit.storage_arrays.fakes import ContractSetup, FakeTransport

VENDORS = sorted(ADAPTERS)


class StaticProvider:
    """Stands in for CredentialProvider: returns ``credential`` and counts
    invalidations. Tests flip ``credential`` to simulate rotation."""

    def __init__(self, credential: Credential):
        self.credential = credential
        self.resolved: list[str] = []
        self.invalidated = 0

    def resolve(self, ref, context):
        self.resolved.append(context)
        return self.credential

    def invalidate(self, ref=None):
        self.invalidated += 1


def fake_module(vendor: str):
    tail = ADAPTERS[vendor].split(":")[0].split(".")[-1]
    try:
        return importlib.import_module(f"tests.unit.storage_arrays.fakes.{tail}")
    except ModuleNotFoundError as e:
        if e.name and e.name.endswith(tail):
            pytest.skip(f"no fake for {vendor} yet")
        raise


class Harness:
    def __init__(self, setup: ContractSetup, live: bool = False):
        self.setup = setup
        self.live = live
        self.provider = StaticProvider(setup.credential)
        self.client = ArrayClient(
            setup.config, self.provider,  # type: ignore[arg-type]
            transport=None if live else FakeTransport(setup.fake),
            session_kwargs={"sleep": lambda s: None},
        )

    @property
    def fake(self):
        return self.setup.fake


def _live_setup(path: str) -> ContractSetup:
    with open(path) as f:
        cfg = json.load(f)
    initiators = {
        Protocol(proto): tuple(
            tuple(Initiator(protocol=Protocol(proto), address=a) for a in group)
            for group in groups)
        for proto, groups in cfg["initiators"].items()
    }
    return ContractSetup(
        fake=None,  # type: ignore[arg-type]
        config=ArrayConfig(id="live", vendor=cfg["vendor"], endpoint=cfg["endpoint"],
                           credential_ref={"kind": "vault-kv", "mount": "m", "path": "p"},
                           ca_bundle=cfg.get("ca_bundle"),
                           verify_tls=cfg.get("verify_tls", True),
                           options=cfg.get("options", {})),
        credential=Credential({k: SecretStr(v) for k, v in cfg["credential"].items()}),
        bad_credential=Credential({k: SecretStr("wrong-" + v)
                                   for k, v in cfg["credential"].items()}),
        initiators=initiators,
        create_foreign_volume=None,  # type: ignore[arg-type]
        has_lun_numbers=cfg.get("has_lun_numbers", True),
    )


def _params():
    live = os.environ.get("SB_ARRAY_LIVE_CONFIG")
    if live:
        return [pytest.param(("live", live), id="live")]
    return [pytest.param(("fake", v), id=v) for v in VENDORS]


@pytest.fixture(params=_params())
def harness(request) -> Harness:
    kind, value = request.param
    if kind == "live":
        h = Harness(_live_setup(value), live=True)
    else:
        h = Harness(fake_module(value).contract_setup())
    yield h
    h.client.close()
