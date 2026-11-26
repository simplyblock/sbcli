"""Vendor key -> adapter class. Adapters are imported lazily, on first use."""
from __future__ import annotations

import importlib

from .base import ArrayAdapter, ArrayUnsupportedError

#: vendor key -> "module:Class" (module relative to this package)
ADAPTERS: dict[str, str] = {
    "pure-flasharray": "adapters.pure:PureFlashArrayAdapter",
    "netapp-ontap": "adapters.netapp_ontap:NetAppOntapAdapter",
    "dell-powerstore": "adapters.dell_powerstore:DellPowerStoreAdapter",
    "dell-powermax": "adapters.dell_powermax:DellPowerMaxAdapter",
    "dell-powerflex": "adapters.dell_powerflex:DellPowerFlexAdapter",
    "hpe-wsapi": "adapters.hpe_wsapi:HpeWsapiAdapter",
    "hpe-nimble": "adapters.hpe_nimble:HpeNimbleAdapter",
    "ibm-flashsystem": "adapters.ibm_flashsystem:IbmFlashSystemAdapter",
    "hitachi-vsp": "adapters.hitachi_vsp:HitachiVspAdapter",
    "huawei-dorado": "adapters.huawei_dorado:HuaweiDoradoAdapter",
    "infinidat-infinibox": "adapters.infinidat:InfinidatAdapter",
}

#: Other names customers use for the same API.
ALIASES: dict[str, str] = {
    "pure": "pure-flasharray",
    "netapp": "netapp-ontap",
    "netapp-asa": "netapp-ontap",
    "lenovo-thinksystem-dm": "netapp-ontap",
    "powerstore": "dell-powerstore",
    "powermax": "dell-powermax",
    "powerflex": "dell-powerflex",
    "hpe-alletra-mp": "hpe-wsapi",
    "hpe-primera": "hpe-wsapi",
    "hpe-3par": "hpe-wsapi",
    "hpe-alletra-6000": "hpe-nimble",
    "hpe-alletra-5000": "hpe-nimble",
    "ibm-storage-virtualize": "ibm-flashsystem",
    "ibm-svc": "ibm-flashsystem",
    "hitachi": "hitachi-vsp",
    "huawei-oceanstor": "huawei-dorado",
    "infinidat": "infinidat-infinibox",
}


def canonical_vendor(vendor: str) -> str:
    key = vendor.strip().lower()
    key = ALIASES.get(key, key)
    if key not in ADAPTERS:
        raise ArrayUnsupportedError(
            f"Unknown array vendor {vendor!r}; supported: {', '.join(sorted(ADAPTERS))}")
    return key


def adapter_class(vendor: str) -> type[ArrayAdapter]:
    target = ADAPTERS[canonical_vendor(vendor)]
    module_name, _, class_name = target.partition(":")
    module = importlib.import_module(f"{__package__}.{module_name}")
    cls = getattr(module, class_name)
    if not (isinstance(cls, type) and issubclass(cls, ArrayAdapter)):
        raise TypeError(f"{target} is not an ArrayAdapter")
    return cls
