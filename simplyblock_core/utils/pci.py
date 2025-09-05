from pathlib import Path
import re
from typing import Annotated, Optional

from pydantic import StringConstraints

from .helpers import single


PCI = Path('/sys/bus/pci')
PCI_DEVICES = PCI / 'devices'
PCI_DRIVERS = PCI / 'drivers'
PCI_ADDRESS_REGEX = re.compile(r'[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}.[0-7]')
NVME_CLASS = bytes([0x01, 0x08, 0x02])


PCIAddress = Annotated[str, StringConstraints(pattern=PCI_ADDRESS_REGEX)]


def device(address: PCIAddress) -> Path:
    return PCI_DEVICES / address


def device_driver(address: PCIAddress) -> Path:
    return device(address) / 'driver'


def vendor_id(address: PCIAddress) -> int:
    return int((device(address) / 'vendor').read_text(), 16)


def device_id(address: PCIAddress) -> int:
    return int((device(address) / 'vendor').read_text(), 16)


def list_devices(*, driver_name: Optional[str] = None, device_class: Optional[bytes] = None):
    assert(sum(param is not None for param in [driver_name, device_class]) == 1)
    if driver_name is not None:
        driver = PCI_DRIVERS / driver_name
        return [
                name
                for path 
                in driver.iterdir()
                if PCI_ADDRESS_REGEX.match(name := path.name) is not None
        ] if driver.exists() else []

    if device_class is not None: 
        return [
                name
                for device
                in PCI_DEVICES.iterdir()
                if int((device / 'class').read_text(), 16).to_bytes(3) == device_class
        ]

    raise AssertionError('unreachable')


def nvme_device_name(address: PCIAddress):
    controller = single((device(address) / 'nvme').iterdir())
    return single(
            name
            for path
            in controller.iterdir()
            if (name := path.name).startswith(controller.name)
    )


def bound_driver_name(address: PCIAddress) -> Optional[str]:
    driver = device_driver(address)
    return driver.readlink().name if driver.exists() else None


def unbind_driver(address: PCIAddress):
    driver = device_driver(address)
    if not driver.exists():
        return

    (driver / 'unbind').write_text(address)


def ensure_driver(address: PCIAddress, driver_name: str, *, override: bool = False):
    (device(address) / 'driver_override').write_text(driver_name if override else '\n')

    driver = device_driver(address)
    if driver.exists():
        if driver.readlink().name == driver_name:
            return

        (driver / 'unbind').write_text(address)

    (PCI_DRIVERS / driver_name / 'bind').write_text(address)


def driver_loaded(driver_name: str) -> bool:
    return (PCI_DRIVERS / driver_name).exists()
