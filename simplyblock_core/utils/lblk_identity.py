"""Identity of a partition used as an lblk device.

A partition has no hardware serial. Its serial is built from the serial of its
disk and its PARTUUID (``<disk serial>-part-<partuuid>``), both of which stay
with it across reboots and renames. Where the PARTUUID could not be read, the
agent made an id up from the host name, the disk serial, the kernel name and
the size (``SYN-<hash>``). That was the case for every partition in the
storage-node container, where lsblk prints no PARTUUID: the kernel name of an
NVMe namespace follows the order its controller was probed in, so the id of a
partition could change on a reboot, and the node no longer knew its device.

The agent reads the PARTUUID from the device now. A partition that a node
recorded under a made-up id keeps that id: it is the key of the device record
and the name of its AIO bdev. Everything that compares what a node recorded
with what its host reports goes through this module, so that the two are
compared under one id:

- an agent reports the made-up id it would have used as ``legacy_serial``,
- a control plane that predates this gets the inventory as before
  (with_legacy_partition_serials), and
- a node's inventory names each partition the node recorded under a made-up
  id by that id (under_recorded_serials).
"""

SYNTHETIC_PREFIX = "SYN-"
_BY_PARTUUID = "/dev/disk/by-partuuid/"


def is_synthetic(serial) -> bool:
    return isinstance(serial, str) and serial.startswith(SYNTHETIC_PREFIX)


def with_legacy_partition_serials(inventory):
    """The inventory as an agent without the PARTUUID probe reported it: a
    partition whose PARTUUID lsblk does not print under its made-up id. For a
    control plane that predates the probe, which knows these partitions under
    that id and under no other."""
    out = []
    for dev in inventory or []:
        legacy = dev.get("legacy_serial")
        if legacy:
            dev = dict(dev, serial=legacy, serial_synthetic=True)
        out.append(dev)
    return out


def same_partition(entry, live) -> bool:
    """Whether a recorded lblk entry and a partition of the live inventory are
    one partition: on the same disk, and either the made-up id of the live
    partition is the recorded one, or the recorded PARTUUID (the field, or a
    /dev/disk/by-partuuid/ path the entry was selected or recorded by) is the
    live one.

    The disk must be the same because a PARTUUID is copied with the partition
    table: a disk cloned from a template carries the PARTUUIDs of every other
    clone."""
    if not isinstance(entry, dict) or live.get("type") != "part":
        return False
    recorded_parent = entry.get("parent_serial")
    live_parent = live.get("parent_serial")
    if recorded_parent and live_parent and recorded_parent != live_parent:
        return False
    serial = entry.get("serial")
    if serial and serial in (live.get("legacy_serial"), live.get("serial")):
        return True
    partuuid = (live.get("partuuid") or "").lower()
    if not partuuid:
        return False
    if (entry.get("partuuid") or "").lower() == partuuid:
        return True
    return any(isinstance(path, str) and path.lower() == _BY_PARTUUID + partuuid
               for path in (entry.get("path"), entry.get("by_id")))


def under_recorded_serials(recorded_entries, inventory):
    """A host inventory in which every partition that the node recorded under
    a made-up id carries that id as its serial (and the id the agent gives it
    now as ``reported_serial``). Everything else is returned as it is.

    A recorded id that two live partitions answer to, or a live partition that
    two recorded ids answer to, is left alone: neither can be told apart."""
    recorded = [e for e in recorded_entries or []
                if isinstance(e, dict) and e.get("type") == "part" and is_synthetic(e.get("serial"))]
    if not recorded or not isinstance(inventory, list):
        return inventory
    parts = [d for d in inventory if isinstance(d, dict) and d.get("type") == "part"]
    out = []
    for dev in inventory:
        if not isinstance(dev, dict) or dev.get("type") != "part":
            out.append(dev)
            continue
        entries = [e for e in recorded if same_partition(e, dev)]
        if len(entries) == 1 and entries[0]["serial"] != dev.get("serial"):
            claimants = [d for d in parts if same_partition(entries[0], d)]
            if len(claimants) == 1:
                dev = dict(dev, serial=entries[0]["serial"], serial_synthetic=True,
                           reported_serial=dev.get("serial"))
        out.append(dev)
    return out


def remember_partuuids(recorded_entries, inventory) -> None:
    """Write the PARTUUID of each recorded partition onto its entry, where the
    entry has none: an entry recorded by an agent that could not read it. With
    it the partition is found again when its made-up id no longer answers.
    ``inventory`` is one that went through under_recorded_serials."""
    by_serial = {d.get("serial"): d for d in inventory or []
                 if isinstance(d, dict) and d.get("type") == "part" and d.get("partuuid")}
    for entry in recorded_entries or []:
        if not isinstance(entry, dict) or entry.get("type") != "part" or entry.get("partuuid"):
            continue
        live = by_serial.get(entry.get("serial"))
        if live is not None:
            entry["partuuid"] = live["partuuid"]
            if not entry.get("parent_serial") and live.get("parent_serial"):
                entry["parent_serial"] = live["parent_serial"]
