"""The data path of a mock array: a vendor fake's volumes, hosts, and
mappings mirrored into the Linux kernel's iSCSI target (LIO).

The fakes answer the vendor's management API and keep the array's state in
memory, which is enough to test the adapters but presents nothing to a host.
With a mirror attached, every volume of the fake becomes a real block device
exported over iSCSI, every host's initiator an ACL of the target, and every
mapping a LUN of that ACL. A host logged in to the target therefore sees a LUN
appear after a mapping and a SCSI rescan, lose it when it is unmapped (the
fence of a takeover), and see it grow after a resize, exactly as with an
array. This is what lets the control plane's whole multi-attach path (node
add, standby presentation, fencing, takeover, growth) run against a mock.

The mirror reads the fake through the vendor's own adapter, in process, so a
LUN's size and identity are what the array plug-in reports for it. The
identity reaches the host as the LUN's SCSI unit serial (lsblk SERIAL), which
LUN matching accepts beside the WWN. Only the volume ids and the hosts'
initiator names are read from the fake directly (``lio_volume_ids``,
``lio_host_iqns``, ``lio_all_iqns``).

Backends: :class:`MemoryLio` for tests, :class:`ConfigfsLio` for the kernel
target (root, ``target_core_iblock``, ``iscsi_target_mod``, and
``python3-rtslib-fb``). Volumes are sparse files on loop devices, exported as
block backstores, so a resize is a truncate and a loop capacity refresh.
"""
from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any
from typing import Protocol as TypingProtocol

from simplyblock_core.credentials import Credential

from ..base import ArrayConfig, Protocol
from ..client import ArrayClient

logger = logging.getLogger(__name__)

KEY_PREFIX = "sbm_"


@dataclass
class LioVolume:
    size_bytes: int
    serial: str


class LioBackend(TypingProtocol):
    def list_volumes(self) -> dict[str, LioVolume]: ...
    def create_volume(self, key: str, size_bytes: int, serial: str) -> None: ...
    def resize_volume(self, key: str, size_bytes: int) -> None: ...
    def delete_volume(self, key: str) -> None: ...
    def list_acls(self) -> dict[str, dict[int, str]]: ...
    def ensure_acl(self, iqn: str) -> None: ...
    def delete_acl(self, iqn: str) -> None: ...
    def map_lun(self, iqn: str, mapped_lun: int, key: str) -> None: ...
    def unmap_lun(self, iqn: str, mapped_lun: int) -> None: ...
    def close(self) -> None: ...


@dataclass
class MemoryLio:
    """The target as dictionaries: volumes by key, and per ACL its LUNs."""

    volumes: dict[str, LioVolume] = field(default_factory=dict)
    acls: dict[str, dict[int, str]] = field(default_factory=dict)
    #: Count of changes, for idempotence checks.
    ops: int = 0

    def list_volumes(self) -> dict[str, LioVolume]:
        return {k: LioVolume(v.size_bytes, v.serial) for k, v in self.volumes.items()}

    def create_volume(self, key: str, size_bytes: int, serial: str) -> None:
        self.ops += 1
        self.volumes[key] = LioVolume(size_bytes, serial)

    def resize_volume(self, key: str, size_bytes: int) -> None:
        self.ops += 1
        self.volumes[key].size_bytes = size_bytes

    def delete_volume(self, key: str) -> None:
        self.ops += 1
        assert all(key not in luns.values() for luns in self.acls.values()), "volume still mapped"
        del self.volumes[key]

    def list_acls(self) -> dict[str, dict[int, str]]:
        return {iqn: dict(luns) for iqn, luns in self.acls.items()}

    def ensure_acl(self, iqn: str) -> None:
        if iqn not in self.acls:
            self.ops += 1
            self.acls[iqn] = {}

    def delete_acl(self, iqn: str) -> None:
        self.ops += 1
        del self.acls[iqn]

    def map_lun(self, iqn: str, mapped_lun: int, key: str) -> None:
        self.ops += 1
        assert key in self.volumes
        self.acls[iqn][mapped_lun] = key

    def unmap_lun(self, iqn: str, mapped_lun: int) -> None:
        self.ops += 1
        del self.acls[iqn][mapped_lun]

    def close(self) -> None:
        pass


class _StaticCredentials:
    def __init__(self, credential: Credential):
        self._credential = credential

    def resolve(self, ref, context):
        return self._credential

    def invalidate(self, ref=None):
        pass


def _host_identity(vol) -> str:
    """What the array plug-in reports for a SCSI LUN (array.san.host_wwn)."""
    from simplyblock_core.array.san import host_wwn
    return host_wwn(vol, Protocol.ISCSI)


class LioMirror:
    """Keeps a backend in step with a vendor fake. ``reconcile`` is cheap and
    idempotent, and is called after every request the mock server answers."""

    def __init__(self, fake: Any, config: ArrayConfig, credential: Credential, backend: LioBackend,
                 *, transport: Any, session_kwargs: dict[str, Any] | None = None,
                 allow: Iterable[str] = ()):
        self.fake = fake
        self.backend = backend
        #: Initiators given an ACL whether or not a host of the array has them:
        #: the hosts log in to the target before they are registered, as they
        #: would to an array that presents nothing to an unknown initiator.
        self.allow = [i.lower() for i in allow]
        self.client = ArrayClient(config, _StaticCredentials(credential),  # type: ignore[arg-type]
                                  transport=transport, session_kwargs=session_kwargs)
        self._lock = threading.Lock()

    @staticmethod
    def key(volume_id: str) -> str:
        return KEY_PREFIX + hashlib.sha1(volume_id.encode()).hexdigest()[:12]

    def close(self) -> None:
        self.client.close()
        self.backend.close()

    # -- desired state ------------------------------------------------------

    def desired(self) -> tuple[dict[str, LioVolume], dict[str, dict[int, str]]]:
        volumes: dict[str, LioVolume] = {}
        acls: dict[str, dict[int, str]] = {iqn: {} for iqn in self.allow}
        for iqn in self.fake.lio_all_iqns():
            acls.setdefault(iqn, {})
        for vid in self.fake.lio_volume_ids():
            vol = self.client.get_volume(vid)
            if vol is None:
                continue
            key = self.key(vol.id)
            volumes[key] = LioVolume(vol.size_bytes, _host_identity(vol))
            for m in self.client.list_mappings(vol.id):
                for iqn in self.fake.lio_host_iqns(m.host_id):
                    luns = acls.setdefault(iqn, {})
                    lun = m.lun if m.lun is not None and m.lun not in luns else _free(luns)
                    luns[lun] = key
        return volumes, acls

    # -- reconcile ----------------------------------------------------------

    def reconcile(self) -> None:
        with self._lock:
            want_vols, want_acls = self.desired()
            have_vols = self.backend.list_volumes()
            have_acls = self.backend.list_acls()

            # Unmap first: what is fenced is fenced before anything else moves.
            for iqn, luns in have_acls.items():
                want = want_acls.get(iqn, {})
                for n, key in luns.items():
                    if want.get(n) != key:
                        self.backend.unmap_lun(iqn, n)
                if iqn not in want_acls:
                    self.backend.delete_acl(iqn)
            for key in have_vols.keys() - want_vols.keys():
                self.backend.delete_volume(key)
            for key, vol in want_vols.items():
                have = have_vols.get(key)
                if have is None:
                    self.backend.create_volume(key, vol.size_bytes, vol.serial)
                elif vol.size_bytes > have.size_bytes:
                    self.backend.resize_volume(key, vol.size_bytes)
            have_acls = self.backend.list_acls()
            for iqn, luns in want_acls.items():
                self.backend.ensure_acl(iqn)
                current = have_acls.get(iqn, {})
                for n, key in luns.items():
                    if current.get(n) != key:
                        self.backend.map_lun(iqn, n, key)


def _free(luns: dict[int, str]) -> int:
    n = 0
    while n in luns:
        n += 1
    return n


# -- kernel target --------------------------------------------------------------


class ConfigfsLio:
    """The kernel's iSCSI target through rtslib. One target, one portal group,
    one ACL per initiator; volumes are sparse files on loop devices."""

    def __init__(self, target_iqn: str, data_dir: str,
                 portal: tuple[str, int] | list[tuple[str, int]] = ("0.0.0.0", 3260)):
        # rtslib keeps its saved configuration here, and refuses to start on a
        # host where no target ever ran and the directory is missing.
        os.makedirs("/etc/rtslib-fb-target", exist_ok=True)
        # python3-rtslib-fb, on the mock's host only
        import rtslib_fb as rts  # type: ignore[import-not-found]

        self.rts = rts
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        fabric = rts.FabricModule("iscsi")
        self._remove_previous(fabric, target_iqn)
        self.target = rts.Target(fabric, target_iqn)
        self.tpg = rts.TPG(self.target, 1)
        for name, value in (("authentication", "0"), ("generate_node_acls", "0"),
                            ("demo_mode_write_protect", "0"), ("demo_mode_discovery", "1")):
            self.tpg.set_attribute(name, value)
        # Several portals give every host several paths to every LUN: the
        # multipath case of FC and iSCSI arrays.
        for ip, port in (portal if isinstance(portal, list) else [portal]):
            rts.NetworkPortal(self.tpg, ip, port)
        self.tpg.enable = True

    def _remove_previous(self, fabric, target_iqn: str) -> None:
        for target in list(fabric.targets):
            if target.wwn == target_iqn:
                target.delete()
        for so in list(self.rts.RTSRoot().storage_objects):
            if so.name.startswith(KEY_PREFIX):
                self._delete_object(so)

    def _path(self, key: str) -> str:
        return os.path.join(self.data_dir, key + ".img")

    def _object(self, key: str):
        for so in self.rts.RTSRoot().storage_objects:
            if so.name == key:
                return so
        raise KeyError(key)

    def _tpg_lun(self, key: str):
        for lun in self.tpg.luns:
            if lun.storage_object.name == key:
                return lun
        raise KeyError(key)

    def _delete_object(self, so) -> None:
        dev = getattr(so, "udev_path", "") or ""
        so.delete()
        if dev.startswith("/dev/loop"):
            subprocess.run(["losetup", "-d", dev], check=False)
        path = self._path(so.name)
        if os.path.exists(path):
            os.unlink(path)

    def list_volumes(self) -> dict[str, LioVolume]:
        out = {}
        for so in self.rts.RTSRoot().storage_objects:
            if so.name.startswith(KEY_PREFIX):
                out[so.name] = LioVolume(os.path.getsize(self._path(so.name)), so.wwn)
        return out

    def create_volume(self, key: str, size_bytes: int, serial: str) -> None:
        path = self._path(key)
        with open(path, "ab") as f:
            f.truncate(size_bytes)
        dev = subprocess.run(["losetup", "--find", "--show", "--direct-io=on", path],
                             check=True, capture_output=True, text=True).stdout.strip()
        so = self.rts.BlockStorageObject(key, dev=dev, wwn=serial)
        self.rts.LUN(self.tpg, storage_object=so)
        logger.info("LIO: volume %s (%d bytes, serial %s) on %s", key, size_bytes, serial, dev)

    def resize_volume(self, key: str, size_bytes: int) -> None:
        so = self._object(key)
        with open(self._path(key), "ab") as f:
            f.truncate(size_bytes)
        subprocess.run(["losetup", "-c", so.udev_path], check=True)
        logger.info("LIO: volume %s grown to %d bytes", key, size_bytes)

    def delete_volume(self, key: str) -> None:
        self._tpg_lun(key).delete()
        self._delete_object(self._object(key))
        logger.info("LIO: volume %s deleted", key)

    def list_acls(self) -> dict[str, dict[int, str]]:
        return {acl.node_wwn: {ml.mapped_lun: ml.tpg_lun.storage_object.name for ml in acl.mapped_luns}
                for acl in self.tpg.node_acls}

    def _acl(self, iqn: str):
        return self.rts.NodeACL(self.tpg, iqn, mode="lookup")

    def ensure_acl(self, iqn: str) -> None:
        self.rts.NodeACL(self.tpg, iqn)

    def delete_acl(self, iqn: str) -> None:
        self._acl(iqn).delete()

    def map_lun(self, iqn: str, mapped_lun: int, key: str) -> None:
        self.rts.MappedLUN(self._acl(iqn), mapped_lun, self._tpg_lun(key))
        logger.info("LIO: %s mapped to %s as LUN %d", key, iqn, mapped_lun)

    def unmap_lun(self, iqn: str, mapped_lun: int) -> None:
        for ml in self._acl(iqn).mapped_luns:
            if ml.mapped_lun == mapped_lun:
                ml.delete()
        logger.info("LIO: LUN %d of %s unmapped", mapped_lun, iqn)

    def close(self) -> None:
        pass
