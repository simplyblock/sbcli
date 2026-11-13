"""AWS EBS io2 Multi-Attach as the "array" of a multi-attach cluster.

Each storage node's LUN is an io2 (or io1) volume created with
``MultiAttachEnabled``, attached to the owner and to its LVS secondary and
tertiary. All instances must be Nitro instances in the one availability zone
of the cluster (``options["az"]``); a multi-attach volume cannot leave its AZ.

Identities:

* LUN id: the volume id (``vol-0123...``). The host-side identity (``wwn``)
  is the NVMe serial EBS gives the volume's namespace, ``vol0123...`` (the id
  without its dash), which lsblk reports as SERIAL on every instance the
  volume is attached to; ``NVMeDevice.lun_wwn`` and the agent's
  ``find_lun_devices`` match on it. Both forms are accepted wherever a LUN id
  is expected.
* host id: the EC2 instance id (``ensure_host`` takes
  ``{"ec2_instance_id": ...}``; the node agent reports it from the instance
  metadata service as ``cloud_instance.id``, StorageNode.cloud_instance_id).

Ownership: every volume created here is tagged ``simplyblock-cluster-id =
<cluster id>``; a volume without this cluster's tag is never attached,
detached, resized or deleted (:class:`ForeignLunError`).

Fencing: ``fence`` is a forced ``DetachVolume`` of the failed instance's
attachment, and returns only once EC2 reports the attachment gone. A
detached EBS volume is hot-removed from the instance's PCIe bus; the instance
can issue no further I/O to it, queued or new.

Credentials (KMS secret, all optional): ``aws_access_key_id``,
``aws_secret_access_key``, ``aws_session_token``. Without them the default
boto3 chain applies (the control plane's instance role / IRSA).

Options: ``az`` (required), ``region`` (default: ``az`` without its letter),
``volume_type`` (``io2`` default, or ``io1``), ``iops`` (default 3000),
``encrypted`` (default true), ``kms_key_id``, ``tags`` (dict, added to every
volume), ``poll_interval`` (s, default 2), ``timeout`` (s, default 300).
``endpoint`` (the registration's endpoint) is an EC2 endpoint URL override
(VPC endpoint), empty for the regional default.
"""

from __future__ import annotations

import logging
import math
import time

from pydantic import SecretStr

from simplyblock_core.array import TAG_CLUSTER_ID, TAG_LUN_NAME, ArrayError, ArrayPlugin

logger = logging.getLogger()

GIB = 1024 ** 3
#: Smallest io1/io2 volume.
MIN_SIZE_GIB = 4
#: EBS Multi-Attach: at most 16 Nitro instances per volume.
MAX_ATTACHMENTS = 16
MULTI_ATTACH_TYPES = ("io2", "io1")
_LIVE_ATTACHMENT = ("attaching", "attached", "busy")
_GONE_VOLUME = ("deleting", "deleted", "error")


def volume_wwn(volume_id: str) -> str:
    """The NVMe serial of an EBS volume as an instance sees it."""
    return volume_id.replace("-", "", 1)


def volume_id_of(lun: str) -> str:
    """``vol-0abc`` from either the volume id or its NVMe serial."""
    lun = (lun or "").strip()
    if lun.startswith("vol-"):
        return lun
    if lun.startswith("vol") and len(lun) > 3:
        return "vol-" + lun[3:]
    raise ArrayError(f"{lun!r} is no EBS volume id")


def _plain(value) -> str:
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    return str(value or "")


class AwsEbsPlugin(ArrayPlugin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ec2 = None
        self._sleep = time.sleep

    # ------------------------------------------------------------- session

    def connect(self, endpoint, credentials, options):
        super().connect(endpoint, credentials, options)
        az = self.options.get("az") or ""
        if not az:
            raise ArrayError("aws-ebs: option 'az' (the availability zone of every node) is required")
        vtype = self.options.get("volume_type", "io2")
        if vtype not in MULTI_ATTACH_TYPES:
            raise ArrayError(f"aws-ebs: volume_type must be one of {MULTI_ATTACH_TYPES} (Multi-Attach)")
        try:
            import boto3
        except ImportError as e:                         # pragma: no cover - boto3 is a dependency
            raise ArrayError("aws-ebs: boto3 is not installed") from e
        creds = self.credentials
        key_id = _plain(creds.get("aws_access_key_id"))
        secret = _plain(creds.get("aws_secret_access_key"))
        if bool(key_id) != bool(secret):
            raise ArrayError("aws-ebs: aws_access_key_id and aws_secret_access_key go together")
        session = boto3.session.Session(
            aws_access_key_id=key_id or None,
            aws_secret_access_key=secret or None,
            aws_session_token=_plain(creds.get("aws_session_token")) or None,
            region_name=self.region,
        )
        self.ec2 = session.client("ec2", endpoint_url=endpoint or None)
        # Validates the credentials and the zone in one call.
        try:
            zones = self.ec2.describe_availability_zones(ZoneNames=[az])["AvailabilityZones"]
        except Exception as e:
            raise ArrayError(f"aws-ebs: availability zone {az} not usable: {self._err(e)}") from e
        if not zones:
            raise ArrayError(f"aws-ebs: availability zone {az} not found")

    @property
    def region(self) -> str:
        return self.options.get("region") or (self.options.get("az") or "")[:-1]

    @property
    def _poll(self) -> float:
        return float(self.options.get("poll_interval", 2))

    @property
    def _timeout(self) -> float:
        return float(self.options.get("timeout", 300))

    @staticmethod
    def _err(e) -> str:
        resp = getattr(e, "response", None) or {}
        err = resp.get("Error") or {}
        if err:
            return f"{err.get('Code')}: {err.get('Message')}"
        return str(e)

    @staticmethod
    def _code(e) -> str:
        return ((getattr(e, "response", None) or {}).get("Error") or {}).get("Code", "")

    def _client(self):
        if self.ec2 is None:
            raise ArrayError("aws-ebs: not connected")
        return self.ec2

    def capabilities(self):
        return {"protocols": ["nvme"], "supports_reservations": True,
                "max_attachments": MAX_ATTACHMENTS}

    # ------------------------------------------------------------- helpers

    def _describe(self, volume_id):
        try:
            vols = self._client().describe_volumes(VolumeIds=[volume_id])["Volumes"]
        except Exception as e:
            if self._code(e) == "InvalidVolume.NotFound":
                return None
            raise ArrayError(f"aws-ebs: describe {volume_id}: {self._err(e)}") from e
        return vols[0] if vols else None

    @staticmethod
    def _tags(vol) -> dict:
        return {t["Key"]: t["Value"] for t in vol.get("Tags") or []}

    def _owned(self, lun):
        vid = volume_id_of(lun)
        vol = self._describe(vid)
        if vol is None or vol.get("State") in _GONE_VOLUME:
            raise ArrayError(f"aws-ebs: volume {vid} not found")
        self.check_owned(vid, self._tags(vol))
        return vol

    @staticmethod
    def _attachment(vol, instance_id):
        for att in vol.get("Attachments") or []:
            if att.get("InstanceId") == instance_id:
                return att
        return None

    @staticmethod
    def _summary(vol) -> dict:
        return {
            "id": vol["VolumeId"],
            "wwn": volume_wwn(vol["VolumeId"]),
            "size": int(vol.get("Size") or 0) * GIB,
            "hosts": [a["InstanceId"] for a in vol.get("Attachments") or []
                      if a.get("State") in _LIVE_ATTACHMENT],
            "state": vol.get("State", ""),
        }

    def _wait(self, what, check):
        """Poll ``check()`` until it returns a truthy value or the timeout."""
        deadline = time.monotonic() + self._timeout
        while True:
            result = check()
            if result:
                return result
            if time.monotonic() >= deadline:
                raise ArrayError(f"aws-ebs: timed out waiting for {what}")
            self._sleep(self._poll)

    def _free_device_name(self, instance_id) -> str:
        try:
            res = self._client().describe_instances(InstanceIds=[instance_id])
            inst = res["Reservations"][0]["Instances"][0]
        except Exception as e:
            raise ArrayError(f"aws-ebs: describe instance {instance_id}: {self._err(e)}") from e
        used = {m.get("DeviceName") for m in inst.get("BlockDeviceMappings") or []}
        used.add(inst.get("RootDeviceName"))
        # The device name is ignored for NVMe on Nitro but must be unique
        # per instance. /dev/sd[f-z], then /dev/sd[f-z][a-z].
        letters = "fghijklmnopqrstuvwxyz"
        candidates = [f"/dev/sd{a}" for a in letters]
        candidates += [f"/dev/sd{a}{b}" for a in letters for b in "abcdefghijklmnopqrstuvwxyz"]
        for name in candidates:
            if name not in used:
                return name
        raise ArrayError(f"aws-ebs: no free device name on {instance_id}")

    # ---------------------------------------------------------- operations

    def ensure_host(self, name, initiators):
        instance_id = (initiators or {}).get("ec2_instance_id") or ""
        if not instance_id.startswith("i-"):
            # The node agent reports the instance id only when its cloud
            # probe runs (docker deployments start it with
            # WITHOUT_CLOUD_INFO). The node's private IP identifies the
            # instance as well, within the zone of the volumes.
            instance_id = self._instance_by_private_ip(name, (initiators or {}).get("ip") or "")
        try:
            res = self._client().describe_instances(InstanceIds=[instance_id])
            inst = res["Reservations"][0]["Instances"][0]
        except ArrayError:
            raise
        except Exception as e:
            raise ArrayError(f"aws-ebs: instance {instance_id} of host {name}: {self._err(e)}") from e
        az = (inst.get("Placement") or {}).get("AvailabilityZone", "")
        if az != self.options.get("az"):
            raise ArrayError(
                f"aws-ebs: instance {instance_id} of host {name} is in {az}, the volumes in "
                f"{self.options.get('az')}; Multi-Attach volumes cannot cross zones")
        return instance_id

    def _instance_by_private_ip(self, name, ip):
        if not ip:
            raise ArrayError(f"aws-ebs: host {name} has no EC2 instance id and no IP")
        try:
            res = self._client().describe_instances(Filters=[
                {"Name": "private-ip-address", "Values": [ip]},
                {"Name": "availability-zone", "Values": [self.options.get("az") or ""]},
                {"Name": "instance-state-name", "Values": ["pending", "running"]}])
        except Exception as e:
            raise ArrayError(f"aws-ebs: find the instance of host {name} ({ip}): {self._err(e)}") from e
        ids = [i["InstanceId"] for r in res.get("Reservations") or [] for i in r.get("Instances") or []]
        if len(ids) != 1:
            raise ArrayError(
                f"aws-ebs: host {name} has no EC2 instance id, and {len(ids)} running instances "
                f"in {self.options.get('az')} have the private IP {ip}")
        return ids[0]

    def create_lun(self, name, size_bytes, tags):
        all_tags = self.cluster_tags(dict(self.options.get("tags") or {}, **(tags or {})), name)
        all_tags.setdefault("Name", name)
        try:
            found = self._client().describe_volumes(Filters=[
                {"Name": f"tag:{TAG_CLUSTER_ID}", "Values": [self.cluster_id]},
                {"Name": f"tag:{TAG_LUN_NAME}", "Values": [name]},
            ])["Volumes"]
        except Exception as e:
            raise ArrayError(f"aws-ebs: look up volume {name}: {self._err(e)}") from e
        live = [v for v in found if v.get("State") not in _GONE_VOLUME]
        if len(live) > 1:
            raise ArrayError(f"aws-ebs: {len(live)} volumes are named {name} in this cluster")
        if live:
            vol = live[0]
            logger.info("aws-ebs: volume %s for %s exists", vol["VolumeId"], name)
        else:
            size_gib = max(MIN_SIZE_GIB, math.ceil(int(size_bytes) / GIB))
            kwargs = {
                "AvailabilityZone": self.options["az"],
                "Size": size_gib,
                "VolumeType": self.options.get("volume_type", "io2"),
                "Iops": int(self.options.get("iops", 3000)),
                "MultiAttachEnabled": True,
                "Encrypted": bool(self.options.get("encrypted", True)),
                "TagSpecifications": [{
                    "ResourceType": "volume",
                    "Tags": [{"Key": k, "Value": v} for k, v in sorted(all_tags.items())],
                }],
            }
            if self.options.get("kms_key_id"):
                kwargs["KmsKeyId"] = self.options["kms_key_id"]
            try:
                vol = self._client().create_volume(**kwargs)
            except Exception as e:
                raise ArrayError(f"aws-ebs: create volume {name}: {self._err(e)}") from e
            logger.info("aws-ebs: created volume %s (%s GiB) for %s", vol["VolumeId"], size_gib, name)
        vid = vol["VolumeId"]

        def _available():
            v = self._describe(vid)
            if v is not None and v.get("State") == "error":
                raise ArrayError(f"aws-ebs: volume {vid} failed to create")
            return v if v is not None and v.get("State") in ("available", "in-use") else None
        vol = self._wait(f"volume {vid} to become available", _available)
        return {k: v for k, v in self._summary(vol).items() if k in ("id", "wwn", "size")}

    def attach(self, lun_id, host_id):
        vol = self._owned(lun_id)
        vid = vol["VolumeId"]
        att = self._attachment(vol, host_id)
        if att is None or att.get("State") not in _LIVE_ATTACHMENT:
            live = [a for a in vol.get("Attachments") or [] if a.get("State") in _LIVE_ATTACHMENT]
            if len(live) >= MAX_ATTACHMENTS:
                raise ArrayError(f"aws-ebs: volume {vid} is attached to {MAX_ATTACHMENTS} instances")
            device = self._free_device_name(host_id)
            try:
                self._client().attach_volume(VolumeId=vid, InstanceId=host_id, Device=device)
            except Exception as e:
                raise ArrayError(f"aws-ebs: attach {vid} to {host_id}: {self._err(e)}") from e
            logger.info("aws-ebs: attaching %s to %s as %s", vid, host_id, device)

        def _attached():
            a = self._attachment(self._describe(vid) or {}, host_id)
            return a is not None and a.get("State") == "attached"
        self._wait(f"volume {vid} to attach to {host_id}", _attached)

    def detach(self, lun_id, host_id, force=False):
        vol = self._owned(lun_id)
        vid = vol["VolumeId"]
        att = self._attachment(vol, host_id)
        if att is None or att.get("State") == "detached":
            return
        if att.get("State") != "detaching" or force:
            try:
                self._client().detach_volume(VolumeId=vid, InstanceId=host_id, Force=bool(force))
            except Exception as e:
                if self._code(e) not in ("IncorrectState", "InvalidAttachment.NotFound"):
                    raise ArrayError(f"aws-ebs: detach {vid} from {host_id}: {self._err(e)}") from e
            logger.info("aws-ebs: detaching %s from %s%s", vid, host_id, " (forced)" if force else "")

        def _detached():
            v = self._describe(vid)
            a = self._attachment(v or {}, host_id)
            return a is None or a.get("State") == "detached"
        self._wait(f"volume {vid} to detach from {host_id}", _detached)

    def resize(self, lun_id, size_bytes):
        vol = self._owned(lun_id)
        vid = vol["VolumeId"]
        target = math.ceil(int(size_bytes) / GIB)
        current = int(vol.get("Size") or 0)
        if target < current:
            raise ArrayError(f"aws-ebs: volume {vid} is {current} GiB; shrinking to {target} GiB is refused")
        if target > current:
            try:
                self._client().modify_volume(VolumeId=vid, Size=target)
            except Exception as e:
                raise ArrayError(f"aws-ebs: grow {vid} to {target} GiB: {self._err(e)}") from e
            logger.info("aws-ebs: growing %s from %s to %s GiB", vid, current, target)

        def _grown():
            try:
                mods = self._client().describe_volumes_modifications(
                    VolumeIds=[vid])["VolumesModifications"]
            except Exception as e:
                if self._code(e) == "InvalidVolumeModification.NotFound":
                    mods = []
                else:
                    raise ArrayError(f"aws-ebs: modification of {vid}: {self._err(e)}") from e
            for m in mods:
                state = m.get("ModificationState")
                if state == "failed":
                    raise ArrayError(f"aws-ebs: growing {vid} failed: {m.get('StatusMessage', '')}")
                if state == "modifying":
                    return False
            # "optimizing" or "completed": the new size is usable.
            return True
        self._wait(f"volume {vid} to grow", _grown)

    def get(self, lun_id):
        return self._summary(self._owned(lun_id))

    def delete_lun(self, lun_id):
        vid = volume_id_of(lun_id)
        vol = self._describe(vid)
        if vol is None or vol.get("State") in ("deleting", "deleted"):
            return
        self.check_owned(vid, self._tags(vol))
        for att in vol.get("Attachments") or []:
            if att.get("State") in _LIVE_ATTACHMENT or att.get("State") == "detaching":
                self.detach(vid, att["InstanceId"], force=True)

        def _available():
            v = self._describe(vid)
            return v is None or v.get("State") == "available"
        self._wait(f"volume {vid} to become available", _available)
        try:
            self._client().delete_volume(VolumeId=vid)
        except Exception as e:
            if self._code(e) != "InvalidVolume.NotFound":
                raise ArrayError(f"aws-ebs: delete {vid}: {self._err(e)}") from e
        logger.info("aws-ebs: deleted volume %s", vid)

    def list_luns(self):
        if not self.cluster_id:
            raise ArrayError("aws-ebs: no cluster id to list the volumes of")
        out = []
        try:
            pages = self._client().get_paginator("describe_volumes").paginate(
                Filters=[{"Name": f"tag:{TAG_CLUSTER_ID}", "Values": [self.cluster_id]}])
            for page in pages:
                for vol in page["Volumes"]:
                    if vol.get("State") not in _GONE_VOLUME:
                        out.append(self._summary(vol))
        except Exception as e:
            raise ArrayError(f"aws-ebs: list volumes: {self._err(e)}") from e
        return out
