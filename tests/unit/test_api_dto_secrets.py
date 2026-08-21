"""PR 4 — request DTOs use SecretStr; response DTOs mask in python mode but unwrap on the JSON wire."""
import json

from pydantic import SecretStr

from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_web.api.v2._dtos import BackupConfigDTO, ClusterDTO, CapacityStatDTO
from uuid import uuid4


def _build_capacity():
    from simplyblock_core.models.stats import StatsObject
    return CapacityStatDTO.from_model(StatsObject())


_BACKUP_CONFIG = {
    "bucket_name": "backups",
    "region": "eu-central-1",
    "access_key_id": "AKID",
    "secret_access_key": "SK",
}


def test_backup_config_params_carry_secretstr():
    params = BackupConfigDTO.model_validate(_BACKUP_CONFIG)
    assert params.credentials is not None
    assert isinstance(params.credentials.access_key_id, SecretStr)
    assert isinstance(params.credentials.secret_access_key, SecretStr)
    assert params.credentials.access_key_id.get_secret_value() == "AKID"
    assert params.credentials.secret_access_key.get_secret_value() == "SK"


def test_backup_config_repr_masks_secret_values():
    text = repr(BackupConfigDTO.model_validate(_BACKUP_CONFIG))
    assert "AKID" not in text
    assert "SK" not in text


def test_backup_config_dump_keeps_secrets_wrapped():
    """write_to_db unwraps at the last moment; anything earlier leaks into logs."""
    stored = BackupConfig.model_validate(_BACKUP_CONFIG).model_dump(exclude_none=True)
    assert isinstance(stored["credentials"]["access_key_id"], SecretStr)
    assert "AKID" not in repr(stored)


def test_backup_config_dump_is_json_serializable():
    """Cluster.backup_config is a plain dict written through BaseModel to FDB.

    A python-mode dump is normally not JSON-safe. Field serializers on the two
    offenders -- the URL and the enum -- are what make this hold without a
    hand-written conversion step.
    """
    from simplyblock_core.models.base_model import BaseModel as CoreBaseModel

    stored = BackupConfig.model_validate({
        **_BACKUP_CONFIG, "local_endpoint": "http://minio:9000",
    }).model_dump(exclude_none=True)

    class _Holder(CoreBaseModel):
        backup_config: dict = {}

    holder = _Holder()
    holder.backup_config = stored
    persisted = json.loads(json.dumps(holder.to_dict(unwrap_secrets=True)))

    assert stored["endpoint"] == "http://minio:9000"
    assert stored["secondary_target"] == 0
    assert persisted["backup_config"]["credentials"]["access_key_id"] == "AKID"
    assert BackupConfig.model_validate(persisted["backup_config"]).endpoint_url == \
        "http://minio:9000"


def _build_cluster_dto():
    return ClusterDTO(
        id=uuid4(),
        name="t",
        nqn="nqn.example",
        status="active",
        is_re_balancing=False,
        block_size=512,
        distr_ndcs=1,
        distr_npcs=1,
        ha=True,
        utilization_warning=5,
        utilization_critical=10,
        provisioned_capacity_critical=100,
        provisioned_capacity_warning=50,
        node_affinity=False,
        anti_affinity=False,
        enable_failure_domain=False,
        secret=SecretStr("CLUSTER-SECRET"),
        tls_enabled=False,
        max_fault_tolerance=1,
        backup_enabled=False,
        capacity=_build_capacity(),
    )


def test_cluster_dto_python_dump_keeps_wrapper():
    d = _build_cluster_dto().model_dump()
    assert isinstance(d["secret"], SecretStr)
    assert "CLUSTER-SECRET" not in repr(d)


def test_cluster_dto_repr_masks_secret():
    text = repr(_build_cluster_dto())
    assert "CLUSTER-SECRET" not in text
    assert "**********" in text


def test_cluster_dto_json_unwraps_secret():
    payload = json.loads(_build_cluster_dto().model_dump_json())
    assert payload["secret"] == "CLUSTER-SECRET"


def test_backup_dto_lists_host_nqns_without_their_keys():
    """Listing backups must not hand out the volume's host authentication.

    The record's entries carry each host's DHCHAP keys and PSK next to its NQN,
    so a DTO that copied them through would publish them to anyone who may list
    backups -- a wider audience than the endpoint that exists to read them.
    """
    from simplyblock_core.models.backup import Backup
    from simplyblock_web.api.v2._dtos import BackupDTO

    backup = Backup()
    backup.uuid = str(uuid4())
    backup.allowed_hosts = [{
        "nqn": "nqn.2024-01.io.test:host",
        "dhchap_key": "DHHC-1:00:secret-dhchap:",
        "dhchap_ctrlr_key": "DHHC-1:00:secret-ctrlr:",
        "psk": "NVMeTLSkey-1:01:secret-psk:",
    }]

    dto = BackupDTO.from_model(backup)

    assert dto.allowed_hosts == ["nqn.2024-01.io.test:host"]
    assert "secret-" not in dto.model_dump_json()
