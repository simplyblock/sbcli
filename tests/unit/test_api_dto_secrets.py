"""PR 4 — request DTOs use SecretStr; response DTOs mask in python mode but unwrap on the JSON wire."""
import json

from pydantic import SecretStr

from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_web.api.v2._dtos import ClusterDTO, CapacityStatDTO
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
    params = BackupConfig.model_validate(_BACKUP_CONFIG)
    assert params.credentials is not None
    assert isinstance(params.credentials.access_key_id, SecretStr)
    assert isinstance(params.credentials.secret_access_key, SecretStr)
    assert params.credentials.access_key_id.get_secret_value() == "AKID"
    assert params.credentials.secret_access_key.get_secret_value() == "SK"


def test_backup_config_repr_masks_secret_values():
    text = repr(BackupConfig.model_validate(_BACKUP_CONFIG))
    assert "AKID" not in text
    assert "SK" not in text


def test_backup_config_storage_dict_keeps_secrets_wrapped():
    """write_to_db unwraps at the last moment; anything earlier leaks into logs."""
    stored = BackupConfig.model_validate(_BACKUP_CONFIG).to_storage_dict()
    assert isinstance(stored["credentials"]["access_key_id"], SecretStr)
    assert "AKID" not in repr(stored)


def test_backup_config_storage_dict_is_json_serializable():
    """Cluster.backup_config is a plain dict written through BaseModel to FDB."""
    from simplyblock_core.models.base_model import BaseModel as CoreBaseModel

    stored = BackupConfig.model_validate({
        **_BACKUP_CONFIG, "local_endpoint": "http://minio:9000",
    }).to_storage_dict()

    class _Holder(CoreBaseModel):
        backup_config: dict = {}

    holder = _Holder()
    holder.backup_config = stored
    json.dumps(holder.to_dict(unwrap_secrets=True))

    assert stored["endpoint"] == "http://minio:9000"
    assert stored["secondary_target"] == "s3"


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
