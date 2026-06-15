"""PR 4 — request DTOs use SecretStr; response DTOs mask in python mode but unwrap on the JSON wire."""
import json

from pydantic import SecretStr

from simplyblock_web.api.v2.cluster import BackupConfigParams, ClusterParams
from simplyblock_web.api.v2._dtos import ClusterDTO, CapacityStatDTO
from uuid import uuid4


def _build_capacity():
    from simplyblock_core.models.stats import StatsObject
    return CapacityStatDTO.from_model(StatsObject())


def test_backup_config_params_carry_secretstr():
    params = BackupConfigParams.model_validate({
        "access_key_id": "AKID",
        "secret_access_key": "SK",
    })
    assert isinstance(params.access_key_id, SecretStr)
    assert isinstance(params.secret_access_key, SecretStr)
    assert params.access_key_id.get_secret_value() == "AKID"
    assert params.secret_access_key.get_secret_value() == "SK"


def test_backup_config_repr_masks_secret_values():
    params = BackupConfigParams.model_validate({
        "access_key_id": "AKID",
        "secret_access_key": "SK",
    })
    text = repr(params)
    assert "AKID" not in text
    assert "SK" not in text


def test_cluster_params_grafana_secret_is_secretstr():
    params = ClusterParams.model_validate({"grafana_secret": "graf-secret"})
    assert isinstance(params.grafana_secret, SecretStr)
    assert params.grafana_secret.get_secret_value() == "graf-secret"


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
