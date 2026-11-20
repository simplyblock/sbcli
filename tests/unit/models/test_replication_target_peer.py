"""The peer fields of ReplicationTarget: secret handling and the peer switch."""
import pytest
from pydantic import SecretStr

from simplyblock_core import utils
from simplyblock_core.models.replication import ReplicationTarget


def _remote_target():
    target = ReplicationTarget()
    target.uuid = "tgt-1"
    target.cluster_id = "src-cluster"
    target.target_cluster_id = "tgt-cluster"
    target.peer_endpoint = "https://cp-b.example:443"
    target.peer_secret = SecretStr("peer-plaintext")
    target.peer_ca_cert = "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"
    return target


def test_a_target_without_endpoint_is_local():
    assert not ReplicationTarget().has_peer()


def test_a_target_with_endpoint_is_remote():
    assert _remote_target().has_peer()


def test_peer_secret_round_trips_through_the_persisted_form():
    persisted = _remote_target().to_dict(unwrap_secrets=True)
    assert persisted["peer_secret"] == "peer-plaintext"

    revived = ReplicationTarget().from_dict(persisted)
    assert isinstance(revived.peer_secret, SecretStr)
    assert revived.peer_secret.get_secret_value() == "peer-plaintext"
    assert revived.peer_endpoint == "https://cp-b.example:443"
    assert revived.peer_ca_cert.startswith("-----BEGIN CERTIFICATE-----")


def test_a_record_from_before_the_peer_fields_reads_as_local():
    revived = ReplicationTarget().from_dict({
        "uuid": "tgt-old", "cluster_id": "src", "target_cluster_id": "tgt"})
    assert not revived.has_peer()
    assert revived.peer_secret.get_secret_value() == ""


def test_repr_masks_peer_secret():
    text = repr(_remote_target())
    assert "peer-plaintext" not in text
    assert "**********" in text


@pytest.mark.parametrize("unwrap", [False, True])
def test_display_json_survives_and_masks_unless_unwrapped(unwrap):
    rendered = utils.dump_json(_remote_target().get_clean_dict(), unwrap_secrets=unwrap)
    assert ("peer-plaintext" in rendered) is unwrap
