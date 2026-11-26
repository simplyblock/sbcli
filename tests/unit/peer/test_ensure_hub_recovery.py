"""ensure_hub on the target: a leaderless lvstore answers ``unsettled`` with
its members' states, and runs the guarded leadership recovery only when the
source asks for it (EnsureHubRequest.recover_leadership).

The source decides when to ask (snapshot_replication._leadership_recovery_due);
the target runs the recovery because only its control plane reaches the
target's nodes. See tests/unit/test_healthy_leaderless_recovery.py for the
source's side.
"""

import types
from unittest.mock import MagicMock, patch

from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import EnsureHubRequest

LANDING = types.SimpleNamespace(node_id="n1", nodes=["n1", "n2"], lvs_name="LVS_7",
                                pool_uuid="pool-1", top_bdev="LVS_7/LAND")


def _ensure_hub(recover):
    db = MagicMock()
    db.get_lvol_by_id.return_value = LANDING
    db.get_pool_by_id.return_value = types.SimpleNamespace(cluster_id="CL")
    db.get_storage_node_by_id.side_effect = lambda i: types.SimpleNamespace(status="online")
    with patch.object(service, "DBController", return_value=db), \
            patch.object(service, "stable_leader", return_value=(None, "0 members report")), \
            patch.object(service, "_recover_leadership") as recovery:
        answer = service.ensure_hub("CL", EnsureHubRequest(
            landing_lvol_id="land-1", recover_leadership=recover,
            recover_reason="replication task t waited 61s" if recover else ""))
    return answer, recovery


def test_an_unsettled_lvstore_reports_its_members():
    answer, recovery = _ensure_hub(recover=False)
    assert answer.state == "unsettled"
    assert answer.members == "n1 online, n2 online"
    recovery.assert_not_called()


def test_the_recovery_runs_only_when_the_source_asks():
    answer, recovery = _ensure_hub(recover=True)
    assert answer.state == "unsettled"
    recovery.assert_called_once_with(LANDING, "replication task t waited 61s")


def test_a_failing_recovery_is_logged_not_raised():
    with patch("simplyblock_core.storage_node_ops.restore_healthy_lvs_leadership",
               side_effect=RuntimeError("fdb")):
        service._recover_leadership(LANDING, "why")      # must not raise
