"""Event folding for the two-node arbiter (contract sections 4 and 5)."""

from simplyblock_core.arbitration import events as ev
from simplyblock_core.models.arbitration import LVS_FENCED, LVS_HOLDING, LVS_NORMAL, LVS_SOLO


def e(seq, status, vuid=3, instance="i1", **kw):
    return dict(seq=seq, status=status, jm_vuid=vuid, instance=instance, **kw)


def test_unhealthy_then_hold_then_healthy():
    s = ev.NodeSignals()
    assert ev.apply(s, e(1, ev.ST_UNHEALTHY, ha_state=LVS_HOLDING))
    assert s.reports_peer_unhealthy() and s.lvs_state[3] == LVS_HOLDING
    assert ev.apply(s, e(2, "ha_solo", epoch=8))
    assert s.lvs_state[3] == LVS_SOLO and s.epoch_seen == 8
    assert ev.apply(s, e(3, ev.ST_HEALTHY))
    assert s.reports_peer_healthy() and not s.reports_peer_unhealthy()


def test_duplicates_and_stale_are_dropped():
    s = ev.NodeSignals()
    assert ev.apply(s, e(5, ev.ST_UNHEALTHY))
    assert not ev.apply(s, e(5, ev.ST_HEALTHY))
    assert not ev.apply(s, e(4, ev.ST_HEALTHY))
    assert s.reports_peer_unhealthy()


def test_instance_change_resets_sequence():
    s = ev.NodeSignals()
    ev.apply(s, e(90, ev.ST_UNHEALTHY))
    assert ev.apply(s, e(1, ev.ST_HEALTHY, instance="i2"))
    assert s.instance == "i2" and s.last_seq == 1


def test_resync_replaces_the_view():
    s = ev.NodeSignals()
    ev.apply(s, e(3, ev.ST_UNHEALTHY))
    status = {"instance": "i3", "seq": 41, "epoch": 9, "lvs": [
        {"jm_vuid": 3, "state": "fenced", "remote_jm": {"health": "unhealthy"}},
        {"jm_vuid": 4, "state": "normal", "remote_jm": {"health": "healthy"}}]}
    assert ev.apply(s, {"status": ev.ST_RESYNC, "instance": "i3", "ha_status": status})
    assert s.instance == "i3" and s.last_seq == 41 and s.epoch_seen == 9
    assert s.lvs_state == {3: LVS_FENCED, 4: LVS_NORMAL}
    assert s.peer_unhealthy == {3: True, 4: False}
    assert not ev.apply(s, e(41, ev.ST_HEALTHY, instance="i3"))


def test_self_fence_and_unfence():
    s = ev.NodeSignals()
    ev.apply(s, e(1, "ha_self_fenced"))
    assert s.lvs_state[3] == LVS_FENCED
    ev.apply(s, e(2, "ha_unfenced"))
    assert s.lvs_state[3] == LVS_NORMAL
