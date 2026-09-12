# coding=utf-8
from simplyblock_core.models.base_model import BaseModel, default_factory


class AlertState(BaseModel):
    """The alert set as it was last observed, for one cluster.

    Alerts are derived from current state, so working out what is wrong needs
    no memory at all. Working out what has just STOPPED being wrong does: a
    resolution is a transition, and a transition is only visible against the
    previous observation. This record is that previous observation.

    One document per cluster rather than one per alert. The whole set is
    rewritten as a unit under a single compare-and-set, so two concurrent
    evaluations cannot each decide they were the one to raise the same alert
    and emit the event twice.

    ``active`` maps alert id -> the alert dict as raised, plus ``first_seen``.
    ``resolved`` maps alert id -> the same, plus ``resolved_at``; entries are
    kept there only long enough for a poller to observe the resolution, then
    dropped.
    """

    cluster_uuid: str = ""
    active: dict = default_factory(dict)
    resolved: dict = default_factory(dict)

    def get_id(self):
        return self.cluster_uuid
