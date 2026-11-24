from simplyblock_core.models.base_model import BaseModel


class ControlPlaneLease(BaseModel):
    """Which site's control plane leads a stretched cluster
    (docs/stretched-control-plane.md).

    One record per cluster, keyed by the cluster id. A stretched cluster has a
    control plane on each of its two sites over one FoundationDB; the holder
    of this lease runs the background services (monitors, task runners) and
    answers mutating API calls, the other site's control plane stands by.

    The record is written only by ``control_plane_controller``: the lease
    agent of the holder renews it (``expires_at`` moves forward, ``epoch``
    stays), a planned move drains it (``STATE_DRAINING``: the holder's
    services stop) and releases it (``STATE_RELEASED``), and the desired
    site's agent acquires a released or expired lease with ``epoch + 1``. An
    operator's takeover releases it on the holder's behalf once the holder's
    site is confirmed down. Nothing acquires a lease another site holds and
    has not released: FoundationDB's own majority rule is what keeps a
    cut-off holder from renewing, and the agent stops the services the
    moment a renewal fails.
    """

    STATE_HELD = "held"
    STATE_DRAINING = "draining"
    STATE_RELEASED = "released"

    cluster_id: str = ""
    # The site and the management host that hold (or last held) the lease.
    holder_site: str = ""
    holder_host: str = ""
    # Incremented by every acquisition; a holder renews with a compare-and-set
    # on it and notices on the next renewal when it was superseded.
    epoch: int = 0
    state: str = ""
    # ISO-8601 UTC; a lease past it may be acquired by the desired site.
    expires_at: str = ""
    # The ISO-8601 UTC time the state last changed (the drain deadline is
    # measured from it).
    state_since: str = ""
    # How the last release came about: "handover", "takeover:<who>" or
    # "renewal_failed".
    released_by: str = ""
    updated_at: str = ""
