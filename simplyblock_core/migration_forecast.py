"""Completion forecast for a migration plan (docs/online-migration.md, G3).

Pure arithmetic over numbers the replication pipeline already records, so it
is cheap enough to recompute on every status read -- which it has to be: the
client keeps writing during the initial copy, and those writes surface as new
outstanding snapshots, so a forecast frozen at start would only drift.

The model follows the two serializations the runners impose per SOURCE
lvstore: one snapshot transfer at a time (snapshot_replication's per-LVS
transfer gate) and one cutover endgame at a time (the LVS claim in
tasks_runner_replication_final). Distinct lvstores progress in parallel.
"""
import statistics

from simplyblock_core import constants

# States whose remaining work is data still to transfer, then a cutover.
_TRANSFERRING = ("replicating", "queued")
# States already in the endgame: only (part of) the cutover remains.
_ENDGAME = ("converging", "cutting_over")


def measured_throughput(samples, min_seconds=1.0):
    """Bytes per second from ``(bytes, seconds)`` samples of completed
    transfers, or None without a usable one.

    Sub-second samples are dropped: a tiny delta's time is almost all
    orchestration (landing volume, hub attach, convert), and averaging those in
    reports a throughput the initial copy will never see.
    """
    usable = [(b, s) for b, s in samples if b > 0 and s >= min_seconds]
    if not usable:
        return None
    total_seconds = sum(s for _, s in usable)
    return sum(b for b, _ in usable) / total_seconds if total_seconds else None


def typical_cutover_seconds(durations):
    """Median measured endgame duration (claim -> done), or the default."""
    usable = [d for d in durations if d and d > 0]
    if not usable:
        return float(constants.REPL_FORECAST_DEFAULT_CUTOVER_SEC)
    return float(statistics.median(usable))


def forecast(volumes, throughput_by_lvs, cutover_seconds, now, dirty_by_lvs=None):
    """Per-volume and overall ETA.

    ``volumes`` -- plan order (largest first, which is also the order their
    cutover tasks were queued and therefore win the LVS claim); each a dict
    with ``lvol_id``, ``lvs_name``, ``state`` and ``bytes_remaining``.
    ``throughput_by_lvs`` -- {lvs_name: bytes/sec or None}.
    ``dirty_by_lvs`` -- {lvs_name: bytes/sec the clients write}: what each
    lvstore's volumes re-dirty while they are being copied.

    A migration converges only while replication outruns the clients' writes:
    the cutover waits until the lag is below REPL_CUTOVER_ENDGAME_LAG_SEC, and
    a volume dirtied faster than it ships never gets there. Such an lvstore has
    no ETA; its volumes report 0, and the overall verdict is not converging.
    Otherwise progress is replication minus what is re-dirtied meanwhile.

    Returns ``(per_volume, overall)``: per_volume maps lvol_id to
    ``eta_seconds``; overall is a dict with bytes_remaining, eta_seconds,
    completes_at, throughput_bytes_per_sec, throughput_measured, converging,
    dirty_bytes_per_sec and not_converging_lvs.
    """
    default_rate = float(constants.REPL_FORECAST_DEFAULT_BYTES_PER_SEC)
    dirty_by_lvs = dirty_by_lvs or {}
    by_lvs: dict = {}
    for vol in volumes:
        if vol["state"] in _TRANSFERRING + _ENDGAME:
            by_lvs.setdefault(vol.get("lvs_name", ""), []).append(vol)

    per_volume = {}
    measured_rates = []
    any_unmeasured = False
    not_converging = []
    eta = 0.0
    for lvs_name, queue in by_lvs.items():
        rate = throughput_by_lvs.get(lvs_name)
        if rate:
            measured_rates.append(rate)
        else:
            any_unmeasured = True
            rate = default_rate
        dirty = float(dirty_by_lvs.get(lvs_name) or 0)
        if dirty >= rate * constants.REPL_CONVERGENCE_MARGIN:
            not_converging.append(lvs_name)
            for vol in queue:
                per_volume[vol["lvol_id"]] = 0
            continue
        effective = rate - dirty
        transferred_at = 0.0     # when this LVS's transfer queue reaches a volume's end
        cutover_free_at = 0.0    # when the LVS claim is next free
        # The endgame holders go first: they hold (or are about to hold) the claim.
        ordered = ([v for v in queue if v["state"] in _ENDGAME]
                   + [v for v in queue if v["state"] in _TRANSFERRING])
        for vol in ordered:
            transferred_at += max(0, vol.get("bytes_remaining") or 0) / effective
            cutover_free_at = max(cutover_free_at, transferred_at) + cutover_seconds
            per_volume[vol["lvol_id"]] = round(cutover_free_at)
        eta = max(eta, cutover_free_at)

    bytes_remaining = sum(max(0, v.get("bytes_remaining") or 0)
                          for queue in by_lvs.values() for v in queue)
    converging = not not_converging
    overall = {
        "bytes_remaining": bytes_remaining,
        "eta_seconds": round(eta) if converging else 0,
        "completes_at": int(now + eta) if converging else 0,
        "throughput_bytes_per_sec": int(
            sum(measured_rates) / len(measured_rates) if measured_rates else default_rate),
        "throughput_measured": bool(measured_rates) and not any_unmeasured,
        "converging": converging,
        "dirty_bytes_per_sec": int(sum(float(v or 0) for v in dirty_by_lvs.values())),
        "not_converging_lvs": sorted(not_converging),
    }
    return per_volume, overall
