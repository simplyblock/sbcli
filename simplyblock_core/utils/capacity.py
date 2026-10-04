"""Raw vs. effective capacity.

Every byte figure in the control plane is one of two things:

**raw**
    Physical bytes on the NVMe devices. Erasure-coding parity lives here
    alongside the data, so an ``ndcs`` + ``npcs`` cluster spends
    ``(ndcs + npcs) / ndcs`` raw bytes for every byte a client stores.
    ``alceml_get_capacity`` (``npages_nmax``/``npages_used``) and
    ``NVMeDevice.size`` are raw.

**effective**
    Bytes as a client sees them: the logical size of an lvol, the sum of
    provisioned lvol sizes, the space a snapshot's allocated clusters
    represent. Everything derived from the lvstore/blobstore layer
    (``num_blocks * block_size``, ``num_allocated_clusters * cluster_sz``) is
    effective, because the distrib bdev underneath already hides parity.

The two were mixed: ``size_prov`` (effective) was divided by ``size_total``
(raw) to produce ``size_prov_util``, so provisioned utilisation was
understated by exactly ``(ndcs + npcs) / ndcs`` — a factor of 1.5 on a 4+2
cluster, which is enough for ``prov_cap_crit`` to never fire. Absolute
utilisation (``size_util``) was unaffected, both of its operands being raw.

The control plane now reports **effective** bytes at every level. Conversion
happens once, where raw device numbers enter the system
(``capacity_and_stats_collector.add_device_stats``); node and cluster records
are sums of already-converted device records, so they need no conversion of
their own. The raw numbers are kept alongside in the ``*_raw`` stat fields so
nothing that was measured is thrown away.
"""


from simplyblock_core.models.cluster import Cluster


def stripe_geometry(cluster: Cluster) -> tuple[int, int]:
    """Return ``(ndcs, npcs)`` for ``cluster``, or ``(1, 0)`` if unset.

    ``distr_ndcs``/``distr_npcs`` default to 0 on the model, so a record
    written before the cluster was configured (or a hand-built ``Cluster()``
    in a test) carries no geometry. ``(1, 0)`` makes the conversions below
    identities rather than a ZeroDivisionError: with no geometry known, raw
    *is* the best estimate of effective.
    """
    ndcs = cluster.distr_ndcs
    npcs = cluster.distr_npcs
    if ndcs < 1 or npcs < 0:
        return 1, 0
    return ndcs, npcs


def to_effective(raw_bytes: int, cluster: Cluster) -> int:
    """Convert raw (physical) bytes to effective (client-visible) bytes.

    Integer arithmetic throughout — the operands are byte counts, and a float
    round-trip loses exactness above 2**53 bytes (8 PiB), which is inside the
    range a large cluster reaches.
    """
    ndcs, npcs = stripe_geometry(cluster)
    return raw_bytes * ndcs // (ndcs + npcs)


def to_raw(effective_bytes: int, cluster: Cluster) -> int:
    """Convert effective (client-visible) bytes to raw (physical) bytes.

    Rounds up: the raw cost of storing ``effective_bytes`` is never less than
    this, so a capacity check built on it cannot admit an over-commit by a
    rounding remainder.
    """
    ndcs, npcs = stripe_geometry(cluster)
    return -(-effective_bytes * (ndcs + npcs) // ndcs)
