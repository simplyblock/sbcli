# coding=utf-8
"""Volume backup to a secondary store.

Five modules, in dependency order -- each one may use the ones above it:

``manifest``
    The self-describing record written into the bucket next to a backup's data,
    and the only thing that can interpret those objects once the cluster that
    wrote them is gone. Owns the control plane's S3 access.
``validation``
    Whether a chain can be restored. Predicates, plus the one function that
    refuses.
``device``
    The S3 devices a node reads and writes through. One bucket each.
``controller``
    Creating, restoring, importing, exporting and discovering backups.
``policy``
    Retention limits and tiered schedules, and the merges they cause.

Deliberately not a facade: nothing is re-exported here. A caller writes

    from simplyblock_core.controllers.backup import controller as backup_controller

so the import says which part of the subsystem it depends on. Flattening that
back into one namespace would undo the reason for splitting it.
"""
