"""lvol migration: moving one volume between nodes of ONE cluster.

Not to be confused with three things we already test under similar names:

    DeviceFailureMigration*   a device fails, data rebalances
    TestMigrationLifecycle    node-level, does not call the migrate verbs
    ReplicationMigration      cross-CLUSTER, via replication-start/commit

This is the two-phase handshake on `volume migrate` / `volume
migrate-continue`, which had no e2e coverage at all before this package.

Ported from the scripts on `origin/lvol-migration-test-scripts`, which were
run by hand against manually deployed clusters and found most of the bugs
this feature has. What changed in the port is recorded in migration_base.py.

    MIG-H  happy path, and the volume is still usable afterwards
    MIG-F  faults injected into a named phase  <- where the bugs were
    MIG-T  HA overlap matrix, snapshot/clone trees, concurrency
    MIG-B  shared-namespace groups (the --batch path)
    MIG-N  migrations that must be refused, and one that must not be
    MIG-P  scale and soak  -- SEPARATE LANE, --testname migration-stress
"""
