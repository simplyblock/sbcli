"""One-shot repair for the 2026-07-17 expansion aftermath.

Fixes the two gaps left by an expansion that ran before the executor fixes
(hublvol creation skipped on the newcomer's LVS; lvol.nodes still pointing
at the re-home donor):

1. Newcomer primary: create the missing hublvol + transfer hublvol, then
   propagate the hublvol port to the sec/tert holders' lvstore_ports and
   (re)connect them to the hublvol.
2. Re-homed LVS: swap donor -> recipient in lvol.nodes of the affected
   primary's lvols, and delete the stale (namespace-less) per-lvol
   subsystem shells the lvol monitor recreated on the donor.

Usage:
    python scripts/repair_expansion_hublvol_lvol_nodes.py \
        <newcomer_primary_id> <rehomed_primary_id> <donor_id> <recipient_id>

For the vm201-204 lab incident:
    newcomer_primary_id = 4e07dc35-f148-4457-adfe-b7a48217553a  (vm204)
    rehomed_primary_id  = 6d340bc1-8eff-4a3e-8c62-a5a69e13a1be  (vm203)
    donor_id            = e45345e9-40a3-4f76-a583-28309512f1d2  (vm201)
    recipient_id        = 4e07dc35-f148-4457-adfe-b7a48217553a  (vm204)
"""

import sys

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol


def repair_newcomer_hublvol(db, newcomer_id):
    primary = db.get_storage_node_by_id(newcomer_id)
    cluster = db.get_cluster_by_id(primary.cluster_id)
    lvs = primary.lvstore
    print(f"newcomer {newcomer_id} lvstore={lvs}")

    if not (primary.hublvol and primary.hublvol.uuid):
        print("  hublvol missing -> creating")
        primary.create_hublvol(cluster_nqn=cluster.nqn)
        primary.create_transfer_hublvol()
        primary = db.get_storage_node_by_id(newcomer_id)
    else:
        print(f"  hublvol present: {primary.hublvol.nqn}:{primary.hublvol.nvmf_port}")

    hub_port = primary.hublvol.nvmf_port
    holder_ids = [n for n in (primary.secondary_node_id,
                              primary.tertiary_node_id) if n]
    for holder_id in holder_ids:
        holder = db.get_storage_node_by_id(holder_id)
        ports = dict(holder.lvstore_ports or {})
        entry = dict(ports.get(lvs, {}))
        if entry.get("hublvol_port") != hub_port:
            print(f"  {holder_id}: lvstore_ports[{lvs}].hublvol_port "
                  f"{entry.get('hublvol_port')} -> {hub_port}")
            entry["hublvol_port"] = hub_port
            ports[lvs] = entry
            holder.lvstore_ports = ports
            holder.write_to_db()
            holder = db.get_storage_node_by_id(holder_id)
        ok = holder.connect_to_hublvol(primary)
        print(f"  {holder_id}: connect_to_hublvol -> {ok}")


def repair_lvol_nodes(db, primary_id, donor_id, recipient_id):
    donor = db.get_storage_node_by_id(donor_id)
    donor_rpc = donor.rpc_client()
    for lvol in db.get_lvols_by_node_id(primary_id):
        if lvol.status == LVol.STATUS_IN_DELETION:
            continue
        nodes = list(lvol.nodes or [])
        if donor_id in nodes:
            lvol.nodes = [recipient_id if n == donor_id else n for n in nodes]
            lvol.write_to_db()
            print(f"  lvol {lvol.get_id()}: nodes {nodes} -> {lvol.nodes}")
        try:
            ret = donor_rpc.subsystem_delete(lvol.nqn)
            print(f"  lvol {lvol.get_id()}: stale subsystem on donor deleted ({ret})")
        except Exception as e:
            print(f"  lvol {lvol.get_id()}: subsystem_delete on donor: {e}")


def main():
    if len(sys.argv) != 5:
        print(__doc__)
        sys.exit(1)
    newcomer_id, rehomed_primary_id, donor_id, recipient_id = sys.argv[1:5]
    db = DBController()
    repair_newcomer_hublvol(db, newcomer_id)
    print(f"re-homed LVS of primary {rehomed_primary_id}: "
          f"{donor_id} -> {recipient_id}")
    repair_lvol_nodes(db, rehomed_primary_id, donor_id, recipient_id)
    print("done — re-run `sbctl sn check <newcomer>` and `sbctl lvol check <id>`")


if __name__ == "__main__":
    main()
