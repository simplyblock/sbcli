"""Diagnose persistent Distr-map health=False: for each node, fetch the SPDK
cluster map for each of its distrib bdevs and compare against the mgmt DB,
printing only the mismatching entries."""
from simplyblock_core.db_controller import DBController
from simplyblock_core import distr_controller

db = DBController()
sns = db.get_storage_nodes()

nodes = {}
devices = {}
for n in sns:
    nodes[n.get_id()] = n
    for dev in n.nvme_devices:
        devices[dev.get_id()] = dev

# Reverse lookup so we can name mismatching uuids.
dev_owner = {dev.get_id(): n.get_id() for n in sns for dev in n.nvme_devices}

print(f"DB view: {len(nodes)} nodes, {len(devices)} devices")
for n in sns:
    print(f"  node {n.get_id()[:8]} status={n.status} health={n.health_check} "
          f"ndev={len(n.nvme_devices)} jm_ids={[j[:8] for j in (n.jm_ids or [])]}")

for n in sns:
    distribs = []
    for bdev in (n.lvstore_stack or []):
        if bdev.get('type') == 'bdev_raid':
            distribs += bdev.get('distribs_list', [])
    if not distribs:
        continue
    for distr in distribs:
        try:
            m = n.rpc_client().distr_get_cluster_map(distr)
        except Exception as e:
            print(f"[{n.get_id()[:8]}] {distr}: RPC failed: {e}")
            continue
        results, passed = distr_controller.parse_distr_cluster_map(m, nodes, devices)
        bad = [r for r in results if r['Results'] != 'ok']
        print(f"[{n.get_id()[:8]}] {distr}: passed={passed} entries={len(results)} bad={len(bad)}")
        for r in bad:
            extra = ""
            if r['Kind'] == 'Device':
                owner = dev_owner.get(r['UUID'])
                extra = f" owner_node={owner[:8] if owner else 'UNKNOWN'}"
            print(f"    {r['Kind']} {r['UUID'][:12]} found={r['Found Status']!r} "
                  f"desired={r['Desired Status']!r} -> {r['Results']}{extra}")
        # Only need one distrib per node to see the pattern.
        break
