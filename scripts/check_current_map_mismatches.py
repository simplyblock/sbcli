from simplyblock_core import db_controller, distr_controller


def main():
    db = db_controller.DBController()
    for node in db.get_storage_nodes():
        if not node.lvstore_stack:
            continue
        distribs = []
        for bdev in node.lvstore_stack:
            if bdev.get("type") == "bdev_raid":
                distribs = bdev.get("distribs_list", [])
                break
        for target in db.get_storage_nodes():
            for distr in distribs:
                try:
                    cmap = target.rpc_client(timeout=5, retry=1).distr_get_cluster_map(distr)
                except Exception as exc:
                    print(f"RPC_FAIL primary={node.get_id()} target={target.get_id()} distr={distr} err={exc}")
                    continue
                if not cmap:
                    print(f"NO_MAP primary={node.get_id()} target={target.get_id()} distr={distr}")
                    continue
                results, passed = distr_controller.parse_distr_cluster_map(cmap)
                if passed:
                    continue
                print(f"MAP_FAIL primary={node.get_id()} target={target.get_id()} distr={distr}")
                for result in results:
                    if result.get("Kind") == "Device" and result.get("Results") == "failed":
                        print(
                            f"  Device {result.get('UUID')} "
                            f"found={result.get('Found Status')} "
                            f"desired={result.get('Desired Status')}"
                        )


if __name__ == "__main__":
    main()
