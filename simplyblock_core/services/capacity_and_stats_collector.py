# coding=utf-8
import statistics
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import device_events
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject, ClusterStatObject

logger = utils.get_logger(__name__)


last_object_record: dict[str, DeviceStatObject] = {}

# --- Per-device latency-deviation detection -----------------------------------
# Sliding-window gray-failure early-warning: if a device's mean block-size-
# normalized IO latency (latency ticks per byte) over LATENCY_WINDOW_SEC exceeds
# LATENCY_OUTLIER_FACTOR x the average of the cluster's other active devices, log
# a WARNING cluster event. Catches a slow-but-not-dead device that the hard
# exclusion path can miss because its NVMe controller still reports "connected"
# (incident 2026-06-24: device 31 was 2x+ slower / timing out while looking
# healthy). Normalizing by average block size (ticks/byte rather than ticks/op)
# keeps devices serving different IO sizes comparable; a device carrying >= 2x
# the cluster-average IOPS or throughput is exempted (its latency is load, not
# degradation), and a device with < LATENCY_MIN_WINDOW_IO ops over the window is
# not judged at all.
LATENCY_WINDOW_SEC = 600          # ~10 min sliding window
LATENCY_OUTLIER_FACTOR = 2.0      # > 2x the cluster average => warn
# Load exemption: a device carrying a disproportionate share of the cluster
# load is *expected* to show higher per-op latency (queueing under load), which
# is not degradation. If a flagged device's windowed-mean IOPS OR throughput is
# at least this many times the cluster average, suppress the warning regardless
# of latency.
LATENCY_LOAD_EXEMPT_FACTOR = 2.0  # >= 2x avg IOPS or throughput => not a warning
LATENCY_MIN_IO_PS = 50            # ignore near-idle cycles (no meaningful latency)
LATENCY_MIN_WINDOW_IO = 500       # need >= 500 ops over the window to judge a device
LATENCY_MIN_SAMPLES = 3           # need a few in-window samples before judging
LATENCY_MIN_DEVICES = 3           # need a meaningful cluster baseline
# Warn once for a degraded device, then stay quiet for 12h before repeating the
# warning for that same device — long enough that a persistently slow device
# does not spam the log every few minutes, while still re-surfacing if it is
# still degraded half a day later.
LATENCY_WARN_COOLDOWN_SEC = 12 * 60 * 60   # 12 hours; repeat per device at most this often
# device_id -> list[(timestamp, latency_ticks_per_op)]
_latency_window: dict = {}
# device_id -> last-warned timestamp
_latency_last_warn: dict = {}


def detect_latency_outliers(device_records):
    """Sliding-window latency-deviation warning.

    device_records: list of (device, DeviceStatObject) collected this cycle for
    one cluster. Appends each active device's per-op latency to its window,
    prunes to LATENCY_WINDOW_SEC, then flags devices whose windowed mean exceeds
    LATENCY_OUTLIER_FACTOR x the average of all active devices.
    """
    now = int(time.time())
    cutoff = now - LATENCY_WINDOW_SEC
    # dev_id -> (device, lat_mean ticks/op, iops_mean ops/s, bw_mean bytes/s)
    device_means = {}
    for device, rec in device_records:
        try:
            total_io = int(rec['read_io_ps']) + int(rec['write_io_ps'])
            total_lat = int(rec['read_latency_ps']) + int(rec['write_latency_ps'])
            total_bytes = int(rec['read_bytes_ps']) + int(rec['write_bytes_ps'])
        except (KeyError, TypeError, ValueError):
            continue
        if total_io < LATENCY_MIN_IO_PS:
            continue  # near-idle: no meaningful latency this cycle
        if total_bytes <= 0:
            continue  # cannot block-size-normalize without bytes moved
        # Block-size-normalized latency: per-op latency divided by the average
        # block size = (total_lat/total_io) / (total_bytes/total_io)
        # = total_lat/total_bytes, i.e. latency ticks per byte. This makes
        # devices serving different IO sizes directly comparable (a device doing
        # larger ops is no longer "slower" just for that).
        lat_per_byte = total_lat / total_bytes
        win = _latency_window.setdefault(device.get_id(), [])
        # sample: (timestamp, latency-per-byte, total IOPS, total throughput bytes/s)
        win.append((now, lat_per_byte, total_io, total_bytes))
        win[:] = [s for s in win if s[0] >= cutoff]
        # Require a meaningful amount of IO over the window, not just a few
        # qualifying cycles: estimate ops served in the window from the sampled
        # rates (~one sample per collector cycle of DEV_STAT_COLLECTOR_INTERVAL_SEC).
        est_window_io = sum(s[2] for s in win) * constants.DEV_STAT_COLLECTOR_INTERVAL_SEC
        if len(win) >= LATENCY_MIN_SAMPLES and est_window_io >= LATENCY_MIN_WINDOW_IO:
            device_means[device.get_id()] = (
                device,
                statistics.fmean(s[1] for s in win),
                statistics.fmean(s[2] for s in win),
                statistics.fmean(s[3] for s in win),
            )

    if len(device_means) < LATENCY_MIN_DEVICES:
        return
    baseline = statistics.fmean(m[1] for m in device_means.values())
    iops_baseline = statistics.fmean(m[2] for m in device_means.values())
    bw_baseline = statistics.fmean(m[3] for m in device_means.values())
    if baseline <= 0:
        return
    for dev_id, (device, dmean, diops, dbw) in device_means.items():
        if dmean <= LATENCY_OUTLIER_FACTOR * baseline:
            continue
        # Load exemption: a device carrying >= LATENCY_LOAD_EXEMPT_FACTOR x the
        # cluster-average IOPS or throughput is expected to be slower (queueing
        # under its heavier load), not degraded — do not warn.
        if iops_baseline > 0 and diops >= LATENCY_LOAD_EXEMPT_FACTOR * iops_baseline:
            logger.info(
                "Device %s latency is high but its load is %.1fx avg IOPS; "
                "treating as load, not degradation (no warning)",
                device.get_id(), diops / iops_baseline)
            continue
        if bw_baseline > 0 and dbw >= LATENCY_LOAD_EXEMPT_FACTOR * bw_baseline:
            logger.info(
                "Device %s latency is high but its load is %.1fx avg throughput; "
                "treating as load, not degradation (no warning)",
                device.get_id(), dbw / bw_baseline)
            continue
        if now - _latency_last_warn.get(dev_id, 0) < LATENCY_WARN_COOLDOWN_SEC:
            continue
        _latency_last_warn[dev_id] = now
        ratio = dmean / baseline
        msg = (f"Device {device.get_id()} (storage_id {device.cluster_device_order}) "
               f"block-size-normalized IO latency {ratio:.1f}x the cluster average over "
               f"{LATENCY_WINDOW_SEC // 60}m ({dmean:.2f} vs {baseline:.2f} ticks/byte) "
               f"- possible degraded device")
        logger.warning(msg)
        try:
            device_events.device_latency_outlier(device, msg)
        except Exception as e:
            logger.error(f"Failed to log latency-outlier event for {dev_id}: {e}")


def add_device_stats(cl, device, capacity_dict, stats_dict):
    now = int(time.time())
    data = {
        "cluster_id": cl.get_id(),
        "uuid": device.get_id(),
        "date": now}

    if capacity_dict and capacity_dict['res'] == 1:
        size_total = int(capacity_dict['npages_nmax']*capacity_dict['pba_page_size'])
        size_used = int(capacity_dict['npages_used']*capacity_dict['pba_page_size'])
        size_free = size_total - size_used
        size_util = 0
        if size_total > 0:
            size_util = int((size_used / size_total) * 100)

        data.update({
            "size_total": size_total,
            "size_used": size_used,
            "size_free": size_free,
            "size_util": size_util,
            "capacity_dict": capacity_dict
        })
    else:
        logger.error(f"Error getting Alceml capacity, response={capacity_dict}")

    if stats_dict:
        stats = stats_dict
        data.update({
            "read_bytes": stats['bytes_read'],
            "read_io": stats['num_read_ops'],
            "read_latency_ticks": stats['read_latency_ticks'],

            "write_bytes": stats['bytes_written'],
            "write_io": stats['num_write_ops'],
            "write_latency_ticks": stats['write_latency_ticks'],

            "unmap_bytes": stats['bytes_unmapped'],
            "unmap_io": stats['num_unmap_ops'],
            "unmap_latency_ticks": stats['unmap_latency_ticks'],
        })

        if device.get_id() in last_object_record:
            last_record = last_object_record[device.get_id()]
        else:
            last_record = DeviceStatObject(data={"uuid": device.get_id(), "cluster_id": cl.get_id()}
                                           ).get_last(db.kv_store)
        if last_record:
            time_diff = (now - last_record.date)
            if time_diff > 0:
                data['read_bytes_ps'] = abs(int((data['read_bytes'] - last_record['read_bytes']) / time_diff))
                data['read_io_ps'] = abs(int((data['read_io'] - last_record['read_io']) / time_diff))
                data['read_latency_ps'] = abs(int((data['read_latency_ticks'] - last_record['read_latency_ticks']) / time_diff))

                data['write_bytes_ps'] = abs(int((data['write_bytes'] - last_record['write_bytes']) / time_diff))
                data['write_io_ps'] = abs(int((data['write_io'] - last_record['write_io']) / time_diff))
                data['write_latency_ps'] = abs(int((data['write_latency_ticks'] - last_record['write_latency_ticks']) / time_diff))

                data['unmap_bytes_ps'] = abs(int((data['unmap_bytes'] - last_record['unmap_bytes']) / time_diff))
                data['unmap_io_ps'] = abs(int((data['unmap_io'] - last_record['unmap_io']) / time_diff))
                data['unmap_latency_ps'] = abs(int((data['unmap_latency_ticks'] - last_record['unmap_latency_ticks']) / time_diff))

        else:
            logger.warning("last record not found")
    else:
        logger.error("Error getting stats")

    stat_obj = DeviceStatObject(data=data)
    stat_obj.write_to_db(db.kv_store)
    last_object_record[device.get_id()] = stat_obj

    all_stats = db.get_device_stats(device, limit=0)
    if len(all_stats) > 10:
        for st in all_stats[10:]:
            st.remove(db.kv_store)

    return stat_obj


def add_node_stats(cluster, node, records, all_lvols):
    size_used = 0
    size_total = 0
    data = {}
    if records:
        records_sum = utils.sum_records(records)
        size_total = records_sum.size_total
        size_used = records_sum.size_used
        data.update(records_sum.get_clean_dict())

    size_prov = 0
    for lvol in all_lvols:
        if lvol.node_id == node.get_id():
            size_prov += lvol.size

    size_util = 0
    size_prov_util = 0
    if size_total > 0:
        size_util = int((size_used / size_total) * 100)
        size_prov_util = int((size_prov / size_total) * 100)

    data.update({
        "cluster_id": cluster.get_id(),
        "uuid": node.get_id(),
        "date": int(time.time()),
        "size_util": size_util,
        "size_prov": size_prov,
        "size_prov_util": size_prov_util
    })
    stat_obj = NodeStatObject(data=data)
    stat_obj.write_to_db(db.kv_store)

    all_stats = db.get_node_stats(node, limit=0)
    if len(all_stats) > 10:
        for st in all_stats[10:]:
            st.remove(db.kv_store)

    return stat_obj


def add_cluster_stats(cl, records):

    if not records:
        return False

    records_sum = utils.sum_records(records)

    size_util = 0
    size_prov_util = 0
    if records_sum.size_total > 0:
        size_util = int((records_sum.size_used / records_sum.size_total) * 100)
        size_prov_util = int((records_sum.size_prov / records_sum.size_total) * 100)

    data = records_sum.get_clean_dict()
    data.update({
        "cluster_id": cl.get_id(),
        "uuid": cl.get_id(),
        "date": int(time.time()),

        "size_util": size_util,
        "size_prov_util": size_prov_util
    })

    stat_obj = ClusterStatObject(data=data)
    stat_obj.write_to_db(db.kv_store)

    all_stats = db.get_cluster_stats(cl, limit=0)
    if len(all_stats) > 10:
        for st in all_stats[10:]:
            st.remove(db.kv_store)

    return stat_obj



# get DB controller
db = db_controller.DBController()


def main():
    logger.info("Starting capacity and stats collector...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cl in clusters:
            snodes = db.get_storage_nodes_by_cluster_id(cl.get_id())
            if not snodes:
                logger.error(f"Cluster has no storage nodes: {cl.get_id()}")

            all_lvols =  db.get_mini_lvols()
            node_records = []
            cluster_device_records = []
            for node in snodes:
                logger.info("Node: %s", node.get_id())
                if node.status != StorageNode.STATUS_ONLINE:
                    logger.info("Node is not online, skipping")
                    continue

                if not node.nvme_devices:
                    logger.error("No devices found in node: %s", node.get_id())
                    continue

                rpc_client = node.rpc_client(timeout=5, retry=2)
                devices_records = []
                for device in node.nvme_devices:
                    logger.info("Getting device stats: %s", device.uuid)
                    if device.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                        logger.info(f"Device is skipped: {device.get_id()} status: {device.status}")
                        continue
                    try:
                        capacity_dict = rpc_client.alceml_get_capacity(device.alceml_name)
                    except Exception as e:
                        logger.error(e)
                        continue
                    ret = rpc_client.get_lvol_stats(device.nvme_bdev)
                    if ret:
                        stats_dict = ret['bdevs'][0]
                        record = add_device_stats(cl, device, capacity_dict, stats_dict)
                        if record:
                            devices_records.append(record)
                            cluster_device_records.append((device, record))

                node_record = add_node_stats(cl, node, devices_records, all_lvols)
                node_records.append(node_record)

            add_cluster_stats(cl, node_records)

            try:
                detect_latency_outliers(cluster_device_records)
            except Exception as e:
                logger.error(f"latency-outlier detection failed for cluster {cl.get_id()}: {e}")

        time.sleep(constants.DEV_STAT_COLLECTOR_INTERVAL_SEC)


if __name__ == "__main__":
    main()
