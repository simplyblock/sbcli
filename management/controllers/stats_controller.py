from management.kv_store import DBController
from management.models.device_stat import DeviceStat


def _get_node_io_data(node):
    db_controller = DBController()
    total_values = {
        "node_id": node.get_id(),
        "read_bytes_per_sec": 0,
        "read_iops": 0,
        "write_bytes_per_sec": 0,
        "write_iops": 0,
        "unmapped_bytes_per_sec": 0,
        "read_latency_ticks": 0,
        "write_latency_ticks": 0,
    }
    for dev in node.nvme_devices:
        record = DeviceStat(data={"uuid": dev.get_id(), "node_id": node.get_id()}).get_last(db_controller.kv_store)
        if not record:
            continue
        total_values["read_bytes_per_sec"] += record.read_bytes_per_sec
        total_values["read_iops"] += record.read_iops
        total_values["write_bytes_per_sec"] += record.write_bytes_per_sec
        total_values["write_iops"] += record.write_iops
        total_values["unmapped_bytes_per_sec"] += record.unmapped_bytes_per_sec
        total_values["read_latency_ticks"] += record.read_latency_ticks
        total_values["write_latency_ticks"] += record.write_latency_ticks

    return total_values


def get_iostats(cluster_id):
    db_controller = DBController()
    cls = db_controller.get_clusters(id=cluster_id)
    if not cls:
        logger.error(f"Cluster not found {cluster_id}")
        return False
    cl = cls[0]

    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    if not nodes:
        logger.error("no nodes found")
        return False

    out = []
    records = []
    for node in nodes:
        record = _get_node_io_data(node)
        if not record:
            continue
        records.append(record)
        out.append({
            "Node": record['node_id'],
            "bytes_read (B/s)": utils.humanbytes(record['read_bytes_per_sec']),
            "num_read_ops (IOPS)": record["read_iops"],
            "bytes_write (B/s)": utils.humanbytes(record["write_bytes_per_sec"]),
            "num_write_ops (IOPS)": record["write_iops"],
            "bytes_unmapped (B/s)": utils.humanbytes(record["unmapped_bytes_per_sec"]),
            "read_latency_ticks": record["read_latency_ticks"],
            "write_latency_ticks": record["write_latency_ticks"],
        })

    total_values = utils.dict_agg(records)
    if total_values:
        out.append({
            "Node": "Total",
            "bytes_read (B/s)": utils.humanbytes(total_values['read_bytes_per_sec']),
            "num_read_ops (IOPS)": total_values["read_iops"],
            "bytes_write (B/s)": utils.humanbytes(total_values["write_bytes_per_sec"]),
            "num_write_ops (IOPS)": total_values["write_iops"],
            "bytes_unmapped (B/s)": utils.humanbytes(total_values["unmapped_bytes_per_sec"]),
            "read_latency_ticks": total_values["read_latency_ticks"],
            "write_latency_ticks": total_values["write_latency_ticks"],
        })

    return out


def _get_random_data(records_count):
    data = []
    now = int(time.time())
    for index in range(records_count):
        d = {}
        d["date"] = now - (index*2)
        d["read_bytes_per_sec"] = random.randint(0, 1024*1024*1024)
        d["read_iops"] = random.randint(0, 1000)
        d["write_bytes_per_sec"] = random.randint(0, 1024*1024*1024)
        d["write_iops"] = random.randint(0, 1000)
        d["unmapped_bytes_per_sec"] = random.randint(0, 1024*1024*1024)
        d["read_latency_ticks"] = random.randint(0, 100)
        d["write_latency_ticks"] = random.randint(0, 100)
        data.append(d)
    return data


def _get_nodes_io_data(nodes, history):
    db_controller = DBController()
    data = []
    for node in nodes:
        for dev in node.nvme_devices:
            stats = db_controller.get_device_stats(dev, history)
            for index, record in enumerate(stats):
                # possible bug here :)
                if index < len(data):
                    data[index]["read_bytes_per_sec"] += record.read_bytes_per_sec
                    data[index]["read_iops"] += record.read_iops
                    data[index]["write_bytes_per_sec"] += record.write_bytes_per_sec
                    data[index]["write_iops"] += record.write_iops
                    data[index]["unmapped_bytes_per_sec"] += record.unmapped_bytes_per_sec
                    data[index]["read_latency_ticks"] += record.read_latency_ticks
                    data[index]["write_latency_ticks"] += record.write_latency_ticks
                else:
                    data.insert(index, {})
                    data[index]["date"] = record.date
                    data[index]["read_bytes_per_sec"] = record.read_bytes_per_sec
                    data[index]["read_iops"] = record.read_iops
                    data[index]["write_bytes_per_sec"] = record.write_bytes_per_sec
                    data[index]["write_iops"] = record.write_iops
                    data[index]["unmapped_bytes_per_sec"] = record.unmapped_bytes_per_sec
                    data[index]["read_latency_ticks"] = record.read_latency_ticks
                    data[index]["write_latency_ticks"] = record.write_latency_ticks
    return data


def get_iostats_history(cluster_id, history_string, records_count=20, random_data=False):
    db_controller = DBController()
    cls = db_controller.get_clusters(id=cluster_id)
    if not cls:
        logger.error(f"Cluster not found {cluster_id}")
        return False
    cl = cls[0]

    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    if not nodes:
        logger.error("no nodes found")
        return False

    out = []

    if not history_string:
        logger.error("Invalid history value")
        return False

    # process history
    results = re.search(r'^(\d+[hmd])(\d+[hmd])?$', history_string.lower())
    if not results:
        logger.error(f"Error parsing history string: {history_string}")
        logger.info(f"History format: xxdyyh , e.g: 1d12h, 1d, 2h, 1m")
        return

    history_in_seconds = 0
    for s in results.groups():
        if not s:
            continue
        ind = s[-1]
        v = int(s[:-1])
        if ind == 'd':
            history_in_seconds += v * (60*60*24)
        if ind == 'h':
            history_in_seconds += v * (60*60)
        if ind == 'm':
            history_in_seconds += v * 60

    records_number = int(history_in_seconds/2)

    if random_data is True:
        records = _get_random_data(records_number)
    else:
        records = _get_cluster_io_data(nodes, records_number)

    # combine records
    rec_count = records_count
    data_per_record = int(len(records)/rec_count)
    new_records = []
    for i in range(rec_count):
        first_index = i*data_per_record
        last_index = (i+1)*data_per_record
        last_index = min(last_index, len(records))
        sl = records[first_index:last_index]
        rec = utils.dict_agg(sl, mean=True)
        new_records.append(rec)

    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read speed": utils.humanbytes(record['read_bytes_per_sec']),
            "Read IOPS": record["read_iops"],
            "Write speed":  utils.humanbytes(record["write_bytes_per_sec"]),
            "Write IOPS": record["write_iops"],
            # "bytes_unmapped (MB/s)": record["unmapped_bytes_per_sec"],
            "Read lat": record["read_latency_ticks"],
            "Write lat": record["write_latency_ticks"],
        })
    return out

