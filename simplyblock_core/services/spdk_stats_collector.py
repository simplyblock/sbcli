import datetime
import time

from simplyblock_core.services.spdk import client as spdk_client


method = "thread_get_stats"
spdk_sock_path = '/var/tmp/spdk.sock'
csv_file_path = "/etc/simplyblock/spdk_top.csv"

def get_file(ret):

    f = open(csv_file_path, 'at', buffering=1)
    f.write("date, ")
    for stats in ret["threads"]:
        f.write(", ".join([k for k in stats]))
    f.write("\n")
    return f


if __name__ == '__main__':

    client = spdk_client.JSONRPCClient(spdk_sock_path, 5260)

    out_file = None
    while True:
        ret = client.call(method)
        now = str(datetime.datetime.now()).split(".")[0]
        if ret and "threads" in ret:
            if not out_file:
                out_file = get_file(ret)
            out_file.write(now+", ")
            for stats in ret["threads"]:
                out_file.write(", ".join([str(k) for k in stats.values()]))
            out_file.write("\n")

        time.sleep(10)
