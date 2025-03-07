import datetime
import time

import docker

from simplyblock_core import utils
from simplyblock_core.services.spdk import client as spdk_client


method = "thread_get_stats"
logger = utils.get_logger(__name__)


def get_file(ret, path):
    logger.info(f"getting file {path}")
    f = open(path, 'at', buffering=1)
    f.write("date, ")
    for stats in ret["threads"]:
        f.write(", ".join([k for k in stats]))
    f.write("\n")
    return f


def get_docker_client():
    return docker.from_env()

if __name__ == '__main__':

    node_docker = get_docker_client()

    while True:
        try:
            for cont in node_docker.containers.list(all=True):
                logger.info(cont.attrs['Name'])
                if cont.attrs['Name'].startswith(f"/spdk") and not cont.attrs['Name'].startswith("/spdk_proxy") :
                    status = cont.attrs['State']["Status"]
                    is_running = cont.attrs['State']["Running"]
                    logger.info(f"is_running: {is_running}")
                    if is_running:
                        spdk_sock_path = f"/var/tmp{cont.attrs['Name']}/spdk.sock"
                        client = spdk_client.JSONRPCClient(spdk_sock_path, 5260)
                        ret = client.call(method)
                        now = str(datetime.datetime.now()).split(".")[0]
                        if ret and "threads" in ret:
                            out_file = get_file(ret, f"/etc/simplyblock{cont.attrs['Name']}_top.csv")
                            out_file.write(now + ", ")
                            for stats in ret["threads"]:
                                out_file.write(", ".join([str(k) for k in stats.values()]))
                            out_file.write("\n")

        except Exception as e:
            logger.error(e)

        time.sleep(10)

