import argparse
import json
import sys

sys.path.append('/root/spdk/python/')
from spdk import rpc


def parse_opts(argv):
    parser = argparse.ArgumentParser(description='Automate distrib bdev creating from a config file for scenario 1')

    parser.add_argument('-c', '--config-file', dest="config_file", action='store',
                        help="Configuration file containing a list of input configurations (start-index end-index operation)",
                        required=True)
    parser.add_argument('-ip', dest="ip", action='store',
                        help="Print debugging output.", required=True)
    parser.add_argument('-p', '--pause', dest="pause", action='store_true', default=False,
                        help="Print debugging output.", required=False)
    return parser.parse_args(argv[1:])


def main(argv=sys.argv):
    opts = parse_opts(argv)
    with open(opts.config_file, 'r') as file:
        json_data = json.load(file)
    nbblock_distrib = json_data.get('nbblock_distrib', 4096)
    nbchunk_distrib = json_data.get('nbchunk_distrib', 4096)
    ndcs = json_data.get('ndcs', 4)
    npcs = json_data.get('npcs', 0)
    page_size = json_data.get('page_size', 4194304)
    functions = [
        {
            "method": "bdev_nvme_attach_controller",
            "params": {
                'name': 'nvme_1f',
                'trtype': "pcie",
                'traddr': "0000:00:1f.0",
            }
        },
        {
            "method": "bdev_split_create",
            "params": {
                "base_bdev": "nvme_1fn1",
                "split_count": 5
            }
        },
        {
            "method": "bdev_alceml_create",
            "params": {
                'name': 'alceml_nvme_1ef1p0',
                "cntr_path": "nvme_1fn1p0",
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": "e272326d-16e3-4b83-abd0-8e56c96bbb90"
            }
        }, {
            "method": "bdev_alceml_create",
            "params": {
                'name': 'alceml_nvme_1ef1p1',
                "cntr_path": "nvme_1fn1p1",
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": "e272326d-16e3-4b83-abd0-8e56c96bbb91"
            }
        },
        {
            "method": "bdev_alceml_create",
            "params": {
                'name': 'alceml_nvme_1ef1p2',
                "cntr_path": "nvme_1fn1p2",
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": "e272326d-16e3-4b83-abd0-8e56c96bbb92"
            }
        },
        {
            "method": "bdev_alceml_create",
            "params": {
                'name': 'alceml_nvme_1ef1p3',
                "cntr_path": "nvme_1fn1p3",
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": "e272326d-16e3-4b83-abd0-8e56c96bbb93"
            }
        },
        {
            "method": "bdev_alceml_create",
            "params": {
                'name': 'alceml_nvme_1ef1p4',
                "cntr_path": "nvme_1fn1p4",
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": "e272326d-16e3-4b83-abd0-8e56c96bbb94"
            }
        },
        {
            "method": "bdev_distrib_create",
            "params": {
                "name": "distrib_1",
                "alloc_names": "alceml_nvme_1ef1p0,alceml_nvme_1ef1p1,alceml_nvme_1ef1p2,alceml_nvme_1ef1p3,alceml_nvme_1ef1p4",
                "vuid": 1,
                "ndcs": ndcs,
                "npcs": npcs,
                "num_blocks": 51200000,
                "block_size": nbblock_distrib,
                "chunk_size": nbchunk_distrib,
                "pba_page_size": page_size
            }
        },
        {
            "method": "nvmf_create_subsystem",
            "params": {
                "nqn": "nqn.2023-02.io.simlpyblock:subsystem",
                "serial_number": "AWS434A7649C61813FCF",
                "model_number": "Amazon EC2 NVMe Instance Storage",
                "allow_any_host": True
            }},
        {
            "method": "nvmf_create_transport",
            "params": {
                "trtype": "TCP"
            }
        },
        {
            "method": "nvmf_subsystem_add_listener",
            "params": {
                "nqn": "nqn.2023-02.io.simlpyblock:subsystem",
                "listen_address": {
                    "trtype": "tcp",
                    "adrfam": "ipv4",
                    "traddr": opts.ip,
                    "trsvcid": "4420"
                }
            }
        },
        {
            "method": "nvmf_subsystem_add_ns",
            "params": {
                "nqn": "nqn.2023-02.io.simlpyblock:subsystem",
                "namespace": {
                    "bdev_name": "distrib_1"
                }
            }
        }
    ]
    for func in functions:
        try:
            if opts.pause:
                input("press any ket to continue")
            client = rpc.client.JSONRPCClient('/var/tmp/spdk.sock', 5260)
            print("Method: %s" % func["method"])
            print("Params: %s" % func["params"])
            ret = client.call(func["method"], func["params"])
            print("Response: %s" % ret)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
