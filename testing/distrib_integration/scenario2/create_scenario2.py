import argparse
import json
import sys

sys.path.append('/root/spdk/python/')
from spdk import rpc


def parse_opts(argv):
    parser = argparse.ArgumentParser(description='Automate distrib bdev creating from a config file for scenario 1')

    parser.add_argument('-c', '--config-file', dest="config_file",
                        help="Configuration file contains alcmel and distrib parameters)",
                        required=True)
    parser.add_argument('-n', '--nodes_json', dest="nodes",
                        help="JSON file contains storage nodes data",
                        required=True)
    parser.add_argument('-vuid', '--vuid', dest="vuid", type=int,
                        help="vuid for the distrib bdev",
                        required=True)
    return parser.parse_args(argv[1:])


def main(argv=sys.argv):
    opts = parse_opts(argv)
    with open(opts.config_file, 'r') as file:
        json_data = json.load(file)
    with open(opts.nodes, 'r') as file:
        nodes = json.load(file)
    nbblock_distrib = json_data.get('nbblock_distrib', 4096)
    nbchunk_distrib = json_data.get('nbchunk_distrib', 4096)
    ndcs = json_data.get('ndcs', 4)
    npcs = json_data.get('npcs', 0)
    page_size = json_data.get('page_size', 2097152)
    node = nodes["main_node"]["node_name"]
    node2 = nodes["remote1_node"]["node_name"]
    node3 = nodes["remote2_node"]["node_name"]
    node_ip = nodes["main_node"]["node_ip"]
    node2_ip = nodes["remote1_node"]["node_ip"]
    node3_ip = nodes["remote2_node"]["node_ip"]
    node_id = nodes["main_node"]["node_uuid"]
    node2_id = nodes["remote1_node"]["node_uuid"]
    node3_id = nodes["remote2_node"]["node_uuid"]
    distrib_nqn = f"nqn.2023-02.io.simlpyblock:subsystem-{nodes['main_node']['node_name']}"
    controller_name = "nvme_1f"
    bdev_name = f"{controller_name}n1"
    sub_bdev1 = f"{bdev_name}p0"
    sub_bdev2 = f"{bdev_name}p1"
    alcmel1_1_name = f"{node}_alceml_{sub_bdev1}"
    alcmel1_2_name = f"{node}_alceml_{sub_bdev2}"
    remote2_1_name = f"remote_{node2}_alceml_{sub_bdev1}"
    remote2_2_name = f"remote_{node2}_alceml_{sub_bdev2}"
    remote3_1_name = f"remote_{node3}_alceml_{sub_bdev1}"
    remote3_2_name = f"remote_{node3}_alceml_{sub_bdev2}"
    alcmel2_1_name = f"{remote2_1_name}n1"
    alcmel2_2_name = f"{remote2_2_name}n1"
    alcmel3_1_name = f"{remote3_1_name}n1"
    alcmel3_2_name = f"{remote3_2_name}n1"
    alcmel1_1_uuid = nodes["main_node"]["alcmel1_1_uuid"]
    alcmel1_2_uuid = nodes["main_node"]["alcmel1_2_uuid"]
    alcmel2_1_uuid = nodes["remote1_node"]["alcmel1_1_uuid"]
    alcmel2_2_uuid = nodes["remote1_node"]["alcmel1_2_uuid"]
    alcmel3_1_uuid = nodes["remote2_node"]["alcmel1_1_uuid"]
    alcmel3_2_uuid = nodes["remote2_node"]["alcmel1_2_uuid"]

    distrib_alloc_names = f"{alcmel1_1_name},{alcmel1_2_name},{alcmel2_1_name},{alcmel2_2_name},{alcmel3_1_name},{alcmel3_2_name}"
    passthru1_1_name = f"{alcmel1_1_name}_PT"
    passthru1_2_name = f"{alcmel1_2_name}_PT"
    passthru2_1_name = f"{node2}_alceml_{sub_bdev1}_PT"
    passthru2_2_name = f"{node2}_alceml_{sub_bdev2}_PT"
    passthru3_1_name = f"{node3}_alceml_{sub_bdev1}_PT"
    passthru3_2_name = f"{node3}_alceml_{sub_bdev2}_PT"
    ngn_passthru1_1 = f"nqn.2023-02.io.simlpyblock:ip-{node}-{passthru1_1_name}"
    ngn_passthru1_2 = f"nqn.2023-02.io.simlpyblock:ip-{node}-{passthru1_2_name}"
    node2_1_nqn = f"nqn.2023-02.io.simlpyblock:ip-{node2}-{passthru2_1_name}"
    node2_2_nqn = f"nqn.2023-02.io.simlpyblock:ip-{node2}-{passthru2_2_name}"
    node3_1_nqn = f"nqn.2023-02.io.simlpyblock:ip-{node3}-{passthru3_1_name}"
    node3_2_nqn = f"nqn.2023-02.io.simlpyblock:ip-{node3}-{passthru3_2_name}"
    functions1 = [
        {
            "method": "bdev_nvme_attach_controller",
            "params": {
                'name': controller_name,
                'trtype': "pcie",
                'traddr': "0000:00:1f.0",
            }
        },
        {
            "method": "bdev_split_create",
            "params": {
                "base_bdev": bdev_name,
                "split_count": 2
            }
        },
        {
            "method": "bdev_alceml_create",
            "params": {
                'name': alcmel1_1_name,
                "cntr_path": sub_bdev1,
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": alcmel1_1_uuid
            }
        },
        {
            "method": "bdev_ptnonexcl_create",
            "params": {
                "name": passthru1_1_name,
                'base_bdev_name': alcmel1_1_name
            }
        },
        {
            "method": "nvmf_create_subsystem",
            "params": {
                "nqn": ngn_passthru1_1,
                "serial_number": "AWS434A7649C61813FCF",
                "model_number": "Amazon EC2 NVMe Instance Storage",
                "allow_any_host": True
            }
        },
        {
            "method": "nvmf_create_transport",
            "params": {
                "trtype": "TCP"
            }
        },
        {
            "method": "nvmf_subsystem_add_listener",
            "params": {
                "nqn": ngn_passthru1_1,
                "listen_address": {
                    "trtype": "tcp",
                    "adrfam": "ipv4",
                    "traddr": node_ip,
                    "trsvcid": "4420"
                }
            }
        },
        {
            "method": "nvmf_subsystem_add_ns",
            "params": {
                "nqn": ngn_passthru1_1,
                "namespace": {
                    "bdev_name": passthru1_1_name
                }
            }
        },
        {
            "method": "bdev_alceml_create",
            "params": {
                'name': alcmel1_2_name,
                "cntr_path": sub_bdev2,
                "num_blocks": 0,
                "block_size": 0,
                "num_blocks_reported": 0,
                "md_size": 0,
                "use_ram": False,
                "pba_init_mode": 3,
                "pba_page_size": page_size,
                "uuid": alcmel1_2_uuid
            }
        },
        {
            "method": "bdev_ptnonexcl_create",
            "params": {
                "name": passthru1_2_name,
                'base_bdev_name': alcmel1_2_name
            }
        },
        {
            "method": "nvmf_create_subsystem",
            "params": {
                "nqn": ngn_passthru1_2,
                "serial_number": "AWS434A7649C61813FCF",
                "model_number": "Amazon EC2 NVMe Instance Storage",
                "allow_any_host": True
            }
        },
        {
            "method": "nvmf_subsystem_add_listener",
            "params": {
                "nqn": ngn_passthru1_2,
                "listen_address": {
                    "trtype": "tcp",
                    "adrfam": "ipv4",
                    "traddr": node_ip,
                    "trsvcid": "4420"
                }
            }
        },
        {
            "method": "nvmf_subsystem_add_ns",
            "params": {
                "nqn": ngn_passthru1_2,
                "namespace": {
                    "bdev_name": passthru1_2_name
                }
            }
        }
    ]
    functions2 = [
        {
            "method": "bdev_nvme_attach_controller",
            "params": {
                'name': remote2_1_name,
                'trtype': 'tcp',
                'traddr': node2_ip,
                'adrfam': 'ipv4',
                'trsvcid': '4420',
                'subnqn': node2_1_nqn
            }
        },
        {
            "method": "bdev_nvme_attach_controller",
            "params": {
                'name': remote2_2_name,
                'trtype': 'tcp',
                'traddr': node2_ip,
                'adrfam': 'ipv4',
                'trsvcid': '4420',
                'subnqn': node2_2_nqn
            }
        },
        {
            "method": "bdev_nvme_attach_controller",
            "params": {
                'name': remote3_1_name,
                'trtype': 'tcp',
                'traddr': node3_ip,
                'adrfam': 'ipv4',
                'trsvcid': '4420',
                'subnqn': node3_1_nqn
            }
        },
        {
            "method": "bdev_nvme_attach_controller",
            "params": {
                'name': remote3_2_name,
                'trtype': 'tcp',
                'traddr': node3_ip,
                'adrfam': 'ipv4',
                'trsvcid': '4420',
                'subnqn': node3_2_nqn
            }
        },
        {
            "method": "distr_send_cluster_map",
            "params": {
                'name': '', 'UUID_node_target': node_id,
                'timestamp': '2023-09-27T05:20:27Z',
                'map_cluster': {
                    node_id: {
                        'availability_group': 0,
                        'status': 'online',
                        'devices': {
                            '0': {
                                'UUID': alcmel1_1_uuid,
                                'bdev_name': alcmel1_1_name,
                                'status': 'online'
                            },
                            '1': {
                                'UUID': alcmel1_2_uuid,
                                'bdev_name': alcmel1_2_name,
                                'status': 'online'
                            }
                        }
                    },
                    node2_id: {
                        'availability_group': 0,
                        'status': 'online',
                        'devices': {
                            '0': {
                                'UUID': alcmel2_1_uuid,
                                'bdev_name': alcmel2_1_name,
                                'status': 'online'
                            },
                            '1': {
                                'UUID': alcmel2_2_uuid,
                                'bdev_name': alcmel2_2_name,
                                'status': 'online'
                            }
                        }
                    },
                    node3_id: {
                        'availability_group': 0,
                        'status': 'online',
                        'devices': {
                            '0': {
                                'UUID': alcmel3_1_uuid,
                                'bdev_name': alcmel3_1_name,
                                'status': 'online'
                            },
                            '1': {
                                'UUID': alcmel3_2_uuid,
                                'bdev_name': alcmel3_2_name,
                                'status': 'online'
                            }
                        }
                    }
                },
                'map_prob': [{'weight': 2328, 'items': [{'weight': 1164, 'id': 0}, {'weight': 1164, 'id': 1}]}]}
        }

    ]
    functions3 = [
        {
            "method": "bdev_distrib_create",
            "params": {
                "name": "distrib_1",
                "alloc_names": distrib_alloc_names,
                "vuid": opts.vuid,
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
                "nqn": distrib_nqn,
                "serial_number": "AWS434A7649C61813FCF",
                "model_number": "Amazon EC2 NVMe Instance Storage",
                "allow_any_host": True
            }
        },
        {
            "method": "nvmf_subsystem_add_listener",
            "params": {
                "nqn": distrib_nqn,
                "listen_address": {
                    "trtype": "tcp",
                    "adrfam": "ipv4",
                    "traddr": node_ip,
                    "trsvcid": "4420"
                }
            }
        },
        {
            "method": "nvmf_subsystem_add_ns",
            "params": {
                "nqn": distrib_nqn,
                "namespace": {
                    "bdev_name": "distrib_1"
                }
            }
        }
    ]
    try:
        client = rpc.client.JSONRPCClient('/var/tmp/spdk.sock', 5260)
    except Exception as e:
            print(e)
    for func in functions1:
        try:
            print("Method: %s" % func["method"])
            print("Params: %s" % func["params"])
            ret = client.call(func["method"], func["params"])
            print("Response: %s" % ret)
        except Exception as e:
            print(e)
    input("create other storage nodes and then add remote alcmel")
    for func in functions2:
        try:
            print("Method: %s" % func["method"])
            print("Params: %s" % func["params"])
            ret = client.call(func["method"], func["params"])
            print("Response: %s" % ret)
        except Exception as e:
            print(e)
    input("create other alcmels and then create distrib1 bdev")
    for func in functions3:
        try:
            print("Method: %s" % func["method"])
            print("Params: %s" % func["params"])
            ret = client.call(func["method"], func["params"])
            print("Response: %s" % ret)
        except Exception as e:
            print(e)
    print(f"sudo nvme connect --transport=tcp --traddr={node_ip} --trsvcid=4420 --nqn={distrib_nqn}")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
