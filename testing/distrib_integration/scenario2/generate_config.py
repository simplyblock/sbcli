import argparse
import json
import sys
import uuid


def parse_opts(argv):
    parser = argparse.ArgumentParser(description='Automate distrib bdev creating from a config file for scenario 1')

    parser.add_argument('-ip1', dest="ip1", action='store',
                        help="IP address of the first storage node.", required=True)
    parser.add_argument('-ip2', dest="ip2", action='store',
                        help="IP address of the second storage node.", required=True)
    parser.add_argument('-ip3', dest="ip3", action='store',
                        help="IP address of the third storage node.", required=True)
    return parser.parse_args(argv[1:])


class StorageNode:
    def __init__(self, ip_address):
        self.ip_address = ip_address
        self.uuid = str(uuid.uuid4())
        self.alcmel1_uuid = str(uuid.uuid4())
        self.alcmel2_uuid = str(uuid.uuid4())
        self.name = str(self.ip_address).replace('.', '-')


def generate_config(main_node, remote1, remote2):
    config = {
        "main_node": {
            "node_name": main_node.name,
            "node_uuid": main_node.uuid,
            "node_ip": main_node.ip_address,
            "alcmel1_1_uuid": main_node.alcmel1_uuid,
            "alcmel1_2_uuid": main_node.alcmel2_uuid
        },
        "remote1_node": {
            "node_name": remote1.name,
            "node_uuid": remote1.uuid,
            "node_ip": remote1.ip_address,
            "alcmel1_1_uuid": remote1.alcmel1_uuid,
            "alcmel1_2_uuid": remote1.alcmel2_uuid
        },
        "remote2_node": {
            "node_name": remote2.name,
            "node_uuid": remote2.uuid,
            "node_ip": remote2.ip_address,
            "alcmel1_1_uuid": remote2.alcmel1_uuid,
            "alcmel1_2_uuid": remote2.alcmel2_uuid
        }
    }
    return config


def write_json_to_file(json_data, json_file):
    try:
        # Write JSON data to the file
        with open(json_file, 'w') as file:
            json.dump(json_data, file, indent=4)
        return True

    except Exception as e:
        print(f"Error: {str(e)}")
        return False


def main(argv=sys.argv):
    opts = parse_opts(argv)
    node1 = StorageNode(opts.ip1)
    node2 = StorageNode(opts.ip2)
    node3 = StorageNode(opts.ip3)
    config1_json = generate_config(node1, node2, node3)
    config2_json = generate_config(node2, node3, node1)
    config3_json = generate_config(node3, node1, node2)
    write_json_to_file(config1_json, 'config1.json')
    write_json_to_file(config2_json, 'config2.json')
    write_json_to_file(config3_json, 'config3.json')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
