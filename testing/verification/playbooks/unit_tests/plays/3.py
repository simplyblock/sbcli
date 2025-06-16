import sys

sys.path.append('/root/spdk/python/')
from spdk import rpc


def main():
    functions = [
        {
            "method": "bdev_nvme_set_options",
            "params": {
                "action_on_timeout": "none",
                "timeout_us": 5000,
                "timeout_admin_us": 0,
                "keep_alive_timeout_ms": 500,
                "retry_count": 1,
                "transport_retry_count": 1,
                "bdev_retry_count": 1
            }
        },
{
          "method": "bdev_nvme_attach_controller",
          "params": {
            "name": "nvme_1f",
            "trtype": "PCIe",
            "traddr": "0000:00:1f.0"
          }
        },
        {
          "method": "bdev_split_create",
          "params": {
            "base_bdev": "nvme_1fn1",
            "split_count": 4
          }
        },
        # {
        #   "method": "bdev_ptnonexcl_create",
        #   "params": {
        #     "name": "nvme_1fn1_PT0",
        #     "base_bdev_name": "nvme_1fn1p0"
        #   }
        # },
        # {
        #   "method": "bdev_ptnonexcl_create",
        #   "params": {
        #     "name": "nvme_1fn1_PT1",
        #     "base_bdev_name": "nvme_1fn1p1"
        #   }
        # },
        # {
        #   "method": "bdev_ptnonexcl_create",
        #   "params": {
        #     "name": "nvme_1fn1_PT2",
        #     "base_bdev_name": "nvme_1fn1p2"
        #   }
        # },
        # {
        #   "method": "bdev_ptnonexcl_create",
        #   "params": {
        #     "name": "nvme_1fn1_PT3",
        #     "base_bdev_name": "nvme_1fn1p3"
        #   }
        # },
        {
          "method": "nvmf_create_subsystem",
          "params": {
            "nqn": "nqn.2023-02.io.simlpyblock:subsystem1",
            "serial_number": "AWS434A7649C61813FCF",
            "model_number": "Amazon EC2 NVMe Instance Storage",
            "allow_any_host": True,
            "ana_reporting": False
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
            "nqn": "nqn.2023-02.io.simlpyblock:subsystem1",
            "listen_address": {
              "trtype": "tcp",
              "adrfam": "ipv4",
              "traddr": "172.31.5.249",
              "trsvcid": "4420"
            }
          }
        },
        {
          "method": "nvmf_subsystem_add_ns",
          "params": {
            "nqn": "nqn.2023-02.io.simlpyblock:subsystem1",
            "namespace": {
              "nsid": 1,
              "bdev_name": "nvme_1fn1p0"
            }
          }
        },
                {
          "method": "nvmf_subsystem_add_ns",
          "params": {
            "nqn": "nqn.2023-02.io.simlpyblock:subsystem1",
            "namespace": {
              "nsid": 2,
              "bdev_name": "nvme_1fn1p1"
            }
          }
        },
                {
          "method": "nvmf_subsystem_add_ns",
          "params": {
            "nqn": "nqn.2023-02.io.simlpyblock:subsystem1",
            "namespace": {
              "nsid": 3,
              "bdev_name": "nvme_1fn1p2"
            }
          }
        },
                {
          "method": "nvmf_subsystem_add_ns",
          "params": {
            "nqn": "nqn.2023-02.io.simlpyblock:subsystem1",
            "namespace": {
              "nsid": 4,
              "bdev_name": "nvme_1fn1p3"
            }
          }
        }
]
    functions1 = [
        {
          "method": "bdev_nvme_attach_controller",
          "params": {
            "name": "nvme_1g",
            "trtype": "tcp",
            "traddr": "172.31.14.60",
            "adrfam": "ipv4",
            "trsvcid": "4420",
            "subnqn": "nqn.2023-02.io.simlpyblock:subsystem1"
          }
        },
        {
          "method": "bdev_nvme_attach_controller",
          "params": {
            "name": "nvme_1h",
            "trtype": "tcp",
            "traddr": "172.31.9.62",
            "adrfam": "ipv4",
            "trsvcid": "4420",
            "subnqn": "nqn.2023-02.io.simlpyblock:subsystem1"
          }
        },
        {
          "method": "bdev_nvme_attach_controller",
          "params": {
            "name": "nvme_1i",
            "trtype": "tcp",
            "traddr": "172.31.4.135",
            "adrfam": "ipv4",
            "trsvcid": "4420",
            "subnqn": "nqn.2023-02.io.simlpyblock:subsystem1"
          }
        }
    ]
    try:
        client = rpc.client.JSONRPCClient('/var/tmp/spdk.sock', 5260)
    except Exception as e:
        print(e)
    for func in functions:
        try:
            #input("press any ket to continue")
            print("Method: %s" % func["method"])
            print("Params: %s" % func["params"])
            ret = client.call(func["method"], func["params"])
            print("Response: %s" % ret)
        except Exception as e:
            print(e)
    input("finsin creating bdev on other nodes")
    for func in functions1:
        try:
            #input("press any ket to continue")
            print("Method: %s" % func["method"])
            print("Params: %s" % func["params"])
            ret = client.call(func["method"], func["params"])
            print("Response: %s" % ret)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    sys.exit(main())
