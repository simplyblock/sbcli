# coding=utf-8
"""Provision the edge-clusters e2e environment on AWS (boto3).

Creates one VPC with a public subnet, then:
- central k3s cluster: 1 mgmt/server node + CENTRAL.workers agents with
  storage EBS volumes (hosts the CP and the 3-node hyperscale cluster),
- one k3s cluster per EDGE_CLUSTERS entry (server [+ agent] with the data
  EBS volumes from the drive matrix).

k3s installs via cloud-init user-data (server first, agents join with the
shared token over the private subnet). All instance/volume state lands in
STATE_FILE for deploy.py / the test suite; --destroy tears everything down
by tag.

Usage:
    python edge_e2e/provision.py --region eu-west-1 --key-name mykey
    python edge_e2e/provision.py --region eu-west-1 --destroy

Requires: boto3, an SSH key pair already registered in the region.
"""
import argparse
import json
import os
import pathlib
import secrets
import sys
import time

import boto3

# Allow running as a script (`python edge_e2e/x.py`) as well as `-m`:
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from edge_e2e.topology import CENTRAL, EDGE_CLUSTERS

TAG_KEY = "simplyblock-edge-e2e"
# Root volume size (GiB). Must hold the whole control-plane image set.
ROOT_DISK_GB = int(os.getenv("EDGE_E2E_ROOT_DISK_GB", "80"))
STATE_FILE = pathlib.Path(__file__).parent / "state.json"

UBUNTU_AMI_PARAM = ("/aws/service/canonical/ubuntu/server/22.04/stable/"
                    "current/amd64/hvm/ebs-gp2/ami-id")

# NB: sgdisk ships INSIDE the `gdisk` package. Naming it separately makes apt
# fail, and with `set -e` that aborted cloud-init before k3s installed — on
# every instance of the first real run (2026-08-10). apt/k3s fetches are
# retried because a freshly booted instance often races DNS/network.
_PREAMBLE = """#!/bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
for i in $(seq 1 12); do apt-get update -y && break || sleep 10; done
for i in $(seq 1 12); do
  apt-get install -y curl nvme-cli fio gdisk jq && break || sleep 10
done
"""

K3S_SERVER_USERDATA = _PREAMBLE + """
for i in $(seq 1 10); do
  curl -sfL https://get.k3s.io | K3S_TOKEN={token} sh -s - server \\
    --write-kubeconfig-mode 644 --disable traefik --node-name {node_name} \\
  && break || sleep 15
done
"""

K3S_AGENT_USERDATA = _PREAMBLE + """
until curl -sk https://{server_ip}:6443 >/dev/null 2>&1; do sleep 5; done
for i in $(seq 1 10); do
  curl -sfL https://get.k3s.io | K3S_URL=https://{server_ip}:6443 \\
    K3S_TOKEN={token} sh -s - agent --node-name {node_name} \\
  && break || sleep 15
done
"""


def _clients(region):
    session = boto3.session.Session(region_name=region)
    return session.client("ec2"), session.client("ssm")


def _latest_ubuntu_ami(ssm):
    return ssm.get_parameter(Name=UBUNTU_AMI_PARAM)["Parameter"]["Value"]


def _pick_availability_zone(ec2, instance_types) -> str:
    """An AZ that offers EVERY instance type this run needs.

    Creating the subnet without an AZ lets AWS pick, and it picked us-east-1e
    — which does not offer m5.xlarge, so RunInstances failed with
    "Unsupported ... in your requested Availability Zone".
    """
    zones = None
    for instance_type in sorted(set(instance_types)):
        offerings = ec2.describe_instance_type_offerings(
            LocationType="availability-zone",
            Filters=[{"Name": "instance-type", "Values": [instance_type]}],
        )["InstanceTypeOfferings"]
        supported = {o["Location"] for o in offerings}
        zones = supported if zones is None else (zones & supported)
    if not zones:
        raise RuntimeError(
            f"no availability zone offers all of {sorted(set(instance_types))}")
    return sorted(zones)[0]


def _ensure_network(ec2, run_id, availability_zone):
    vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16",
                         TagSpecifications=_tags("vpc", run_id, "edge-e2e-vpc"))["Vpc"]
    ec2.modify_vpc_attribute(VpcId=vpc["VpcId"], EnableDnsSupport={"Value": True})
    ec2.modify_vpc_attribute(VpcId=vpc["VpcId"], EnableDnsHostnames={"Value": True})
    igw = ec2.create_internet_gateway(
        TagSpecifications=_tags("internet-gateway", run_id, "edge-e2e-igw"))["InternetGateway"]
    ec2.attach_internet_gateway(InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc["VpcId"])
    subnet = ec2.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.90.1.0/24",
                               AvailabilityZone=availability_zone,
                               TagSpecifications=_tags("subnet", run_id, "edge-e2e-subnet"))["Subnet"]
    ec2.modify_subnet_attribute(SubnetId=subnet["SubnetId"],
                                MapPublicIpOnLaunch={"Value": True})
    route_tables = ec2.describe_route_tables(
        Filters=[{"Name": "vpc-id", "Values": [vpc["VpcId"]]}])["RouteTables"]
    ec2.create_route(RouteTableId=route_tables[0]["RouteTableId"],
                     DestinationCidrBlock="0.0.0.0/0",
                     GatewayId=igw["InternetGatewayId"])
    sg = ec2.create_security_group(
        GroupName=f"edge-e2e-{run_id}", Description="simplyblock edge e2e",
        VpcId=vpc["VpcId"], TagSpecifications=_tags("security-group", run_id, "edge-e2e-sg"))
    ec2.authorize_security_group_ingress(GroupId=sg["GroupId"], IpPermissions=[
        {"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": sg["GroupId"]}]},
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
         "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        {"IpProtocol": "tcp", "FromPort": 6443, "ToPort": 6443,
         "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        # The management API (and the edge campaign's API client) reach the
        # control plane over the ingress on 80/443.
        {"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
         "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
         "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
    ])
    return {"vpc": vpc["VpcId"], "subnet": subnet["SubnetId"], "sg": sg["GroupId"],
            "igw": igw["InternetGatewayId"], "availability_zone": availability_zone}


def _tags(resource_type, run_id, name):
    return [{"ResourceType": resource_type,
             "Tags": [{"Key": TAG_KEY, "Value": run_id}, {"Key": "Name", "Value": name}]}]


def _block_devices(drives, root_device_name):
    # The AMI's default root volume is 8 GiB, which the control-plane install
    # exhausts on image pulls alone (FDB, CSI, minio, admin-control, SPDK):
    # run-1786464991 hit 87% used with ~1 GiB free and the kubelet evicted
    # FDB and admin-control pods. Size the root volume explicitly.
    mappings = [{
        "DeviceName": root_device_name,
        "Ebs": {"VolumeSize": ROOT_DISK_GB, "VolumeType": "gp3",
                "DeleteOnTermination": True},
    }]
    for index, drive in enumerate(drives):
        mappings.append({
            # /dev/sdf.. maps to /dev/nvme{index+1}n1 on nitro
            "DeviceName": f"/dev/sd{chr(ord('f') + index)}",
            "Ebs": {"VolumeSize": drive.size_gb, "VolumeType": "gp3",
                    "DeleteOnTermination": True},
        })
    return mappings


def _root_device_name(ec2, ami) -> str:
    return ec2.describe_images(ImageIds=[ami])["Images"][0].get(
        "RootDeviceName", "/dev/sda1")


def _run_instance(ec2, *, ami, itype, key_name, subnet, sg, name, run_id,
                  user_data, drives=(), root_device_name="/dev/sda1"):
    result = ec2.run_instances(
        ImageId=ami, InstanceType=itype, KeyName=key_name, MinCount=1, MaxCount=1,
        NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": subnet, "Groups": [sg],
                            "AssociatePublicIpAddress": True}],
        BlockDeviceMappings=_block_devices(drives, root_device_name),
        UserData=user_data,
        TagSpecifications=_tags("instance", run_id, name),
    )
    return result["Instances"][0]["InstanceId"]


def _wait_running(ec2, instance_ids):
    ec2.get_waiter("instance_running").wait(InstanceIds=instance_ids)
    described = ec2.describe_instances(InstanceIds=instance_ids)
    info = {}
    for reservation in described["Reservations"]:
        for instance in reservation["Instances"]:
            name = next(t["Value"] for t in instance["Tags"] if t["Key"] == "Name")
            volumes = [m["Ebs"]["VolumeId"] for m in instance["BlockDeviceMappings"]
                       if not m["DeviceName"].endswith("a1") and m["DeviceName"] != instance["RootDeviceName"]]
            info[name] = {
                "instance_id": instance["InstanceId"],
                "private_ip": instance["PrivateIpAddress"],
                "public_ip": instance.get("PublicIpAddress", ""),
                "data_volumes": volumes,
            }
    return info


def provision(region, key_name):
    ec2, ssm = _clients(region)
    ami = _latest_ubuntu_ami(ssm)
    root_device = _root_device_name(ec2, ami)
    run_id = f"run-{int(time.time())}"
    needed_types = [CENTRAL.mgmt_instance_type, CENTRAL.instance_type,
                    *(spec.instance_type for spec in EDGE_CLUSTERS)]
    zone = _pick_availability_zone(ec2, needed_types)
    print(f"Using availability zone {zone} for {sorted(set(needed_types))}")
    net = _ensure_network(ec2, run_id, zone)

    state = {"region": region, "run_id": run_id, "key_name": key_name,
             "network": net, "central": {}, "edge": {}}
    instance_ids = []

    # --- central: server (mgmt) + workers ------------------------------------
    central_token = secrets.token_hex(16)
    server_name = f"{CENTRAL.name}-mgmt"
    server_id = _run_instance(
        ec2, ami=ami, itype=CENTRAL.mgmt_instance_type, key_name=key_name,
        subnet=net["subnet"], sg=net["sg"], name=server_name, run_id=run_id,
        user_data=K3S_SERVER_USERDATA.format(token=central_token, node_name=server_name),
        root_device_name=root_device)
    instance_ids.append(server_id)
    server_ip = ec2.describe_instances(InstanceIds=[server_id])[
        "Reservations"][0]["Instances"][0]["PrivateIpAddress"]

    worker_names = []
    for w in range(CENTRAL.workers):
        name = f"{CENTRAL.name}-worker-{w + 1}"
        worker_names.append(name)
        instance_ids.append(_run_instance(
            ec2, ami=ami, itype=CENTRAL.instance_type, key_name=key_name,
            subnet=net["subnet"], sg=net["sg"], name=name, run_id=run_id,
            user_data=K3S_AGENT_USERDATA.format(server_ip=server_ip,
                                                token=central_token, node_name=name),
            drives=CENTRAL.storage_drives, root_device_name=root_device))
    state["central"] = {"token": central_token, "server": server_name,
                        "workers": worker_names}

    # --- edge clusters --------------------------------------------------------
    for spec in EDGE_CLUSTERS:
        token = secrets.token_hex(16)
        server_name = f"{spec.name}-n1"
        server_id = _run_instance(
            ec2, ami=ami, itype=spec.instance_type, key_name=key_name,
            subnet=net["subnet"], sg=net["sg"], name=server_name, run_id=run_id,
            user_data=K3S_SERVER_USERDATA.format(token=token, node_name=server_name),
            drives=spec.drives, root_device_name=root_device)
        instance_ids.append(server_id)
        node_names = [server_name]
        if spec.nodes == 2:
            server_ip = ec2.describe_instances(InstanceIds=[server_id])[
                "Reservations"][0]["Instances"][0]["PrivateIpAddress"]
            agent_name = f"{spec.name}-n2"
            node_names.append(agent_name)
            instance_ids.append(_run_instance(
                ec2, ami=ami, itype=spec.instance_type, key_name=key_name,
                subnet=net["subnet"], sg=net["sg"], name=agent_name, run_id=run_id,
                user_data=K3S_AGENT_USERDATA.format(server_ip=server_ip, token=token,
                                                    node_name=agent_name),
                drives=spec.drives, root_device_name=root_device))
        state["edge"][spec.name] = {"token": token, "nodes": node_names,
                                    "device_paths": spec.device_paths,
                                    "node_count": spec.nodes}

    print(f"Waiting for {len(instance_ids)} instances to run...")
    info = _wait_running(ec2, instance_ids)
    state["instances"] = info
    STATE_FILE.write_text(json.dumps(state, indent=2))
    print(f"State written to {STATE_FILE}")
    print("Give cloud-init ~3-5 minutes to finish the k3s installs, "
          "then run: python edge_e2e/deploy.py")


def destroy(region):
    ec2, _ = _clients(region)
    if not STATE_FILE.exists():
        print("No state file; nothing to destroy by state — sweeping by tag.")
        run_filter = [{"Name": "tag-key", "Values": [TAG_KEY]}]
    else:
        run_id = json.loads(STATE_FILE.read_text())["run_id"]
        run_filter = [{"Name": f"tag:{TAG_KEY}", "Values": [run_id]}]

    reservations = ec2.describe_instances(Filters=run_filter)["Reservations"]
    ids = [i["InstanceId"] for r in reservations for i in r["Instances"]
           if i["State"]["Name"] not in ("terminated", "shutting-down")]
    if ids:
        print(f"Terminating {len(ids)} instances...")
        ec2.terminate_instances(InstanceIds=ids)
        ec2.get_waiter("instance_terminated").wait(InstanceIds=ids)
    for sg in ec2.describe_security_groups(Filters=run_filter)["SecurityGroups"]:
        ec2.delete_security_group(GroupId=sg["GroupId"])
    for subnet in ec2.describe_subnets(Filters=run_filter)["Subnets"]:
        ec2.delete_subnet(SubnetId=subnet["SubnetId"])
    for igw in ec2.describe_internet_gateways(Filters=run_filter)["InternetGateways"]:
        for attachment in igw["Attachments"]:
            ec2.detach_internet_gateway(InternetGatewayId=igw["InternetGatewayId"],
                                        VpcId=attachment["VpcId"])
        ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])
    for vpc in ec2.describe_vpcs(Filters=run_filter)["Vpcs"]:
        ec2.delete_vpc(VpcId=vpc["VpcId"])
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    print("Destroyed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--key-name", help="EC2 key pair name (required to provision)")
    parser.add_argument("--destroy", action="store_true")
    args = parser.parse_args()
    if args.destroy:
        destroy(args.region)
        return
    if not args.key_name:
        sys.exit("--key-name is required to provision")
    provision(args.region, args.key_name)


if __name__ == "__main__":
    main()
