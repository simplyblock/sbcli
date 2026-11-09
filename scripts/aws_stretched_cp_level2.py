#!/usr/bin/env python3
"""Level-2 functional bed for the stretched control plane (docs/stretched-control-plane.md)
on AWS Docker Swarm: two management hosts (sites A and B) on ONE FoundationDB, six
storage nodes (three per site), synchronous replication (scheme a), then the
leadership tests.

What it proves: the lease agent on both sites, services paused on the standby,
307 redirects from the standby's API, the readiness gates and the drain hand-over
of a planned move, and the takeover's liveness probe and lease move. What it
cannot prove: FoundationDB majority behaviour (the swarm's FDB is one cluster)
and anything latency-related - that is the level-3 bed.

Docker Swarm note: the control-plane services are replicated=1 and would run on
ONE manager; the bed scales each of them to one replica per manager
(``--replicas 2 --replicas-max-per-node 1``) so that each site has its own
instance, which is what the lease gate expects. WebAppAPI is already global.

Usage:
  python aws_stretched_cp_level2.py deploy    # launch + install + cluster + tests
  python aws_stretched_cp_level2.py test      # tests only (metadata from a deploy)
  python aws_stretched_cp_level2.py collect   # pull logs + status into the report dir
  python aws_stretched_cp_level2.py teardown  # terminate the instances
  python aws_stretched_cp_level2.py all       # deploy, test, collect, teardown

Env overrides: SB_CP_IMAGE, SB_BRANCH, SB_KEEP=1 (all: skip the teardown),
SB_SN_TYPE, SB_MGMT_TYPE, SB_REPORT_DIR.
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import paramiko

# --- lab constants (us-east-1 test account) ----------------------------------
AMI_ID = os.environ.get("SB_AMI_ID", "ami-0dfc569a8686b9320")
KEY_NAME = os.environ.get("SB_KEY_NAME", "mtes01")
KEY_PATH = os.path.expanduser(os.environ.get("SB_KEY_PATH", "~/.ssh/mtes01.pem"))
SUBNET_ID = os.environ.get("SB_SUBNET_ID", "subnet-0593459d6b931ee4c")
SG_ID = os.environ.get("SB_SG_ID", "sg-02e89a1372e9f39e9")
REGION = os.environ.get("SB_REGION", "us-east-1")
BRANCH = os.environ.get("SB_BRANCH", "feat/stretch-control-plane")
USER = "ec2-user"
IFACE = "eth0"
MAX_SUBSYS = "75"
MGMT_TYPE = os.environ.get("SB_MGMT_TYPE", "m6i.xlarge")
SN_TYPE = os.environ.get("SB_SN_TYPE", "m6id.2xlarge")
SITES = ("A", "B")
SN_PER_SITE = 3
ECR_REPO = "public.ecr.aws/simply-block/simplyblock"
HERE = Path(__file__).parent
META_PATH = HERE / "cluster_metadata_stretch_l2.json"
REPORT_DIR = Path(os.environ.get("SB_REPORT_DIR", str(HERE / "stretch_l2_report")))
TAG_IN_USE = "stretch-l2"

SBCTL = "sudo /usr/local/bin/sbctl -d"


def log(msg):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {msg}", flush=True)


# --- image -------------------------------------------------------------------

def ecr_tag_exists(tag):
    import urllib.error
    import urllib.request
    token = json.load(urllib.request.urlopen(
        "https://public.ecr.aws/token/?scope=repository:simply-block/simplyblock:pull", timeout=30))["token"]
    req = urllib.request.Request(
        f"https://public.ecr.aws/v2/simply-block/simplyblock/manifests/{tag}",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.docker.distribution.manifest.v2+json, "
                           "application/vnd.oci.image.index.v1+json, "
                           "application/vnd.oci.image.manifest.v1+json"})
    try:
        urllib.request.urlopen(req, timeout=30)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def resolve_cp_image(wait_minutes=30):
    """The control-plane image of the branch HEAD (CI publishes
    <branch-with-dashes>-<sha8>); waits for CI to publish it."""
    override = os.environ.get("SB_CP_IMAGE")
    if override:
        return override
    sha = subprocess.check_output(["git", "rev-parse", "--short=8", "HEAD"], cwd=HERE).decode().strip()
    tag = f"{BRANCH.replace('/', '-')}-{sha}"
    deadline = time.time() + wait_minutes * 60
    while not ecr_tag_exists(tag):
        if time.time() > deadline:
            raise RuntimeError(f"CP image {ECR_REPO}:{tag} not published within {wait_minutes} min")
        log(f"waiting for CI to publish {ECR_REPO}:{tag} ...")
        time.sleep(60)
    return f"{ECR_REPO}:{tag}"


# --- ssh ---------------------------------------------------------------------

def ssh_exec(ip, cmds, get_output=False, check=False, timeout=900, quiet=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=USER, key_filename=KEY_PATH, allow_agent=False,
                look_for_keys=False, timeout=60, banner_timeout=60)
    results = []
    try:
        for cmd in cmds:
            if not quiet:
                log(f"[{ip}] $ {cmd}")
            _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
            out = stdout.read().decode(errors="replace")
            err = stderr.read().decode(errors="replace")
            rc = stdout.channel.recv_exit_status()
            results.append((rc, out, err) if get_output else rc)
            if rc != 0 and not quiet:
                log(f"[{ip}] rc={rc}")
                for line in (out.strip().splitlines() or [])[-12:]:
                    print(f"    out: {line}")
                for line in (err.strip().splitlines() or [])[-12:]:
                    print(f"    err: {line}")
            if rc != 0 and check:
                raise RuntimeError(f"command failed on {ip} (rc={rc}): {cmd}\n{out[-2000:]}\n{err[-2000:]}")
    finally:
        ssh.close()
    return results


def out_of(ip, cmd, timeout=600):
    rc, out, err = ssh_exec(ip, [cmd], get_output=True, timeout=timeout, quiet=True)[0]
    return rc, out, err


def wait_for_ssh(ip, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ssh_exec(ip, ["true"], quiet=True)
            return
        except Exception:
            time.sleep(10)
    raise TimeoutError(f"SSH not ready on {ip}")


# --- aws ---------------------------------------------------------------------

def launch(name, itype, site):
    ec2 = boto3.resource("ec2", region_name=REGION)
    return ec2.create_instances(
        ImageId=AMI_ID, InstanceType=itype, MinCount=1, MaxCount=1, KeyName=KEY_NAME,
        NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": SUBNET_ID, "Groups": [SG_ID],
                            "AssociatePublicIpAddress": True}],
        BlockDeviceMappings=[{"DeviceName": "/dev/sda1",
                              "Ebs": {"VolumeSize": 60, "DeleteOnTermination": True, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": name}, {"Key": "in-use-by", "Value": TAG_IN_USE},
            {"Key": "stretch-site", "Value": site}]}])[0]


def launch_all():
    hosts = {"mgmt": {}, "sn": {s: [] for s in SITES}}
    insts = []
    for site in SITES:
        inst = launch(f"SB-STRETCH-MGMT-{site}", MGMT_TYPE, site)
        hosts["mgmt"][site] = inst
        insts.append(inst)
        for i in range(SN_PER_SITE):
            inst = launch(f"SB-STRETCH-SN-{site}{i + 1}", SN_TYPE, site)
            hosts["sn"][site].append(inst)
            insts.append(inst)
    for inst in insts:
        inst.wait_until_running()
        inst.reload()
    return hosts


def terminate(meta):
    ids = meta["instance_ids"]
    log(f"terminating {ids}")
    boto3.client("ec2", region_name=REGION).terminate_instances(InstanceIds=ids)


# --- deployment --------------------------------------------------------------

def install_sbcli(ips):
    cmds = [
        "sudo dnf install git python3-pip nvme-cli fio iptables -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock/sbcli@{BRANCH}"
        " --upgrade --force --ignore-installed requests",
    ]
    with ThreadPoolExecutor(max_workers=len(ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, cmds, False, True, 1800) for ip in ips]:
            t.result()


def uuid_in(text):
    m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", text)
    if not m:
        raise RuntimeError(f"no uuid in: {text[-500:]}")
    return m.group(1)


def scale_services_per_manager(mgmt_ip):
    """One instance of every control-plane service per manager (see module doc)."""
    rc, out, _ = out_of(mgmt_ip, "sudo docker service ls --format '{{.Name}}|{{.Mode}}|{{.Image}}'")
    for line in out.splitlines():
        name, mode, image = (line.split("|") + ["", ""])[:3]
        if mode != "replicated" or "simplyblock" not in image.split("/")[-1]:
            continue
        rc, args, _ = out_of(mgmt_ip, f"sudo docker service inspect {name} --format "
                                      "'{{.Spec.TaskTemplate.ContainerSpec.Args}} {{.Spec.TaskTemplate.ContainerSpec.Command}}'")
        if "simplyblock_core/services" not in args and "simplyblock-task-runner" not in args \
                and "simplyblock-service" not in args:
            continue
        ssh_exec(mgmt_ip, [f"sudo docker service update --replicas 2 --replicas-max-per-node 1 "
                           f"--detach {name}"], check=True)


def deploy():
    cp_image = resolve_cp_image()
    sbctl_img = f"sudo SIMPLY_BLOCK_DOCKER_IMAGE={cp_image} /usr/local/bin/sbctl -d"
    log(f"=== level-2 stretched control plane: branch={BRANCH} image={cp_image} ===")
    hosts = launch_all()
    meta = {
        "branch": BRANCH, "cp_image": cp_image, "region": REGION,
        "mgmt": {s: {"id": hosts["mgmt"][s].id, "ip": hosts["mgmt"][s].public_ip_address,
                     "priv": hosts["mgmt"][s].private_ip_address} for s in SITES},
        "sn": {s: [{"id": i.id, "ip": i.public_ip_address, "priv": i.private_ip_address}
                   for i in hosts["sn"][s]] for s in SITES},
    }
    meta["instance_ids"] = [meta["mgmt"][s]["id"] for s in SITES] + [n["id"] for s in SITES for n in meta["sn"][s]]
    META_PATH.write_text(json.dumps(meta, indent=2))
    log(f"instances: {json.dumps(meta, indent=2)}")

    all_ips = [meta["mgmt"][s]["ip"] for s in SITES] + [n["ip"] for s in SITES for n in meta["sn"][s]]
    for ip in all_ips:
        wait_for_ssh(ip)
    log("phase 1: install sbcli everywhere")
    install_sbcli(all_ips)

    a, b = meta["mgmt"]["A"], meta["mgmt"]["B"]
    log("phase 2a: cluster create on site A")
    create = (f"{sbctl_img} cluster create --ifname {IFACE} --ha-type ha"
              f" --data-chunks-per-stripe 1 --parity-chunks-per-stripe 1 --max-subsys {MAX_SUBSYS}"
              f" --sync-replication --stretched-control-plane --site A"
              f" --site-api-endpoint http://{a['priv']} --leader-site A")
    ssh_exec(a["ip"], [create], check=True, timeout=2400)
    cluster_id = uuid_in(out_of(a["ip"], f"{SBCTL} cluster list")[1])
    secret = out_of(a["ip"], f"{SBCTL} cluster get-secret {cluster_id}")[1].strip().splitlines()[-1].strip()
    meta.update(cluster_id=cluster_id, secret=secret)
    META_PATH.write_text(json.dumps(meta, indent=2))

    log("phase 2b: control-plane add on site B")
    add_cp = (f"{sbctl_img} control-plane add {a['priv']} {cluster_id} {secret} --ifname {IFACE} --mode docker"
              f" --site B --api-endpoint http://{b['priv']}")
    ssh_exec(b["ip"], [add_cp], check=True, timeout=2400)
    time.sleep(20)
    log("phase 2c: one service instance per manager")
    scale_services_per_manager(a["ip"])

    log("phase 3: configure + deploy storage nodes")
    sn_ips = [n["ip"] for s in SITES for n in meta["sn"][s]]
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, [f"{SBCTL} sn configure --max-subsys {MAX_SUBSYS}"], False, True)
                  for ip in sn_ips]:
            t.result()
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, [f"{sbctl_img} sn deploy --isolate-cores --ifname {IFACE}"], False, True)
                  for ip in sn_ips]:
            t.result()
    # The deploy's hugepage / core isolation takes effect on reboot (as the
    # other AWS beds do it); reboot returns non-zero, so it is not checked.
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, ["sudo reboot"], False, False, 60, True) for ip in sn_ips]:
            try:
                t.result()
            except Exception:  # noqa: BLE001 - the ssh session dies with the reboot
                pass
    time.sleep(45)
    for ip in sn_ips:
        wait_for_ssh(ip)
    log("storage nodes back after reboot; waiting for the node API")
    time.sleep(90)

    log("phase 4: add nodes per site")
    for site in SITES:
        for n in meta["sn"][site]:
            for attempt in range(5):
                try:
                    ssh_exec(a["ip"], [f"{sbctl_img} sn add-node {cluster_id} {n['priv']}:5000 {IFACE} --site {site}"],
                             check=True, timeout=1800)
                    break
                except RuntimeError:
                    if attempt == 4:
                        raise
                    log(f"retrying add-node {n['priv']} in 30s")
                    time.sleep(30)
    rc, sn_list, _ = out_of(a["ip"], f"{SBCTL} sn list")
    log(sn_list)
    log("phase 5: activate")
    ssh_exec(a["ip"], [f"{SBCTL} cluster activate {cluster_id}"], check=True, timeout=3600)
    ssh_exec(a["ip"], [f"{SBCTL} pool add pool01 {cluster_id}"], check=True)
    meta["deployed_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    META_PATH.write_text(json.dumps(meta, indent=2))
    log("=== deploy DONE ===")
    return meta


# --- tests -------------------------------------------------------------------

class Results:
    def __init__(self):
        self.items = []

    def record(self, name, ok, detail=""):
        self.items.append({"test": name, "ok": bool(ok), "detail": detail})
        log(f"{'PASS' if ok else 'FAIL'} {name}: {detail[:300]}")

    def summary(self):
        passed = sum(1 for i in self.items if i["ok"])
        return f"{passed}/{len(self.items)} passed"


def cp_status(ip, cluster_id):
    rc, out, err = out_of(ip, f"{SBCTL} control-plane status {cluster_id} --json")
    start = out.find("{")
    if start < 0:
        raise RuntimeError(f"no status json from {ip}: rc={rc} {out[-300:]} {err[-300:]}")
    return json.loads(out[start:])


def leader_status(ip, cid, holder=None, min_epoch=0, mode="leader"):
    """The control-plane status of ``ip`` when it reports ``mode`` (and the
    lease holder / epoch asked for), else None; for wait_until."""
    s = cp_status(ip, cid)
    if s.get("mode") != mode:
        return None
    if holder and s.get("lease_holder_site") != holder:
        return None
    if min_epoch and (s.get("lease_epoch") or 0) < min_epoch:
        return None
    return s


def wait_until(pred, timeout, every=3):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = pred()
            if last:
                return last
        except Exception as e:  # noqa: BLE001 - polled condition
            last = e
        time.sleep(every)
    return False


def service_task_logs(mgmt_ip, service, node_hostname, tail=200):
    """Logs of the task of ``service`` that runs on the manager ``node_hostname``."""
    rc, out, _ = out_of(mgmt_ip, f"sudo docker service ps {service} --no-trunc --filter desired-state=running "
                                 "--format '{{.ID}}|{{.Node}}'")
    for line in out.splitlines():
        tid, node = (line.split("|") + [""])[:2]
        if node_hostname and node_hostname.split(".")[0] in node:
            rc, logs, err = out_of(mgmt_ip, f"sudo docker service logs --raw --tail {tail} {tid} 2>&1")
            return logs + err
    return ""


def block_site_from(site_hosts, from_priv_ip, enable=True):
    flag = "-I" if enable else "-D"
    cmds = [f"sudo iptables {flag} INPUT -s {from_priv_ip} -p icmp -j DROP",
            f"sudo iptables {flag} INPUT -s {from_priv_ip} -p tcp --dport 5000 -j DROP"]
    for ip in site_hosts:
        ssh_exec(ip, cmds, quiet=True)


def run_tests(meta):
    res = Results()
    cid = meta["cluster_id"]
    a, b = meta["mgmt"]["A"], meta["mgmt"]["B"]
    rc, hn_a, _ = out_of(a["ip"], "hostname")
    rc, hn_b, _ = out_of(b["ip"], "hostname")
    hn_a, hn_b = hn_a.strip(), hn_b.strip()

    # T1: A leads, B stands by
    st_a = wait_until(lambda: leader_status(a["ip"], cid), 120)
    res.record("T1a site A is the leader", bool(st_a), json.dumps(st_a)[:400] if st_a else "no leader status")
    st_b = cp_status(b["ip"], cid)
    res.record("T1b site B stands by", st_b.get("mode") == "standby" and st_b.get("lease_holder_site") == "A",
               f"mode={st_b.get('mode')} holder={st_b.get('lease_holder_site')} local={st_b.get('local_site')}")

    # T2: the standby's services are paused, the leader's run
    logs_b = service_task_logs(a["ip"], "app_StorageNodeMonitor", hn_b) or service_task_logs(a["ip"], "StorageNodeMonitor", hn_b)
    res.record("T2a storage-node monitor on B is paused", "does not lead" in logs_b, logs_b[-300:])
    logs_a = service_task_logs(a["ip"], "app_StorageNodeMonitor", hn_a) or service_task_logs(a["ip"], "StorageNodeMonitor", hn_a)
    res.record("T2b storage-node monitor on A runs", "Looping for cluster" in logs_a or "Creating thread" in logs_a,
               logs_a[-300:])

    # T3: B's API redirects a mutation to A, serves a read, keeps control-plane routes
    auth = f"-H 'Authorization: {cid} {meta['secret']}'"
    rc, out, _ = out_of(b["ip"], f"curl -s -o /dev/null -w '%{{http_code}} %{{redirect_url}}' -X POST "
                                 f"http://127.0.0.1:5000/api/v2/clusters/{cid}/storage-pools {auth} "
                                 f"-H 'Content-Type: application/json' -d '{{\"name\":\"redir-test\"}}'")
    res.record("T3a standby API answers a mutation with 307 to the leader",
               out.startswith("307") and a["priv"] in out, out)
    rc, out, _ = out_of(b["ip"], f"curl -s -o /dev/null -w '%{{http_code}}' http://127.0.0.1:5000/api/v2/clusters/{cid}/ {auth}")
    res.record("T3b standby API serves a read", out.strip() == "200", out)
    rc, out, _ = out_of(b["ip"], f"curl -s -o /dev/null -w '%{{http_code}}' http://127.0.0.1:5000/api/v2/clusters/{cid}/control-plane/ {auth}")
    res.record("T3c standby answers the control-plane route itself", out.strip() == "200", out)

    # T4: planned move A -> B and back
    rc, out, err = out_of(a["ip"], f"{SBCTL} control-plane set-leader-site {cid} B")
    res.record("T4a set-leader-site B accepted", rc == 0 and "Refused" not in out, (out + err)[-300:])
    moved = wait_until(lambda: leader_status(b["ip"], cid, holder="B"), 90)
    res.record("T4b lease moved to B within 90s", bool(moved), json.dumps(moved)[:300] if moved else "not moved")
    st_a = cp_status(a["ip"], cid)
    res.record("T4c A stands by after the move", st_a.get("mode") == "standby", f"mode={st_a.get('mode')}")
    logs_b = service_task_logs(a["ip"], "app_StorageNodeMonitor", hn_b) or service_task_logs(a["ip"], "StorageNodeMonitor", hn_b)
    res.record("T4d B's services resumed", "leads again" in logs_b or "Looping for cluster" in logs_b, logs_b[-300:])
    rc, out, err = out_of(a["ip"], f"{SBCTL} control-plane set-leader-site {cid} A")
    back = wait_until(lambda: leader_status(a["ip"], cid, holder="A"), 90)
    res.record("T4e lease moved back to A", bool(back), (out + err)[-200:])

    # T5: takeover - refused while A answers, accepted once A is unreachable
    rc, out, err = out_of(b["ip"], f"{SBCTL} control-plane takeover {cid} --site B --confirm-site-down A")
    res.record("T5a takeover refused while site A answers", "Refused" in out and "answers" in out, (out + err)[-300:])
    site_a_hosts = [a["ip"]] + [n["ip"] for n in meta["sn"]["A"]]
    block_site_from(site_a_hosts, b["priv"], True)
    try:
        time.sleep(5)
        rc, out, err = out_of(b["ip"], f"{SBCTL} control-plane takeover {cid} --site B --confirm-site-down A", timeout=900)
        res.record("T5b takeover accepted once site A is unreachable", rc == 0 and "Takeover recorded" in out,
                   (out + err)[-300:])
        taken = wait_until(lambda: leader_status(b["ip"], cid, holder="B", min_epoch=3), 60)
        res.record("T5c B leads after the takeover with a new epoch", bool(taken),
                   json.dumps(taken)[:300] if taken else "not leading")
    finally:
        block_site_from(site_a_hosts, b["priv"], False)
    st_a = wait_until(lambda: leader_status(a["ip"], cid, mode="standby"), 60)
    res.record("T5d A returns as standby (its lease is stale)", bool(st_a), json.dumps(st_a)[:200] if st_a else "")
    rc, out, err = out_of(a["ip"], f"{SBCTL} control-plane set-leader-site {cid} A")
    back = wait_until(lambda: leader_status(a["ip"], cid), 90)
    res.record("T5e lease moved back to A after the takeover", bool(back), (out + err)[-200:])

    # T6: a red gate refuses the planned move
    sn_b1 = meta["sn"]["B"][0]
    rc, out, _ = out_of(a["ip"], f"{SBCTL} sn list --json")
    try:
        nodes = json.loads(out[out.find("["):])
        node_b1 = next(n for n in nodes if n.get("Mgmt IP", n.get("mgmt_ip", "")) == sn_b1["priv"])
        node_b1_id = node_b1.get("UUID", node_b1.get("uuid"))
    except Exception:  # noqa: BLE001 - optional test
        node_b1_id = None
    if node_b1_id:
        ssh_exec(a["ip"], [f"{SBCTL} sn shutdown {node_b1_id} --force"], timeout=900)
        time.sleep(20)
        rc, out, err = out_of(a["ip"], f"{SBCTL} control-plane set-leader-site {cid} B")
        res.record("T6a move refused while a B node is not online", "Refused" in out and "not online" in out,
                   (out + err)[-300:])
        ssh_exec(a["ip"], [f"{SBCTL} sn restart {node_b1_id}"], timeout=1800)
        ok = wait_until(lambda: "online" in out_of(a["ip"], f"{SBCTL} sn list")[1].split(sn_b1["priv"])[-1][:200], 600, 10)
        res.record("T6b node back online", bool(ok), "")

    final = cp_status(a["ip"], cid)
    res.record("T7 final state: A leads, cluster active", final.get("mode") == "leader", json.dumps(final)[:300])
    return res


# --- collect -----------------------------------------------------------------

def collect(meta, results=None):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    cid = meta["cluster_id"]
    for site in SITES:
        ip = meta["mgmt"][site]["ip"]
        for name, cmd in (("status", f"{SBCTL} control-plane status {cid} --json"),
                          ("sn_list", f"{SBCTL} sn list"),
                          ("cluster_logs", f"{SBCTL} cluster get-logs {cid} --limit 300"),
                          ("services", "sudo docker service ps $(sudo docker service ls -q) --no-trunc --format '{{.Name}}|{{.Node}}|{{.CurrentState}}'"),
                          ("events_cp", f"{SBCTL} cluster get-logs {cid} --limit 2000 | grep -i 'control plane\\|leader' | tail -100")):
            try:
                rc, out, err = out_of(ip, cmd)
                (REPORT_DIR / f"{site}_{name}.txt").write_text(out + err)
            except Exception as e:  # noqa: BLE001 - best effort
                (REPORT_DIR / f"{site}_{name}.txt").write_text(f"failed: {e}")
    a = meta["mgmt"]["A"]["ip"]
    rc, out, _ = out_of(a, "sudo docker service ls --format '{{.Name}}'")
    for svc in out.split():
        if any(k in svc for k in ("Monitor", "WebAppAPI", "tasks", "Tasks", "MgmtNode")):
            rc, logs, err = out_of(a, f"sudo docker service logs --raw --tail 400 {svc} 2>&1 | grep -i "
                                      f"'control plane\\|lease\\|paused\\|leads again\\|DISCONNECTED' | tail -200")
            (REPORT_DIR / f"logs_{svc}.txt").write_text(logs + err)
    if results is not None:
        (REPORT_DIR / "results.json").write_text(json.dumps(results.items, indent=2))
        lines = ["# Stretched control plane, level 2 (Docker Swarm) results", "",
                 f"cluster {cid}, image {meta['cp_image']}, {results.summary()}", "",
                 "| test | result | detail |", "| --- | --- | --- |"]
        for i in results.items:
            lines.append(f"| {i['test']} | {'PASS' if i['ok'] else 'FAIL'} | {i['detail'][:160].replace('|', '/')} |")
        (REPORT_DIR / "report.md").write_text("\n".join(lines))
    log(f"collected into {REPORT_DIR}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("phase", choices=["deploy", "test", "collect", "teardown", "all"])
    args = ap.parse_args()
    meta = json.loads(META_PATH.read_text()) if META_PATH.exists() else None
    results = None
    try:
        if args.phase in ("deploy", "all"):
            meta = deploy()
        if args.phase in ("test", "all"):
            results = run_tests(meta)
            log(f"=== tests: {results.summary()} ===")
        if args.phase in ("collect", "all", "test"):
            collect(meta, results)
    except Exception:
        traceback.print_exc()
        if meta and args.phase == "all":
            try:
                collect(meta, results)
            except Exception:
                traceback.print_exc()
        if meta and args.phase == "all" and not os.environ.get("SB_KEEP"):
            terminate(meta)
        print("RESULT: FAILED", flush=True)
        return 1
    if args.phase in ("teardown",) or (args.phase == "all" and not os.environ.get("SB_KEEP")):
        terminate(meta)
    print(f"RESULT: DONE {results.summary() if results else ''}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
