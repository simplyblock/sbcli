# coding=utf-8
"""fio workload plumbing: a privileged pod per cluster that nvme-connects a
volume and runs the standard job (2 jobs, iodepth 2, 10 GiB each, 30/70
read/write mix, max_latency 20s so a stall is an explicit fio failure)."""

from e2e.edge import helpers

FIO_IMAGE = "ubuntu:22.04"

# max_latency turns an IO stall into a hard job failure — the interruption
# detector for the failover tests.
FIO_CMD = ("fio --name=edge-e2e --filename={device} --direct=1 --ioengine=libaio "
           "--rw=randrw --rwmixread=30 --bs=4k --iodepth=2 --numjobs=2 "
           "--size={size} --max_latency=20s --time_based={time_based} "
           "--runtime={runtime} --group_reporting --output-format=json")

POD_TEMPLATE = """apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  labels: {{app: edge-e2e-fio}}
spec:
  hostNetwork: true
  hostPID: true
  restartPolicy: Never
  containers:
    - name: fio
      image: {image}
      securityContext: {{privileged: true}}
      command: ["/bin/bash", "-c"]
      args:
        - |
          set -e
          apt-get update -qq && apt-get install -y -qq fio nvme-cli > /dev/null
          # Connect EVERY path (active first, passive second) — the passive
          # path activates on takeover without a reconnect.
          {connect_cmds}
          sleep 3
          DEV=$(nvme list -o json | python3 -c "import json,sys; \\
            print([d['DevicePath'] for d in json.load(sys.stdin)['Devices'] \\
                   if '{nqn_tail}' in d.get('SubsystemNQN','') or True][0])")
          {fio}
      volumeMounts:
        - {{name: dev, mountPath: /dev}}
  volumes:
    - {{name: dev, hostPath: {{path: /dev}}}}
"""


def start_fio_pod(state, server_name, pod_name, connect, *, size="10G",
                  runtime=0):
    """Render + apply the fio pod on the cluster whose k3s server is
    server_name. `connect` is one entry or the full connect-info list; every
    listed path is connected (active/passive dual paths on 2-node clusters).
    runtime>0 makes the run time-based (for failover windows); runtime=0 runs
    the full size once."""
    entries = connect if isinstance(connect, list) else [connect]
    connect_cmds = "\n          ".join(
        f"nvme connect -t tcp -a {e['ip']} -s {e['port']} -n {e['nqn']} "
        f"--ctrl-loss-tmo=-1 --reconnect-delay=2 || true"
        for e in entries)
    fio = FIO_CMD.format(device="$DEV", size=size,
                         time_based=1 if runtime else 0,
                         runtime=runtime or 60)
    manifest = POD_TEMPLATE.format(
        pod_name=pod_name, image=FIO_IMAGE, connect_cmds=connect_cmds,
        nqn_tail=entries[0]["nqn"].split(":")[-1], fio=fio)
    helpers.ssh(state, server_name,
                f"cat <<'EOF' | sudo kubectl apply -f -\n{manifest}\nEOF")


def wait_fio_result(state, server_name, pod_name, timeout=3600) -> dict:
    """Wait for the pod to finish; return {'succeeded': bool, 'log': str}."""
    def phase():
        out = helpers.kubectl(
            state, server_name,
            f"get pod {pod_name} -o jsonpath='{{.status.phase}}'", check=False)
        return out.strip() in ("Succeeded", "Failed") and out.strip()

    final = helpers.wait_for(f"fio pod {pod_name} completion", phase,
                             timeout=timeout, interval=15)
    log = helpers.kubectl(state, server_name, f"logs {pod_name}", check=False)
    return {"succeeded": final == "Succeeded", "log": log}


def delete_fio_pod(state, server_name, pod_name):
    helpers.kubectl(state, server_name,
                    f"delete pod {pod_name} --ignore-not-found --wait=false",
                    check=False)


def fio_interrupted(result) -> bool:
    """A failed pod, a latency violation, or io errors count as interruption."""
    if not result["succeeded"]:
        return True
    log = result["log"]
    return "max latency exceeded" in log or '"error" : 0' not in log.replace(" ", " ")
