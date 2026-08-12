# coding=utf-8
"""One-shot orchestrator for the edge-clusters e2e campaign.

Runs the whole thing end to end and leaves a self-contained run directory
behind (per-stage logs, junit xml, cluster status snapshots, and — on
failure — collected pod/service logs from every cluster):

    provision -> deploy (test 1) -> tests 2..6 -> [repeat for --soak-cycles]
              -> log collection -> optional teardown

Usage:
    python edge_e2e/run_all.py --region eu-west-1 --key-name mykey
    python edge_e2e/run_all.py --skip-provision --only 04,05      # re-run subset
    python edge_e2e/run_all.py --soak-cycles 12 --keep            # overnight soak
    python edge_e2e/run_all.py --teardown-only

Exit code is non-zero if any stage failed, so CI can gate on it.
"""
import argparse
import datetime
import json
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).parent
REPO = HERE.parent
RUNS = HERE / "runs"

# Test-id prefixes in execution order; --only selects a subset.
STAGES = ["02", "03a", "03b", "04", "05a", "05b", "06"]


def _now():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


class Runner:
    def __init__(self, run_dir):
        self.run_dir = run_dir
        self.results = []

    def run(self, name, argv, timeout=None):
        """Run one stage, tee its output to <run_dir>/<name>.log, record the
        outcome. Returns True on success."""
        log_path = self.run_dir / f"{name}.log"
        print(f"\n=== [{_now()}] {name}: {' '.join(argv)}")
        started = time.monotonic()
        with open(log_path, "w", encoding="utf-8", errors="replace") as log:
            try:
                process = subprocess.run(argv, cwd=REPO, stdout=log,
                                         stderr=subprocess.STDOUT, timeout=timeout)
                rc = process.returncode
            except subprocess.TimeoutExpired:
                log.write(f"\n*** stage timed out after {timeout}s ***\n")
                rc = 124
        duration = round(time.monotonic() - started, 1)
        ok = rc == 0
        self.results.append({"stage": name, "rc": rc, "ok": ok,
                             "duration_s": duration, "log": str(log_path)})
        print(f"--- {name}: {'PASS' if ok else f'FAIL (rc={rc})'} in {duration}s "
              f"-> {log_path}")
        return ok

    def summary(self):
        path = self.run_dir / "summary.json"
        path.write_text(json.dumps(self.results, indent=2))
        print(f"\n===== summary ({path})")
        for entry in self.results:
            print(f"  {'PASS' if entry['ok'] else 'FAIL'}  {entry['stage']:<28} "
                  f"{entry['duration_s']:>8}s")
        return all(entry["ok"] for entry in self.results)


def collect_logs(run_dir):
    """Best-effort forensic capture from every cluster in the state file."""
    try:
        sys.path.insert(0, str(REPO))
        from edge_e2e import helpers
        state = helpers.load_state()
    except Exception as e:
        print(f"log collection skipped: {e}")
        return

    out = run_dir / "cluster-logs"
    out.mkdir(exist_ok=True)
    targets = [(f"{state['central']['server']}", "central")]
    for name, entry in state.get("edge", {}).items():
        targets.extend((node, name) for node in entry["nodes"])

    for node_name, label in targets:
        for what, command in (
                ("nodes", "get nodes -o wide"),
                ("pods", "get pods -A -o wide"),
                ("events", "get events -A --sort-by=.lastTimestamp"),
        ):
            try:
                text = helpers.kubectl(state, node_name, command, check=False,
                                       timeout=60)
            except Exception as e:
                text = f"<collection failed: {e}>"
            (out / f"{label}-{node_name}-{what}.txt").write_text(text or "")
        try:
            text = helpers.ssh(state, node_name,
                               "sudo journalctl -u k3s -u k3s-agent --no-pager -n 2000",
                               check=False, timeout=120)
            (out / f"{label}-{node_name}-k3s.log").write_text(text or "")
        except Exception:
            pass
    print(f"cluster logs collected -> {out}")


def snapshot_status(run_dir, tag):
    """Record every cluster's status + node states (cheap, non-fatal)."""
    try:
        sys.path.insert(0, str(REPO))
        from edge_e2e import helpers
        state = helpers.load_state()
        base = state["central"]["api_url"]
        snapshot = {}
        for name, entry in state.get("edge", {}).items():
            api = helpers.EdgeApi(base, entry["cluster_id"], entry["secret"])
            snapshot[name] = {
                "cluster": api.cluster_status(),
                "nodes": [{"hostname": n["hostname"], "status": n["status"],
                           "leader_of": n.get("leader_of", []),
                           "partitions": [(p["device_path"], p["status"])
                                          for p in n["partitions"]]}
                          for n in api.nodes()],
            }
        (run_dir / f"status-{tag}.json").write_text(json.dumps(snapshot, indent=2))
    except Exception as e:
        print(f"status snapshot ({tag}) skipped: {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--key-name")
    parser.add_argument("--skip-provision", action="store_true")
    parser.add_argument("--skip-deploy", action="store_true")
    parser.add_argument("--only", help="comma-separated test ids, e.g. 03b,04")
    parser.add_argument("--soak-cycles", type=int, default=1,
                        help="repeat the test stages N times (fault soak)")
    parser.add_argument("--keep", action="store_true",
                        help="do not destroy the environment at the end")
    parser.add_argument("--teardown-only", action="store_true")
    parser.add_argument("--settle-sec", type=int, default=120,
                        help="wait after provision for cloud-init/k3s")
    args = parser.parse_args()

    python = sys.executable
    RUNS.mkdir(exist_ok=True)
    run_dir = RUNS / f"run-{_now()}"
    run_dir.mkdir()
    print(f"run directory: {run_dir}")
    runner = Runner(run_dir)

    if args.teardown_only:
        runner.run("teardown", [python, "edge_e2e/provision.py",
                                "--region", args.region, "--destroy"])
        sys.exit(0 if runner.summary() else 1)

    ok = True
    try:
        if not args.skip_provision:
            if not args.key_name:
                sys.exit("--key-name is required unless --skip-provision")
            ok = runner.run("01-provision",
                            [python, "edge_e2e/provision.py", "--region", args.region,
                             "--key-name", args.key_name], timeout=3600)
            if ok:
                print(f"waiting {args.settle_sec}s for cloud-init / k3s...")
                time.sleep(args.settle_sec)

        if ok and not args.skip_deploy:
            # deploy.py IS test 1 (deploy simplyblock on all clusters)
            ok = runner.run("02-deploy-test01", [python, "edge_e2e/deploy.py"],
                            timeout=7200)

        if ok:
            selected = ([s.strip() for s in args.only.split(",")]
                        if args.only else STAGES)
            for cycle in range(1, args.soak_cycles + 1):
                snapshot_status(run_dir, f"cycle{cycle}-pre")
                for stage in selected:
                    name = f"03-tests-cycle{cycle}-{stage}"
                    stage_ok = runner.run(name, [
                        python, "-m", "pytest", "edge_e2e/test_edge_e2e.py",
                        "-v", "-k", f"test_{stage}_",
                        f"--junitxml={run_dir / (name + '.xml')}",
                        "-p", "no:cacheprovider",
                    ], timeout=14400)
                    ok = ok and stage_ok
                snapshot_status(run_dir, f"cycle{cycle}-post")
                if not ok and args.soak_cycles > 1:
                    print("stopping soak early: a cycle failed")
                    break
    finally:
        if not ok:
            collect_logs(run_dir)
        if not args.keep and not args.skip_provision:
            runner.run("99-teardown", [python, "edge_e2e/provision.py",
                                       "--region", args.region, "--destroy"],
                       timeout=1800)

    sys.exit(0 if runner.summary() else 1)


if __name__ == "__main__":
    main()
