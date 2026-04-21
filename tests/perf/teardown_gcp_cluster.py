#!/usr/bin/env python3
"""
teardown_gcp_cluster.py – Destroys all GCP instances for the simplyblock cluster.

Deletes:
  - sb-mgmt
  - sb-sn-0 through sb-sn-4
  - sb-client

Run from your local machine (same place you ran setup_gcp_perf.py):
  python teardown_gcp_cluster.py
"""

import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

_GCLOUD_CMD = ["cmd", "/c", "gcloud"] if sys.platform == "win32" else ["gcloud"]

# ---------------------------------------------------------------------------
# Match these to your setup_gcp_perf.py settings
# ---------------------------------------------------------------------------
PROJECT_ID  = "devmichael"
ZONE        = "us-central1-b"
NAME_PREFIX = "sb"
SN_COUNT    = 5
# ---------------------------------------------------------------------------

def _gcloud(args, check=True):
    cmd = _GCLOUD_CMD + ["--project", PROJECT_ID, "--quiet"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        # Ignore "not found" errors — already deleted
        if "was not found" in result.stderr or "notFound" in result.stderr:
            return None
        print(f"  FAILED: {' '.join(args[:4])}")
        print(f"  stderr: {result.stderr.strip()}")
        raise RuntimeError(f"gcloud failed (rc={result.returncode})")
    return result.stdout


def delete_instance(name):
    print(f"  Deleting instance: {name} ...")
    _gcloud([
        "compute", "instances", "delete", name,
        "--zone", ZONE,
        "--delete-disks", "all",
    ], check=True)
    print(f"  Deleted: {name}")


def delete_firewall_rule(name):
    print(f"  Deleting firewall rule: {name} ...")
    _gcloud(["compute", "firewall-rules", "delete", name], check=True)
    print(f"  Deleted firewall rule: {name}")


def main():
    all_instances = (
        [f"{NAME_PREFIX}-mgmt", f"{NAME_PREFIX}-client"]
        + [f"{NAME_PREFIX}-sn-{i}" for i in range(SN_COUNT)]
    )

    print("=" * 60)
    print("Tearing down simplyblock GCP cluster")
    print(f"  Project : {PROJECT_ID}")
    print(f"  Zone    : {ZONE}")
    print(f"  Instances to delete: {all_instances}")
    print("=" * 60)

    confirm = input("\nType 'yes' to confirm deletion of all instances: ").strip()
    if confirm.lower() != "yes":
        print("Aborted.")
        sys.exit(0)

    # Delete all instances in parallel
    print("\nDeleting instances in parallel...")
    with ThreadPoolExecutor(max_workers=len(all_instances)) as ex:
        futures = [ex.submit(delete_instance, name) for name in all_instances]
        errors = []
        for f in futures:
            try:
                f.result()
            except Exception as e:
                errors.append(str(e))

    if errors:
        print(f"\nWarnings during instance deletion:")
        for e in errors:
            print(f"  {e}")

    # Optionally delete firewall rules
    fw = input("\nAlso delete firewall rules sb-allow-ssh and sb-allow-internal? (yes/no): ").strip()
    if fw.lower() == "yes":
        for rule in [f"{NAME_PREFIX}-allow-ssh", f"{NAME_PREFIX}-allow-internal"]:
            try:
                delete_firewall_rule(rule)
            except Exception as e:
                print(f"  Warning: {e}")

    print("\n" + "=" * 60)
    print("Teardown complete. Re-deploy with:")
    print("  python setup_gcp_perf.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
