# Running Tests: E2E, Stress, and Upgrade

---

## 1. Setup for Running Tests

You can run these tests from **any machine** (Management Node, Local Machine, Lab VMs, etc.) as long as the **Bastion** and **API** are reachable. It is not restricted to any machine.


Clone the repository:
```sh
git clone https://github.com/simplyblock-io/sbcli.git
```

Navigate to your project directory:

```sh
cd sbcli/e2e
```

---

## 2. Export Required Environment Variables (Common for All)

Before running any test, set up these environment variables:

```sh
export SSH_USER=root
export KEY_NAME=simplyblock-us-east-2.pem
export BASTION_SERVER=<mgmt ip>
export API_BASE_URL=http://<mgmt ip>/
export SBCLI_CMD=sbcli-dev
export CLUSTER_SECRET=<Your_Cluster_Secret>
export CLUSTER_ID=<Your_Cluster_ID>
```

**Notes:**
- `CLUSTER_ID` and `CLUSTER_SECRET` should be **taken from the cluster created by the user**.
- `CLIENT_IP` is **only needed for Stress Tests**, not for E2E or Upgrade Tests.
---

## 3. Running an E2E Test

To run an **E2E test**:

```sh
nohup python3 e2e.py --testname RandomE2ETest > e2e_output.log 2>&1 &
```

> Make sure you are in the `e2e` directory before running.

---

## 4. Running a Stress Test

For **Stress Testing**, you need to export an additional variable:

```sh
export CLIENT_IP=192.168.xx.xx

OR

export CLIENT_IP="<IP1> <IP2>"
```

Then run:

```sh
nohup python3 stress.py --testname RandomFailoverTest --send_debug_notification True --upload_logs True > stress_output.log 2>&1 &
```

**Note:**

- Use `--upload_logs True` to automatically upload logs to MinIO after the test.

- To send slack notification in case of failure use `export SLACK_WEBHOOK_URL="SLACK_HOOK"` and use flag `--send_debug_notification True`

---

## 5. Running an Upgrade Test

For **Upgrade Testing**, no additional variables are needed.
Run:

```sh
nohup python3 upgrade.py --testname RandomUpgradeTest > upgrade_output.log 2>&1 &
```

---

## 6. Uploading Logs to MinIO

If logs were **not uploaded automatically** during Stress Test or you want to upload manually, export these variables:

```sh
export MINIO_ACCESS_KEY="MinIOAccessKey"
export MINIO_SECRET_KEY="MinIOSecretKey"
export BASTION_IP="<Mgmt IP>"
export MNODES="<Mgmt IP>"
export STORAGE_PRIVATE_IPS="<IP1> <IP2> <IP3>"
export GITHUB_RUN_ID="<Name of folder on MINIO>"
```

**Note:**
- `GITHUB_RUN_ID` is optional. If not set, today's date will be used as the folder name in MinIO.

Run the upload script:

```sh
python3 logs/upload_logs_to_miniio.py
```

---

## 7. Downloading Logs from MinIO

To download logs:

```sh
export MINIO_ACCESS_KEY="MinIOAccessKey"
export MINIO_SECRET_KEY="MinIOSecretKey"
```

Navigate to your desired directory and run:

```sh
python3 download_logs_from_minio.py "e2e-run-logs/<Folder to download>/"
```

---

## 8. Additional Resources

- All log-related scripts are available here:
  [GitHub Logs Directory](https://github.com/simplyblock-io/sbcli/tree/main/e2e/logs)

- For full E2E testing pipelines, you can also use GitHub Actions directly.

---

# 🔧 Happy Testing!