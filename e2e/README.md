# Running Stress Tests

## 1. Login to the Test Machine
- SSH into the **192.168.10.72** machine as `root` using the **simplyblock-us-east-2.pem** key.
- Navigate to the stress test directory:
  ```sh
  cd raunak/sbcli-client-stress-node-affinity/e2e
  ```

## 2. Export Required Variables
Before running the stress test, set up the necessary environment variables:
```sh
export AWS_REGION=us-east-2
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
export SSH_USER=root
export KEY_NAME=simplyblock-us-east-2.pem
export BASTION_SERVER=192.168.10.61
export API_BASE_URL=http://192.168.10.61/
export SBCLI_CMD=sbcli-down
export CLUSTER_SECRET=pPnq6A01X6sJPa0Hfo9X
export CLUSTER_ID=4c9dd051-c9ba-4297-8c5c-c97652403041
export SLACK_WEBHOOK_URL="SLACK_HOOK"
export CLIENT_IP=192.168.10.93
```

**Note:** Modify the **CLUSTER_ID** and **CLUSTER_SECRET** by creating a setup on **192.168.10.61**.

## 3. Running the Stress Test
Execute the stress test in the background using `nohup`:
```sh
nohup python3 stress.py --testname RandomFailoverTest --send_debug_notification True > output.log 2>&1 &
```

## 4. Uploading Logs to MinIO
Once the test is completed, export the following variables before uploading logs:
```sh
export MINIO_ACCESS_KEY="MinIOAccessKey"
export MINIO_SECRET_KEY="MinIOSecretKey"
export BASTION_IP="192.168.10.61"
export MNODES="192.168.10.61"
export STORAGE_PRIVATE_IPS="192.168.10.62 192.168.10.63 192.168.10.64"
export SEC_STORAGE_PRIVATE_IPS="192.168.10.65"
export GITHUB_RUN_ID=07-03-2025-RandomOutage-FIOInterrupt-GCImage-sbcli-down
```

- **`GITHUB_RUN_ID`**: This can be set to the **directory name** you want to store logs in MinIO.
  - If **not provided**, it will default to **today’s date**.

Run the log upload script:
```sh
python3 logs/upload_logs_to_miniio.py
```

## 5. Downloading Logs from MinIO
To download logs, export MinIO credentials:
```sh
export MINIO_ACCESS_KEY="MinIOAccessKey"
export MINIO_SECRET_KEY="MinIOSecretKey"
```

Then navigate to the directory where you want to download the logs and run:
```sh
python3 download_logs_from_minio.py "e2e-run-logs/<Folder to download>/"
```

## 6. Additional Resources
- **All log-related scripts** are available here:
  [GitHub Logs Directory](https://github.com/simplyblock-io/sbcli/tree/main/e2e/logs)
- For **E2E testing**, GitHub Actions can be used directly.

