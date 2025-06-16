import os
import boto3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# MinIO Configuration
MINIO_ENDPOINT = "http://192.168.10.164:9000"
MINIO_BUCKET = "e2e-run-logs"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# Initialize MinIO Client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Download function
def download_file(s3_file_key, minio_prefix, local_base_dir):
    thread_name = threading.current_thread().name
    relative_file_path = s3_file_key[len(minio_prefix):].lstrip("/")
    local_file_path = os.path.join(local_base_dir, relative_file_path)

    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

    if "_iolog" in s3_file_key:
        return f"[{thread_name}] [SKIP] {s3_file_key} (iolog file)"
    
    while True:
        start_time = time.time()
        print(f"[{thread_name}] [START] {s3_file_key}")
        try:
            s3_client.download_file(MINIO_BUCKET, s3_file_key, local_file_path)
            duration = time.time() - start_time
            return f"[{thread_name}] [DONE]  {s3_file_key} → {local_file_path} ({duration:.2f}s)"
        except Exception as e:
            print(f"[{thread_name}] [RETRY] Failed to download {s3_file_key}: {e}")
            time.sleep(2)  # Optional: add small delay to avoid hammering the server


# Main recursive download logic
def download_from_minio(minio_uri, max_workers=5):
    if not minio_uri.startswith(f"{MINIO_BUCKET}/"):
        print(f"[ERROR] URI must start with '{MINIO_BUCKET}/'")
        return

    minio_prefix = minio_uri[len(MINIO_BUCKET) + 1:]
    local_base_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(local_base_dir, exist_ok=True)

    print(f"[INFO] Fetching file list from MinIO: {minio_uri}")
    objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=minio_prefix)

    if "Contents" not in objects:
        print(f"[ERROR] No files found under {minio_uri}")
        return

    total_files = len(objects["Contents"])
    print(f"[INFO] Found {total_files} files. Starting download with {max_workers} threads...\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_file, obj["Key"], minio_prefix, local_base_dir)
            for obj in objects["Contents"]
        ]
        for i, future in enumerate(as_completed(futures), 1):
            print(f"[{i}/{total_files}] {future.result()}")

# CLI entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel download logs from MinIO")
    parser.add_argument("minio_uri", help="e.g., e2e-run-logs/07-03-2025-Folder/IP")
    parser.add_argument("--workers", type=int, default=5, help="Parallel threads (default=5)")
    args = parser.parse_args()
    download_from_minio(args.minio_uri, args.workers)
