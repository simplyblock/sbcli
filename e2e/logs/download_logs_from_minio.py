import os
import boto3
import argparse

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

# Function to recursively download files from MinIO
def download_from_minio(minio_uri):
    """
    Downloads all files from a given MinIO URI and maintains the folder structure.
    
    Args:
        minio_uri (str): The MinIO URI (e.g., "e2e-run-logs/07-03-2025-RandomOutage-FIOInterrupt-GCImage-sbcli-down/192.168.10.62")
    """
    if not minio_uri.startswith(f"{MINIO_BUCKET}/"):
        print(f"[ERROR] Invalid MinIO URI. It should start with '{MINIO_BUCKET}/'.")
        return

    # Remove bucket name from URI to get the prefix
    minio_prefix = minio_uri[len(MINIO_BUCKET) + 1 :]

    # Ensure logs directory exists
    local_base_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(local_base_dir, exist_ok=True)

    print(f"[INFO] Downloading logs from MinIO path: {minio_uri}")

    # List all files under the given prefix
    objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=minio_prefix)

    if "Contents" not in objects:
        print(f"[ERROR] No files found under {minio_uri}.")
        return

    for obj in objects["Contents"]:
        s3_file_key = obj["Key"]
        relative_file_path = s3_file_key[len(minio_prefix) :].lstrip("/")
        local_file_path = os.path.join(local_base_dir, relative_file_path)

        # Ensure local directories exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        if "_iolog" in s3_file_key:
            print(f"[INFO] Skipping download for iolog file: {s3_file_key}")
        else:
            print(f"[INFO] Downloading {s3_file_key} → {local_file_path}")

            # Download the file
            try:
                s3_client.download_file(MINIO_BUCKET, s3_file_key, local_file_path)
                print(f"[SUCCESS] Downloaded: {local_file_path}")
            except Exception as e:
                print(f"[ERROR] Failed to download {s3_file_key}: {e}")

# Parse arguments
parser = argparse.ArgumentParser(description="Download logs from MinIO recursively.")
parser.add_argument("minio_uri", type=str, help="The MinIO URI to download logs from.")

args = parser.parse_args()

# Execute download
download_from_minio(args.minio_uri)
