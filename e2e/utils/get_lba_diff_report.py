# #!/usr/bin/env python3

# """
# FIO Corruption Analysis Script
# - Connects to a remote machine via SSH
# - Identifies LBA of corruption
# - Extracts block using dd
# - Fetches .expected and .received
# - Diffs all blocks
# - Generates reproducible diff report
# """

# import paramiko
# from scp import SCPClient
# import subprocess
# import os
# from pathlib import Path
# import posixpath

# def create_ssh_client(host, key_path):
#     k = paramiko.Ed25519Key.from_private_key_file(key_path)
#     c = paramiko.SSHClient()
#     c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     c.connect(hostname=host, username='root', pkey=k)
#     return c

# def get_physical_block(ssh, filepath):
#     cmd = f"sudo filefrag -v {filepath}"
#     print(f"Executing: {cmd}")
#     stdin, stdout, stderr = ssh.exec_command(cmd)
#     lines = stdout.readlines()
#     for line in lines:
#         if ".." in line and ":" in line:
#             parts = line.strip().split()
#             logical_range = parts[1].split("..")
#             physical_range = parts[3].split("..")
#             logical_start = int(logical_range[0])
#             physical_start = int(physical_range[0])
#             return physical_start, logical_start
#     raise Exception("Could not parse filefrag output")

# def get_block_size(ssh, filepath):
#     cmd = f"stat -f {filepath}"
#     print(f"Executing: {cmd}")
#     stdin, stdout, stderr = ssh.exec_command(cmd)
#     for line in stdout.readlines():
#         if "Fundamental block size" in line:
#             return int(line.strip().split()[-1])
#     raise Exception("Could not determine block size")

# def find_block_device(ssh, filepath):
#     remote_dir = posixpath.dirname(filepath)
#     cmd = f"df {remote_dir} | tail -1 | awk '{{print $1}}'"
#     print(f"Executing: {cmd}")
#     stdin, stdout, stderr = ssh.exec_command(cmd)
#     device = stdout.read().decode().strip()
#     if not device:
#         raise Exception("Could not find block device for the file")
#     return device

# def get_remote_timestamp(ssh):
#     cmd = "date +%Y%m%d_%H%M%S"
#     print(f"Executing: {cmd}")
#     stdin, stdout, stderr = ssh.exec_command(cmd)
#     timestamp = stdout.read().decode().strip()
#     if not timestamp:
#         raise Exception("Could not retrieve remote timestamp")
#     return timestamp

# def read_disk_block(ssh, block_device, lba, sector_size, count=16, output_file="corrupted_from_disk.bin"):
#     cmd = f"sudo dd if={block_device} bs={sector_size} skip={lba} count={count} status=none of={output_file}"
#     print(f"Executing: {cmd}")
#     ssh.exec_command(cmd)

# def fetch_files(scp, remote_files, local_dir):
#     os.makedirs(local_dir, exist_ok=True)
#     for f in remote_files:
#         print(f"Fetching: {f} to {local_dir}")
#         scp.get(f, os.path.join(local_dir, os.path.basename(f)))

# def to_hex(file_path, hex_path):
#     print(f"Hex dumping: {file_path} to {hex_path}")
#     with open(hex_path, 'w') as hex_out:
#         subprocess.run(["xxd", file_path], stdout=hex_out)

# def diff_hex(file1, file2, label):
#     print(f"Generating diff: {file1} vs {file2} => diff_{label}.txt")
#     with open(f"diff_{label}.txt", "w") as f:
#         subprocess.run(["diff", "-u", file1, file2], stdout=f)

# def main():
#     remote_host = os.environ.get("FIO_REMOTE_HOST")
#     fio_file_path = os.environ.get("FIO_FILE_PATH")
#     offset_bytes = int(os.environ.get("FIO_OFFSET_BYTES"))
#     expected_path = os.environ.get("FIO_EXPECTED_PATH")
#     received_path = os.environ.get("FIO_RECEIVED_PATH")
#     length_bytes = int(os.environ.get("FIO_LENGTH_BYTES", "32768"))

#     if not all([remote_host, fio_file_path, expected_path, received_path]):
#         raise EnvironmentError("One or more required environment variables are missing.")

#     ssh_key_path = os.path.join(Path.home(), ".ssh", "simplyblock-us-east-2.pem")
#     ssh = create_ssh_client(remote_host, ssh_key_path)
#     scp = SCPClient(ssh.get_transport())

#     timestamp = get_remote_timestamp(ssh)
#     work_dir = f"./fio_diff_{timestamp}"
#     os.makedirs(work_dir, exist_ok=True)

#     block_size = get_block_size(ssh, fio_file_path)
#     physical_start, logical_start = get_physical_block(ssh, fio_file_path)
#     block_device = find_block_device(ssh, fio_file_path)
#     print(f"Block size: {block_size}, Block device: {block_device}, Phy start: {physical_start}, Log start: {logical_start} ")

#     logical_block = offset_bytes // block_size
#     print(f"Calculated logical block index: {logical_block} = {offset_bytes} // {block_size}")

#     physical_block = physical_start + (logical_block - logical_start)
#     print(f"Corresponding physical block: {physical_start} + ({logical_block} - {logical_start}) = {physical_block}")

#     lba = physical_block
#     print(f"\nFinal LBA for offset {offset_bytes} is: {lba}\n")

#     print("Reading block via dd on remote machine...")
#     block_count = length_bytes // block_size
#     read_disk_block(ssh, block_device, lba, block_size, count=block_count, output_file="corrupted_from_disk.bin")

#     print("Fetching files to local machine...")
#     fetch_files(scp, [expected_path, received_path, "corrupted_from_disk.bin"], work_dir)

#     print("Converting to hex for diffing...")
#     to_hex(os.path.join(work_dir, os.path.basename(expected_path)), os.path.join(work_dir, "expected.hex"))
#     to_hex(os.path.join(work_dir, os.path.basename(received_path)), os.path.join(work_dir, "received.hex"))
#     to_hex(os.path.join(work_dir, "corrupted_from_disk.bin"), os.path.join(work_dir, "disk.hex"))

#     print("Generating diff reports...")
#     diff_hex(os.path.join(work_dir, "expected.hex"), os.path.join(work_dir, "disk.hex"), "expected_vs_disk")
#     diff_hex(os.path.join(work_dir, "received.hex"), os.path.join(work_dir, "disk.hex"), "received_vs_disk")
#     diff_hex(os.path.join(work_dir, "expected.hex"), os.path.join(work_dir, "received.hex"), "expected_vs_received")

#     print(f"\nAll diffs and hex dumps saved in: {work_dir}\n")
#     ssh.close()

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3

"""
FIO Corruption Analysis Script
- Connects to a remote machine via SSH
- Identifies LBA of corruption
- Extracts block using dd
- Diffs expected/received vs disk
- Generates reproducible diff report on remote
- Copies all results to local machine
"""

import paramiko
from scp import SCPClient
import os
from pathlib import Path
import posixpath

def create_ssh_client(host, key_path):
    k = paramiko.Ed25519Key.from_private_key_file(key_path)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=host, username='root', pkey=k)
    return c

def exec_command(ssh, cmd):
    print(f"Executing: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    return out.strip(), err.strip()

def get_physical_block(ssh, filepath):
    out, _ = exec_command(ssh, f"sudo filefrag -v {filepath}")
    for line in out.splitlines():
        if ".." in line and ":" in line:
            parts = line.strip().split()
            logical_start = int(parts[1].split("..")[0])
            physical_start = int(parts[3].split("..")[0])
            return physical_start, logical_start
    raise Exception("Could not parse filefrag output")

def get_block_size(ssh, filepath):
    out, _ = exec_command(ssh, f"stat -f {filepath}")
    for line in out.splitlines():
        if "Fundamental block size" in line:
            return int(line.strip().split()[-1])
    raise Exception("Could not determine block size")

def find_block_device(ssh, filepath):
    remote_dir = posixpath.dirname(filepath)
    out, _ = exec_command(ssh, f"df {remote_dir} | tail -1 | awk '{{print $1}}'")
    return out

def get_remote_timestamp(ssh):
    out, _ = exec_command(ssh, "date +%Y%m%d_%H%M%S")
    return out

def read_disk_block(ssh, block_device, lba, sector_size, count=16, output_file="corrupted_from_disk.bin"):
    cmd = f"sudo dd if={block_device} bs={sector_size} skip={lba} count={count} status=none of={output_file}"
    exec_command(ssh, cmd)

def to_hex_remote(ssh, binary_file, hex_file):
    cmd = f"xxd {binary_file} > {hex_file}"
    exec_command(ssh, cmd)

def diff_remote(ssh, file1, file2, out_diff):
    cmd = f"diff -u {file1} {file2} > {out_diff}"
    exec_command(ssh, cmd)

def fetch_files(scp, remote_files, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    for f in remote_files:
        print(f"Fetching: {f} to {local_dir}")
        scp.get(f, os.path.join(local_dir, os.path.basename(f)))

def main():
    remote_host = os.environ.get("FIO_REMOTE_HOST")
    fio_file_path = os.environ.get("FIO_FILE_PATH")
    offset_bytes = int(os.environ.get("FIO_OFFSET_BYTES"))
    expected_path = os.environ.get("FIO_EXPECTED_PATH")
    received_path = os.environ.get("FIO_RECEIVED_PATH")
    length_bytes = int(os.environ.get("FIO_LENGTH_BYTES", "32768"))

    if not all([remote_host, fio_file_path, expected_path, received_path]):
        raise EnvironmentError("One or more required environment variables are missing.")

    ssh_key_path = os.path.join(Path.home(), ".ssh", "simplyblock-us-east-2.pem")
    ssh = create_ssh_client(remote_host, ssh_key_path)
    scp = SCPClient(ssh.get_transport())

    timestamp = get_remote_timestamp(ssh)
    remote_dir = f"/tmp/fio_diff_{timestamp}"
    exec_command(ssh, f"mkdir -p {remote_dir}")

    block_size = get_block_size(ssh, fio_file_path)
    physical_start, logical_start = get_physical_block(ssh, fio_file_path)
    block_device = find_block_device(ssh, fio_file_path)
    print(f"Block size: {block_size}, Block device: {block_device}, Phy start: {physical_start}, Log start: {logical_start} ")

    logical_block = offset_bytes // block_size
    print(f"Calculated logical block index: {logical_block} = {offset_bytes} // {block_size}")

    physical_block = physical_start + (logical_block - logical_start)
    print(f"Corresponding physical block: {physical_start} + ({logical_block} - {logical_start}) = {physical_block}")

    lba = physical_block
    print(f"\nFinal LBA for offset {offset_bytes} is: {lba}\n")

    print("Reading block via dd on remote machine...")
    block_count = length_bytes // block_size
    corrupted_path = posixpath.join(remote_dir, "corrupted_from_disk.bin")
    read_disk_block(ssh, block_device, lba, block_size, count=block_count, output_file=corrupted_path)

    print("Generating hex dumps on remote...")
    expected_hex = posixpath.join(remote_dir, "expected.hex")
    received_hex = posixpath.join(remote_dir, "received.hex")
    disk_hex = posixpath.join(remote_dir, "disk.hex")
    to_hex_remote(ssh, expected_path, expected_hex)
    to_hex_remote(ssh, received_path, received_hex)
    to_hex_remote(ssh, corrupted_path, disk_hex)

    print("Generating diffs on remote...")
    diff_remote(ssh, expected_hex, disk_hex, posixpath.join(remote_dir, "diff_expected_vs_disk.txt"))
    diff_remote(ssh, received_hex, disk_hex, posixpath.join(remote_dir, "diff_received_vs_disk.txt"))
    diff_remote(ssh, expected_hex, received_hex, posixpath.join(remote_dir, "diff_expected_vs_received.txt"))

    print("Fetching all remote artifacts to local machine...")
    fetch_files(scp, [expected_path, received_path, corrupted_path,
                     expected_hex, received_hex, disk_hex,
                     posixpath.join(remote_dir, "diff_expected_vs_disk.txt"),
                     posixpath.join(remote_dir, "diff_received_vs_disk.txt"),
                     posixpath.join(remote_dir, "diff_expected_vs_received.txt")], f"./fio_diff_{timestamp}")

    print(f"\nAll diffs and hex dumps saved in: ./fio_diff_{timestamp}\n")
    ssh.close()

if __name__ == "__main__":
    main()
