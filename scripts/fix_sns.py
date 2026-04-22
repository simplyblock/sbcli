import paramiko
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor

with open('cluster_metadata.json') as f:
    meta = json.load(f)
KEY = os.path.expanduser('~/.ssh/mtes01.pem')
BRANCH = 'feature-lvol-migration'
cluster_uuid = meta['cluster_uuid']

def setup_sn(sn):
    ip = sn['public_ip']
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username='ec2-user', key_filename=KEY)

    cmds = [
        ('upgrade sbcli', f'sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH} --upgrade --force --ignore-installed requests 2>&1 | tail -5'),
        ('install nvme-cli', 'sudo dnf install -y nvme-cli 2>&1 | tail -3'),
        ('configure --force', 'echo YES | sudo /usr/local/bin/sbctl sn configure --max-lvol 10 --force 2>&1'),
        ('deploy', 'sudo /usr/local/bin/sbctl sn deploy --isolate-cores --ifname eth0 2>&1'),
    ]
    for label, cmd in cmds:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
        out = stdout.read().decode()
        rc = stdout.channel.recv_exit_status()
        print(f'[{ip}] {label}: rc={rc}')
        if rc != 0:
            print(f'  {out[-800:]}')
            ssh.close()
            return False
        else:
            # Show last few lines on success too
            lines = out.strip().split('\n')
            for line in lines[-3:]:
                print(f'  {line}')

    print(f'[{ip}] rebooting...')
    ssh.exec_command('sudo reboot')
    ssh.close()
    return True

print('Setting up all 6 storage nodes in parallel...')
with ThreadPoolExecutor(max_workers=6) as ex:
    results = list(ex.map(setup_sn, meta['storage_nodes']))
print(f'Results: {results}')

if not all(results):
    print('Some nodes failed!')
    exit(1)

print('Waiting 90s for reboot...')
time.sleep(90)

for sn in meta['storage_nodes']:
    ip = sn['public_ip']
    for attempt in range(30):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username='ec2-user', key_filename=KEY, timeout=5)
            ssh.close()
            print(f'[{ip}] SSH ready')
            break
        except Exception:
            time.sleep(5)

mgmt = paramiko.SSHClient()
mgmt.set_missing_host_key_policy(paramiko.AutoAddPolicy())
mgmt.connect(meta['mgmt']['public_ip'], username='ec2-user', key_filename=KEY)

for sn in meta['storage_nodes']:
    priv_ip = sn['private_ip']
    cmd = f'sudo /usr/local/bin/sbctl sn add-node {cluster_uuid} {priv_ip}:5000 eth0 2>&1'
    print(f'add-node {priv_ip}...')
    stdin, stdout, stderr = mgmt.exec_command(cmd, timeout=120)
    out = stdout.read().decode()
    rc = stdout.channel.recv_exit_status()
    print(f'  rc={rc} {out[-300:]}')

print('Activating cluster...')
time.sleep(10)
stdin, stdout, stderr = mgmt.exec_command(f'sudo /usr/local/bin/sbctl cluster activate {cluster_uuid} 2>&1', timeout=60)
print(stdout.read().decode())

stdin, stdout, stderr = mgmt.exec_command(f'sudo /usr/local/bin/sbctl pool add pool01 {cluster_uuid} 2>&1', timeout=60)
print(stdout.read().decode())

stdin, stdout, stderr = mgmt.exec_command('sudo /usr/local/bin/sbctl cluster list 2>&1')
print(stdout.read().decode())
stdin, stdout, stderr = mgmt.exec_command('sudo /usr/local/bin/sbctl sn list 2>&1')
print(stdout.read().decode())

mgmt.close()
