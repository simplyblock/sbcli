import subprocess


def cp_blocks_to_file(disk_path, output_file, block_size=512, blocks_count=512):
    cmd = ["dd",
           "if=%s" % disk_path,
           "of=%s" % output_file,
           "bs=%s" % block_size,
           "count=%s" % blocks_count,
           "status=progress"]
    process = subprocess.Popen(cmd, stderr=subprocess.PIPE)
    line = ''
    while True:
        out = process.stderr.read(1)
        if out == '' and process.poll() is not None:
            break
        if out != '':
            s = out.decode("utf-8")
            if s == '\r':
                print(line)
                line = ''
            else:
                line = line + s
    print(line)
