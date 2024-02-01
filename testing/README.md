# Testing CLI

CLI to test ALLOC/BDEV

## Usage
```bash
$ python2.7 main.py --help
usage: main.py [-h] [-d] {iotest} ...

Ultra Testing CLI

positional arguments:
  {iotest}
    iotest     Create files and perform write, read operations

optional arguments:
  -h, --help   show this help message and exit
  -d, --debug  Print debug messages
```

### iotest
```bash
$ python2.7 main.py iotest --help
usage: main.py iotest [-h] -p DIR_PATH -c FILES_COUNT -s FILE_SIZE

Create files and perform write, read and compare operations

optional arguments:
  -h, --help            show this help message and exit
  -p DIR_PATH, --dir-path DIR_PATH
                        Path to the mounted device directory
  -c FILES_COUNT, --files-count FILES_COUNT
                        Number of files to be created
  -s FILE_SIZE, --file-size FILE_SIZE
                        Each file size in bytes
```