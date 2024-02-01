import argparse
import hashlib
import logging
import os
import re
import sys

_LOG_FILE = "/var/log/simply-block.log"
logger = logging.getLogger("simply_block")


def parse_opts(argv):
    parser = argparse.ArgumentParser(description='Automate Unit Test for block I/O operations')
    parser.add_argument('-d', '--debug', dest="debug", action='store_true', default=False,
                        help="Print debugging output.", required=False)
    parser.add_argument('-c', '--config-file', dest="config_file", action='store',
                        help="Configuration file containing a list of input configurations (start-index end-index operation)",
                        required=True)
    parser.add_argument('-s', '--block-size', dest="block_size", default="512", action='store',
                        help="The size of each block to be written.", required=False)
    parser.add_argument('-b', '--block-device', dest="block_device", action='store',
                        help="The block device to be tested", required=True)
    return parser.parse_args(argv[1:])


def parse_size(size_str):
    size_str = size_str.strip().lower()
    size_match = re.match(r'^(\d+(\.\d+)?)\s*([kmgtKMGT]??)$', size_str)
    if size_match:
        value, _, unit = size_match.groups()
        value = float(value)

        if unit in ['k', "K"]:
            return int(value * 1024)
        elif unit in ['m', "M"]:
            return int(value * 1024 ** 2)
        elif unit in ['g', "G"]:
            return int(value * 1024 ** 3)
        elif unit in ['t', "T"]:
            return int(value * 1024 ** 4)
        else:
            return int(value)

    else:
        raise ValueError(f"Invalid size format: {size_str}")
        exit(1)


def configure_logger(debug=False):
    LOG_FORMAT = '[%(asctime)s] [%(levelname)s] %(message)s'
    DATE_FORMAT = '%Y/%m/%d %I:%M:%S %p'
    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(format=LOG_FORMAT, datefmt=DATE_FORMAT,
                        level=log_level, filename=_LOG_FILE,
                        filemode='w')


def generate_data_and_hash(block_size):
    random_data = os.urandom(block_size)
    data_hash = hashlib.md5(random_data).hexdigest()
    return random_data, data_hash


def write_block(block_device, block_index, data, block_size):
    with open(block_device, 'rb+') as f:
        f.seek(block_index * block_size)
        f.write(data)


def read_block(block_device, block_index, block_size):
    with open(block_device, 'rb') as f:
        f.seek(block_index * block_size)
        data = f.read(block_size)
        data_hash = hashlib.md5(data).hexdigest()
        return data, data_hash


def perform_block_io(config_file, block_device, block_size):
    logger.info(f"Start testing for combinations in {config_file}")
    block_map, zero_hash = {}, hashlib.md5(bytearray(block_size)).hexdigest()
    error_list = []
    with open(config_file, 'r') as config:
        for line_number, line in enumerate(config, start=1):
            parts = line.strip().split()
            if len(parts) != 3:
                err_msg = f"{line_number}, {block_device}, Invalid configuration line"
                logger.error(err_msg)
                error_list.append(err_msg)
                continue
            start_index, end_index, operation = int(parts[0]), int(parts[1]), parts[2]
            if operation not in ['read', 'write']:
                err_msg = f"{line_number}, {block_device}, Invalid operation: {operation}"
                logger.error(err_msg)
                error_list.append(err_msg)
                continue
            logger.info(f"Start testing for line: {line}, start_index: {start_index}, end_index: {end_index}")
            for block_index in range(start_index, end_index + 1):
                if operation == 'write':
                    data, data_hash = generate_data_and_hash(block_size)
                    logger.debug(f"Storing data_hash: {data_hash} for block: {block_index}")
                    block_map[block_index] = data_hash
                    write_block(block_device, block_index, data, block_size)
                elif operation == 'read':
                    data, data_hash = read_block(block_device, block_index, block_size)
                    expected_hash = block_map.get(block_index, zero_hash)
                    logger.debug(
                        f"This block has {'not ' if expected_hash == zero_hash else ''}been written before, expected hash is: {expected_hash}")
                    if expected_hash != data_hash:
                        err_msg = f"{line_number}, {block_device}, Hash mismatch for block {block_index}"
                        logger.error(err_msg)
                        error_list.append(err_msg)
    if error_list:
        print(f"The test failed :(, please check {_LOG_FILE} for more info")
        exit(1)
    else:
        print("The test run successfully :)")
        exit(0)


def main(argv=sys.argv):
    opts = parse_opts(argv)
    configure_logger(opts.debug)
    perform_block_io(opts.config_file, opts.block_device, parse_size(opts.block_size))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
