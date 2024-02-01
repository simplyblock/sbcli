import argparse
import logging

import constants
from files_gen import FileGenerator


class CLIWrapper:

    parser = argparse.ArgumentParser(description='Ultra Testing CLI')
    parser.add_argument("-d", '--debug', help='Print debug messages', required=False, action='store_true')
    subparser = parser.add_subparsers(dest='command')

    # Initialize cluster parser
    iotest = subparser.add_parser('iotest',
                                  description='Create files and perform write, read and compare operations',
                                  help='Create files and perform write, read and compare operations')

    iotest.add_argument("-p", "--dir-path", help='Path to the mounted device directory', required=True)
    iotest.add_argument("-c", "--files-count", help='Number of files to be created', type=int, required=True)
    iotest.add_argument("-s", "--file-size", help='Each file size in bytes', type=int, required=True)

    def __init__(self):
        pass

    def run(self):
        args = self.parser.parse_args()
        log_level = constants.LOG_LEVEL
        if args.debug:
            log_level = logging.DEBUG
        logger = logging.getLogger()
        logger.setLevel(log_level)

        if args.command == 'iotest':
            dir_path = args.dir_path
            files_count = args.files_count
            file_size = args.file_size
            logger.debug("Dir path: %s", dir_path)
            logger.debug("Files count: %s", files_count)
            logger.debug("File size: %s", file_size)
            fg = FileGenerator(dir_path, files_count, file_size)
            fg.run()
