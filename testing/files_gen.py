import logging
import os.path

import hashlib
import random
import string

import threading
from concurrent.futures import ThreadPoolExecutor

import constants

logger = logging.getLogger()


class FileGenerator:

    def __init__(self, dir_path, files_count, file_size):
        self.dir_path = dir_path
        self.files_count = files_count
        self.file_size = file_size
        self.files_map = {}
        self.file_names_formate = "io{:0>%sd}" % len(str(files_count))

    def run(self):
        logger.info("Writing files START")
        self.write_files()
        logger.info("Writing files END")

        logger.info("Validating files START")
        count_validation_passed = self.validate_files()
        logger.info("%s FAILED, %s PASSED", self.files_count - count_validation_passed, count_validation_passed)

    def write_files(self):
        threads = []
        for i in range(self.files_count):
            file_sequential = i+1
            file_name = self.get_file_name(file_sequential)
            file_content = self.generate_random_content()
            file_path = os.path.join(self.dir_path, file_name)
            content_hash = hashlib.md5(file_content).hexdigest()
            self.files_map[file_sequential] = {
                "name": file_name,
                "path": file_path,
                "size": self.file_size,
                "md5sum": content_hash
            }
            t = threading.Thread(target=self.write_to_file, args=[file_content, file_path])
            t.start()
            threads.append(t)
        for thread in threads:
            thread.join()

    def validate_files(self):
        count_validation_passed = 0
        executor = ThreadPoolExecutor(max_workers=constants.THREADS_POOL_COUNT)
        results = executor.map(self.validate_file_worker, self.files_map)
        for result in results:
            if result is True:
                count_validation_passed += 1
        logger.info("Validating files END")
        return count_validation_passed

    def get_file_name(self, file_sequential):
        return self.file_names_formate.format(file_sequential)

    def generate_random_content(self):
        return ''.join(random.choice(string.ascii_letters) for x in range(self.file_size))

    def write_to_file(self, file_content, file_path):
        logger.debug("Writing file %s", file_path)
        with open(file_path, 'wb') as f:
            f.write(file_content)

    def validate_file_worker(self, file_index):
        file_info = self.files_map[file_index]
        logger.info("Validating file %s", file_info['path'])
        try:
            file_content = open(file_info['path'], 'rb').read()
        except IOError as e:
            logger.error("Failed to open/read file %s", file_info['path'])
            logger.debug(e)
            return False

        md5_hash = hashlib.md5(file_content).hexdigest()
        if md5_hash == file_info['md5sum']:
            logger.debug("%s True", md5_hash)
            return True
