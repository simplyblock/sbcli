
class TestNotFoundException(Exception):
    def __init__(self, testname, available_tests):
        self.testname = testname
        self.available_tests = available_tests
        super().__init__(f"Test '{testname}' not found. Available tests are: {available_tests}")
