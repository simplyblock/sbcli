class TestNotFoundException(Exception):
    def __init__(self, testname, available_tests):
        self.testname = testname
        self.available_tests = available_tests
        super().__init__(f"Test '{testname}' not found. Available tests are: {available_tests}")


class MultipleExceptions(Exception):
    def __init__(self, exceptions):
        self.exceptions = exceptions
        super().__init__(self._create_message())

    def _create_message(self):
        messages = [f"{case}: {ex}" for case, ex in self.exceptions.items()]
        return "\n".join(messages)

class LvolNotConnectException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)

class SkippedTestsException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)

class CoreFileFoundException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)