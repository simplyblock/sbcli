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
        messages = []
        for case, ex_info in self.exceptions.items():
            if isinstance(ex_info, list) and ex_info:
                # ex_info may be [exception] or [exception, traceback_str]
                exc = ex_info[0]
                tb_str = ex_info[1] if len(ex_info) > 1 else None
                if tb_str:
                    # Include last meaningful line of traceback for context
                    tb_lines = [ln.strip() for ln in tb_str.strip().splitlines() if ln.strip()]
                    detail = tb_lines[-1] if tb_lines else str(exc)
                    messages.append(f"{case}: {detail}")
                else:
                    messages.append(f"{case}: {exc}")
            else:
                messages.append(f"{case}: {ex_info}")
        # Join with ' || ' so all failures appear on one line — grep/CI can capture everything
        return " || ".join(messages)

class LvolNotConnectException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)

class SkippedTestsException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)

class CoreFileFoundException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)