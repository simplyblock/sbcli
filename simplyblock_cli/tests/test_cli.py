import unittest
from unittest import mock


class TestCLI(unittest.TestCase):

    def test_main_called(self):
        with mock.patch("simplyblock_cli.cli.CLIWrapper") as CLIWrapperMock:
            from simplyblock_cli import cli
            cli.main()
            CLIWrapperMock.assert_called()


if __name__ == '__main__':
    unittest.main()
