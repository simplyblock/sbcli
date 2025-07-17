from simplyblock_cli import cli

def test_main_called(mocker):
    mocker.patch('simplyblock_cli.cli.CLIWrapper')
    cli.main()
    cli.CLIWrapper.assert_called()
