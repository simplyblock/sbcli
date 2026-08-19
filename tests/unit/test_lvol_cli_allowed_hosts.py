"""The CLI's own check on ``lvol add --allowed-hosts``.

An NQN reaching a volume's allow-list is validated by the type it is declared
with -- ``util.NQN`` in the v2 request models. The CLI has no such model: it
reads a JSON file and calls ``add_lvol_ha`` in-process, so it is the one door
the API's types do not stand in front of, and it has to hold the same line.

What is downstream of that line is why it matters: the allow-list is copied onto
every backup of the volume and published in that backup's manifest, both of
which declare their NQNs with the same type and would refuse the value there --
during a backup, or during the recovery that reads the manifest.
"""
import json
from unittest.mock import patch

import pytest

from simplyblock_cli import clibase


VALID = "nqn.2024-01.io.simplyblock:host"


class _Args:
    """Stand-in for the argparse namespace, with only the attributes set."""

    def __init__(self, **kwargs):
        defaults = dict(
            name="vol", size="1G", pool="pool", host_id=None, ha_type="default",
            comp=False, encrypt=False, distr_vuid=0, snapshot=False, max_size="0",
            max_rw_iops=0, max_rw_mbytes=0, max_r_mbytes=0, max_w_mbytes=0,
            lvol_priority_class=0, ndcs=0, npcs=0, uid=None, pvc_name=None,
            namespaced=False, max_namespace_per_subsys=None, fabric="tcp",
            replicate=False, allowed_hosts=None,
        )
        defaults.update(kwargs)
        for key, value in defaults.items():
            setattr(self, key, value)


@pytest.fixture
def hosts_file(tmp_path):
    def write(content):
        path = tmp_path / "hosts.json"
        path.write_text(json.dumps(content))
        return str(path)
    return write


def _run(args):
    with patch.object(clibase.lvol_controller, "add_lvol_ha",
                      return_value=("lvol-1", None)) as add_lvol_ha:
        result = clibase.CLIWrapperBase.volume__add(None, "add", args)
    return result, add_lvol_ha


class TestAllowedHostsFile:

    def test_valid_nqns_are_passed_through(self, hosts_file):
        result, add_lvol_ha = _run(_Args(allowed_hosts=hosts_file([VALID])))

        assert result is not False
        assert add_lvol_ha.call_args.kwargs["allowed_hosts"] == [VALID]

    @pytest.mark.parametrize("given", [
        "nqn:host",          # no date segment: what the tests used to pass
        "just-a-string",
        "",
    ])
    def test_a_malformed_nqn_is_refused(self, given, hosts_file, capsys):
        result, add_lvol_ha = _run(_Args(allowed_hosts=hosts_file([VALID, given])))

        assert result is False
        add_lvol_ha.assert_not_called()
        assert repr(given) in capsys.readouterr().out

    def test_a_non_string_entry_is_refused(self, hosts_file):
        """The file is operator-supplied JSON, so the entries are not even str."""
        result, add_lvol_ha = _run(_Args(allowed_hosts=hosts_file([{"nqn": VALID}])))

        assert result is False
        add_lvol_ha.assert_not_called()

    def test_no_file_means_no_restriction(self):
        """Absent is not an empty allow-list: it leaves the subsystem open."""
        result, add_lvol_ha = _run(_Args())

        assert result is not False
        assert add_lvol_ha.call_args.kwargs["allowed_hosts"] is None
