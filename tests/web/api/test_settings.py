# coding=utf-8
"""
test_web_settings.py – unit tests for simplyblock_web.settings.
"""

from simplyblock_web.settings import Settings, _parse_str_list, _parse_int_list


class TestParseStrList:
    def test_comma_separated_string(self):
        result = _parse_str_list(
            "system:serviceaccount:default:op-a,system:serviceaccount:default:op-b"
        )
        assert result == [
            "system:serviceaccount:default:op-a",
            "system:serviceaccount:default:op-b",
        ]

    def test_trims_whitespace(self):
        result = _parse_str_list(" a , b , c ")
        assert result == ["a", "b", "c"]

    def test_ignores_empty_segments(self):
        result = _parse_str_list(",a,,b,")
        assert result == ["a", "b"]

    def test_passthrough_list(self):
        lst = ["x", "y"]
        assert _parse_str_list(lst) is lst

    def test_empty_string_yields_empty_list(self):
        assert _parse_str_list("") == []


class TestParseIntList:
    def test_comma_separated_string(self):
        assert _parse_int_list("1,2") == [1, 2]

    def test_single_string(self):
        assert _parse_int_list("1") == [1]

    def test_bare_int(self):
        # _CommaSupportedEnvSource applies json.loads first, so a bare digit
        # env var arrives as a Python int rather than a string.
        assert _parse_int_list(2) == [2]

    def test_passthrough_list(self):
        lst = [1, 2]
        assert _parse_int_list(lst) is lst

    def test_empty_string_yields_empty_list(self):
        assert _parse_int_list("") == []


class TestSettings:
    def test_default_is_empty_list(self, monkeypatch):
        monkeypatch.delenv("SB_K8S_ADMIN_SERVICE_ACCOUNTS", raising=False)
        s = Settings()
        assert s.k8s_admin_service_accounts == []

    def test_parses_env_var(self, monkeypatch):
        monkeypatch.setenv(
            "SB_K8S_ADMIN_SERVICE_ACCOUNTS",
            "system:serviceaccount:ns:sa1,system:serviceaccount:ns:sa2",
        )
        s = Settings()
        assert s.k8s_admin_service_accounts == [
            "system:serviceaccount:ns:sa1",
            "system:serviceaccount:ns:sa2",
        ]

    def test_single_entry(self, monkeypatch):
        monkeypatch.setenv(
            "SB_K8S_ADMIN_SERVICE_ACCOUNTS",
            "system:serviceaccount:default:my-operator",
        )
        s = Settings()
        assert s.k8s_admin_service_accounts == ["system:serviceaccount:default:my-operator"]


class TestApiVersionsSetting:
    def test_default_is_all_versions(self, monkeypatch):
        monkeypatch.delenv("SB_API_VERSIONS", raising=False)
        s = Settings()
        assert s.api_versions == {1, 2}

    def test_comma_separated_env_var(self, monkeypatch):
        monkeypatch.setenv("SB_API_VERSIONS", "1,2")
        s = Settings()
        assert s.api_versions == {1, 2}

    def test_single_version_v1_only(self, monkeypatch):
        monkeypatch.setenv("SB_API_VERSIONS", "1")
        s = Settings()
        assert s.api_versions == {1}

    def test_single_version_v2_only(self, monkeypatch):
        monkeypatch.setenv("SB_API_VERSIONS", "2")
        s = Settings()
        assert s.api_versions == {2}

    def test_empty_disables_all(self, monkeypatch):
        monkeypatch.setenv("SB_API_VERSIONS", "")
        s = Settings()
        assert s.api_versions == set()

    def test_unknown_version_fails(self, monkeypatch):
        monkeypatch.setenv("SB_API_VERSIONS", "1,3")
        with pytest.raises(ValidationError, match="SB_API_VERSIONS"):
            Settings()
