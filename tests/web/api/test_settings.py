# coding=utf-8
"""
test_web_settings.py – unit tests for simplyblock_web.settings.
"""

from simplyblock_web.settings import Settings, _parse_str_list


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
