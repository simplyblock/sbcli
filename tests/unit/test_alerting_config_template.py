"""Tests for the new multi-receiver alerting configuration.

``alerting_config_parser.parse_alerting_config`` reads the alerting configuration file (same
shape as the ``controlplane.observability.grafana.notifications`` block of the
operator Helm chart) and layers it onto ``alerting_config_parser.ALERTING_DEFAULTS``;
``utils.render_configfile_alerting`` feeds the result to
``alert_resources.yaml.j2``. The assertions below pin down what Grafana needs
from the rendered provisioning file:

  * every enabled receiver becomes exactly one entry under ``contactPoints``,
    and a disabled one leaves no trace,
  * credentials land under ``secureSettings``, never under ``settings``,
  * the Go notification templates survive Jinja rendering verbatim,
  * a receiver that is enabled but cannot deliver (no URL, no key) is
    rejected instead of being provisioned as a silent black hole.
"""

import textwrap
from pathlib import Path

import pytest
import yaml

from simplyblock_cli import alerting_config_parser
from simplyblock_core import utils


def write_config(tmp_path, body):
    path = tmp_path / "alerting.yaml"
    path.write_text(textwrap.dedent(body))
    return path


def render(tmp_path, body):
    values = alerting_config_parser.parse_alerting_config(write_config(tmp_path, body))
    return yaml.safe_load(utils.render_configfile_alerting(values))


def receivers(rendered):
    return {r["uid"]: r for r in rendered["contactPoints"][0]["receivers"]}


# --- merging -----------------------------------------------------------------

def test_no_config_file_yields_no_values():
    """Without a config file the caller must fall back to --contact-point.

    Returning the defaults here instead of None would make every deployment
    take the config-file branch of render_and_deploy_alerting_configs(), which
    silently drops the contact point of a cluster created before
    --alerting-config-path existed.
    """
    assert alerting_config_parser.parse_alerting_config(None) is None


def test_missing_config_file_is_reported_as_the_alerting_config(tmp_path):
    """A path that does not exist must fail loudly, and say what was missing.

    Falling back to --contact-point here would turn an operator typo into a
    cluster that looks configured but never alerts. The message has to name
    the alerting config specifically -- a create-cluster invocation carries
    several other path arguments, and a bare errno does not disambiguate.
    """
    missing = tmp_path / "nope.yaml"

    with pytest.raises(FileNotFoundError, match="Alerting configuration file not found"):
        alerting_config_parser.parse_alerting_config(missing)


def test_merge_fills_in_every_default(tmp_path):
    values = alerting_config_parser.parse_alerting_config(write_config(tmp_path, ""))

    assert set(values) == set(alerting_config_parser.ALERTING_DEFAULTS)
    for receiver, defaults in alerting_config_parser.ALERTING_DEFAULTS.items():
        assert values[receiver] == defaults


def test_merge_keeps_defaults_for_unset_keys(tmp_path):
    values = alerting_config_parser.parse_alerting_config(write_config(tmp_path, """
        opsgenie:
          enabled: true
          apiKey: og-key
          autoClose: false
    """))

    assert values["opsgenie"]["autoClose"] is False           # overridden
    assert values["opsgenie"]["sendTagsAs"] == "tags"         # default
    assert values["opsgenie"]["apiUrl"] == "https://api.opsgenie.com/v2/alerts"


def test_merge_treats_explicit_null_as_default(tmp_path):
    values = alerting_config_parser.parse_alerting_config(write_config(tmp_path, """
        webhook:
          enabled: true
          url: https://example.com/hook
          httpMethod:
    """))

    assert values["webhook"]["httpMethod"] == "POST"


def test_fully_commented_config_enables_nothing():
    """alerting.test.yaml carries every key, all of them commented out.

    Parsing it must therefore reproduce ALERTING_DEFAULTS exactly, with no
    receiver enabled -- this is the "operator commented everything back out"
    case, and it doubles as a check that no key in the fixture has drifted
    out of ALERTING_DEFAULTS (an unknown key would raise).
    """
    example = Path(__file__).parent / "alerting.test.yaml"
    values = alerting_config_parser.parse_alerting_config(example)

    assert not any(receiver["enabled"] for receiver in values.values())


@pytest.mark.parametrize("body, message", [
    ("sms:\n  enabled: true\n", "unknown receiver"),
    ("slack:\n  enabled: true\n  webhook_url: x\n", "unknown "),
    ("slack:\n  enabled: true\n", "is enabled but url is empty"),
    ("pagerduty:\n  enabled: true\n", "integrationKey is empty"),
    ("slack: true\n", "must be a YAML mapping"),
    ("- slack\n", "must contain a YAML mapping"),
])
def test_merge_rejects_invalid_config(tmp_path, body, message):
    with pytest.raises(ValueError, match=message):
        alerting_config_parser.parse_alerting_config(write_config(tmp_path, body))


# --- rendering ---------------------------------------------------------------

def test_no_receiver_enabled_renders_templates_only(tmp_path):
    rendered = render(tmp_path, "")

    assert set(rendered) == {"templates"}


def test_disabled_receiver_leaves_no_contact_point(tmp_path):
    rendered = render(tmp_path, """
        slack:
          enabled: true
          url: https://hooks.slack.com/services/T/B/X
        teams:
          enabled: false
          url: https://outlook.office.com/webhook/abc
    """)

    assert set(receivers(rendered)) == {"grafana"}
    assert "outlook.office.com" not in yaml.dump(rendered)


def test_policy_routes_simplyblock_alerts_to_the_contact_point(tmp_path):
    rendered = render(tmp_path, """
        slack:
          enabled: true
          url: https://hooks.slack.com/services/T/B/X
    """)

    policy = rendered["policies"][0]
    assert policy["receiver"] == rendered["contactPoints"][0]["name"]
    assert policy["routes"][0]["object_matchers"] == [["app", "=", "simplyblock"]]


def test_every_receiver_renders_once(tmp_path):
    rendered = render(tmp_path, """
        slack:
          enabled: true
          url: https://hooks.slack.com/services/T/B/X
        teams:
          enabled: true
          url: https://outlook.office.com/webhook/abc
        pagerduty:
          enabled: true
          integrationKey: pd-key
        opsgenie:
          enabled: true
          apiKey: og-key
        webhook:
          enabled: true
          url: https://example.com/hook
    """)

    assert {uid: r["type"] for uid, r in receivers(rendered).items()} == {
        "grafana": "slack",
        "grafana-teams": "teams",
        "grafana-pagerduty": "pagerduty",
        "grafana-opsgenie": "opsgenie",
        "grafana-webhook": "webhook",
    }


def test_go_notification_templates_survive_jinja(tmp_path):
    rendered = render(tmp_path, """
        slack:
          enabled: true
          url: https://hooks.slack.com/services/T/B/X
    """)

    slack = receivers(rendered)["grafana"]
    assert slack["settings"]["title"] == '{{ template "simplyblock.title" . }}'
    assert slack["settings"]["text"] == '{{ template "simplyblock.message" . }}'

    templates = {t["name"]: t["template"] for t in rendered["templates"]}
    assert set(templates) == {"simplyblock.title", "simplyblock.message"}
    assert templates["simplyblock.message"].startswith("{{ range .Alerts }}")
    assert "{{ .Labels.alertname }}" in templates["simplyblock.message"]


def test_webhook_is_left_without_title_or_message(tmp_path):
    """A generic receiver expects Grafana's own alert JSON, unmodified."""
    rendered = render(tmp_path, """
        webhook:
          enabled: true
          url: https://example.com/hook
    """)

    settings = receivers(rendered)["grafana-webhook"]["settings"]
    assert "title" not in settings
    assert "message" not in settings
    assert settings["httpMethod"] == "POST"
    assert settings["maxAlerts"] == 0


def test_booleans_render_as_yaml_booleans(tmp_path):
    rendered = render(tmp_path, """
        opsgenie:
          enabled: true
          apiKey: og-key
          autoClose: false
          overridePriority: true
    """)

    settings = receivers(rendered)["grafana-opsgenie"]["settings"]
    assert settings["autoClose"] is False
    assert settings["overridePriority"] is True


def test_optional_pagerduty_fields_are_omitted_when_empty(tmp_path):
    rendered = render(tmp_path, """
        pagerduty:
          enabled: true
          integrationKey: pd-key
          component: cluster
    """)

    settings = receivers(rendered)["grafana-pagerduty"]["settings"]
    assert settings["component"] == "cluster"
    assert "class" not in settings
    assert "group" not in settings
    assert settings["severity"] == "critical"


@pytest.mark.parametrize("receiver, uid, body, secret_key, secret_value", [
    ("pagerduty", "grafana-pagerduty", """
        pagerduty:
          enabled: true
          integrationKey: pd-key
    """, "integrationKey", "pd-key"),
    ("opsgenie", "grafana-opsgenie", """
        opsgenie:
          enabled: true
          apiKey: og-key
    """, "apiKey", "og-key"),
])
def test_credentials_go_to_secure_settings(tmp_path, receiver, uid, body, secret_key, secret_value):
    entry = receivers(render(tmp_path, body))[uid]

    assert entry["secureSettings"] == {secret_key: secret_value}
    assert secret_key not in entry["settings"]


def test_webhook_basic_auth_splits_username_and_password(tmp_path):
    entry = receivers(render(tmp_path, """
        webhook:
          enabled: true
          url: https://example.com/hook
          username: user
          password: pass
    """))["grafana-webhook"]

    assert entry["settings"]["username"] == "user"
    assert entry["secureSettings"] == {"password": "pass"}


def test_webhook_authorization_header_splits_scheme_and_credentials(tmp_path):
    entry = receivers(render(tmp_path, """
        webhook:
          enabled: true
          url: https://example.com/hook
          authorizationScheme: Bearer
          authorizationCredentials: token
    """))["grafana-webhook"]

    assert entry["settings"]["authorization_scheme"] == "Bearer"
    assert entry["secureSettings"] == {"authorization_credentials": "token"}


def test_webhook_without_a_complete_credential_pair_has_no_secure_settings(tmp_path):
    """A username with no password would otherwise emit an empty password."""
    entry = receivers(render(tmp_path, """
        webhook:
          enabled: true
          url: https://example.com/hook
          username: user
          authorizationScheme: Bearer
    """))["grafana-webhook"]

    assert "secureSettings" not in entry


def test_quotes_in_a_credential_do_not_break_the_yaml(tmp_path):
    """The rendered file is fed to Grafana as YAML -- it must stay parseable."""
    rendered = render(tmp_path, """
        webhook:
          enabled: true
          url: "https://example.com/hook?q=it's"
          username: user
          password: "pa's\\"s"
    """)

    entry = receivers(rendered)["grafana-webhook"]
    assert entry["settings"]["url"] == "https://example.com/hook?q=it's"
    assert entry["secureSettings"]["password"] == "pa's\"s"


def test_legacy_template_is_still_reachable():
    """--contact-point must keep rendering through the untouched legacy path."""
    rendered = yaml.safe_load(utils.render_legacy_alerting(
        "https://hooks.slack.com/services/T/B/X", "http://grafana.example/grafana",
    ))

    receiver = rendered["contactPoints"][0]["receivers"][0]
    assert receiver["type"] == "slack"
    assert receiver["settings"]["url"] == "https://hooks.slack.com/services/T/B/X"
