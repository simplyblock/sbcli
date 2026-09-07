from pathlib import Path
from typing import Any

import yaml

# Defaults of the alerting configuration file. Mirrors the
# controlplane.observability.grafana.notifications block of the
# simplyblock-operator Helm chart values, so that one file describes the
# receivers for both the docker and the kubernetes deployment. Keep in sync
# with alert_resources.yaml.j2, which relies on every key being present.
ALERTING_DEFAULTS: dict[str, dict[str, Any]] = {
    'slack': {
        'enabled': False,
        'url': '',
    },
    'teams': {
        'enabled': False,
        'url': '',
    },
    'pagerduty': {
        'enabled': False,
        'integrationKey': '',
        'severity': 'critical',
        'class': '',
        'component': '',
        'group': '',
    },
    'opsgenie': {
        'enabled': False,
        'apiKey': '',
        'apiUrl': 'https://api.opsgenie.com/v2/alerts',
        'autoClose': True,
        'overridePriority': False,
        'sendTagsAs': 'tags',
    },
    'webhook': {
        'enabled': False,
        'url': '',
        'httpMethod': 'POST',
        'username': '',
        'password': '',
        'authorizationScheme': '',
        'authorizationCredentials': '',
        'maxAlerts': 0,
    },
}

# Keys without which an enabled receiver cannot deliver anything. Grafana
# accepts such a contact point and then silently drops every notification, so
# reject it while the operator is still watching.
ALERTING_REQUIRED_KEYS: dict[str, list[str]] = {
    'slack': ['url'],
    'teams': ['url'],
    'pagerduty': ['integrationKey'],
    'opsgenie': ['apiKey'],
    'webhook': ['url'],
}


def parse_alerting_config(alert_config_path: Path | None) -> dict[str, Any] | None:
    """Read the alerting configuration file and layer it onto ALERTING_DEFAULTS.

    Returns the values dictionary consumed by alert_resources.yaml.j2: one
    entry per receiver type, each carrying every key the template reads, or
    None when no configuration file was given -- the caller then falls back to
    the deprecated single --contact-point alerting.
    """

    if not alert_config_path:
        return None

    try:
        with open(alert_config_path) as config_file:
            config = yaml.safe_load(config_file)
    except FileNotFoundError as exc:
        # The bare errno message names the path but not what it was meant to
        # be, which is unhelpful in a create-cluster invocation carrying a
        # dozen other paths.
        raise FileNotFoundError(
            f"Alerting configuration file not found: {alert_config_path}"
        ) from exc

    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ValueError(f"Alerting configuration {alert_config_path} must contain a YAML mapping")

    unknown_receivers = sorted(set(config) - set(ALERTING_DEFAULTS))
    if unknown_receivers:
        raise ValueError(
            f"Alerting configuration {alert_config_path} has unknown receiver(s) "
            f"{', '.join(unknown_receivers)}. Known receivers: {', '.join(ALERTING_DEFAULTS)}"
        )

    values: dict[str, Any] = {}
    for receiver, defaults in ALERTING_DEFAULTS.items():
        overrides = config.get(receiver) or {}
        if not isinstance(overrides, dict):
            raise ValueError(
                f"Alerting configuration {alert_config_path}: receiver '{receiver}' must be a YAML mapping"
            )

        unknown_keys = sorted(set(overrides) - set(defaults))
        if unknown_keys:
            raise ValueError(
                f"Alerting configuration {alert_config_path}: receiver '{receiver}' has unknown "
                f"key(s) {', '.join(unknown_keys)}. Known keys: {', '.join(defaults)}"
            )

        # An explicit null means "use the default", matching Helm's behaviour.
        settings = dict(defaults)
        settings.update({key: value for key, value in overrides.items() if value is not None})

        if settings['enabled']:
            missing = [key for key in ALERTING_REQUIRED_KEYS[receiver] if not settings[key]]
            if missing:
                raise ValueError(
                    f"Alerting configuration {alert_config_path}: receiver '{receiver}' is enabled "
                    f"but {', '.join(missing)} is empty"
                )

        values[receiver] = settings

    return values
