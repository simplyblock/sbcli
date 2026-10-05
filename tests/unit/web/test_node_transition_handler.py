"""NodeTransitionInProgress must reach the operator as a retryable 503.

The operator retries 5xx and treats 409 as "already running", so a node that
is still shutting down must not come back as either a success or a 409: the
device step would then be recorded as started when it never ran.
"""

import asyncio
import json

from simplyblock_core.exceptions import NodeTransitionInProgress


def test_the_handler_answers_503_with_the_reason():
    from simplyblock_web import app as web_app

    response = asyncio.run(web_app.node_transition_handler(
        None, NodeTransitionInProgress("node n1 is still shutting down")))

    assert response.status_code == 503
    assert "still shutting down" in json.loads(response.body)["detail"]


def test_the_handler_is_registered_on_the_app():
    from simplyblock_web import app as web_app

    assert NodeTransitionInProgress in web_app.app.exception_handlers
