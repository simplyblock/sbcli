"""A repeated spdk_process_start joins the call that is already running.

Lab OKD, 2026-11-17, three masters as storage nodes: the control plane's
client to the node agent gives a request 300 s and then sends it again. The
agent ran the repeat as a new call, which deleted the CPU topology Job the
first call was waiting on and created another. The Job was restarted at 5 min
19 s, 5 min 26 s and again a minute later, so it never reached its own
6-minute limit, and the first call reported the deleted Job as completed.
"""

import threading
from unittest import mock

import pytest
from flask import Response

from simplyblock_web.api.internal.storage_node import kubernetes as k8s_node


def _started(target):
    thread = threading.Thread(target=target)
    thread.start()
    return thread


def test_a_repeat_of_a_running_call_waits_for_it_and_gets_its_outcome():
    flight = k8s_node._SingleFlight()
    entered, release = threading.Event(), threading.Event()
    runs, outcomes = [], []

    def call():
        runs.append(1)
        entered.set()
        assert release.wait(10)
        return "pod created"

    first = _started(lambda: outcomes.append(flight.run("node-1", call)))
    assert entered.wait(10)
    repeat = _started(lambda: outcomes.append(flight.run("node-1", call)))
    # The repeat is waiting, not running: only the first call got in.
    repeat.join(0.2)
    assert repeat.is_alive()
    assert len(runs) == 1

    release.set()
    first.join(10)
    repeat.join(10)
    assert outcomes == ["pod created", "pod created"]
    assert len(runs) == 1


def test_a_failure_reaches_the_repeat_as_well():
    flight = k8s_node._SingleFlight()
    entered, release = threading.Event(), threading.Event()
    failures = []

    def call():
        entered.set()
        assert release.wait(10)
        raise RuntimeError("Job failed")

    def run():
        with pytest.raises(RuntimeError, match="Job failed") as failure:
            flight.run("node-1", call)
        failures.append(failure.value)

    first = _started(run)
    assert entered.wait(10)
    repeat = _started(run)
    repeat.join(0.2)
    release.set()
    first.join(10)
    repeat.join(10)
    assert len(failures) == 2


def test_a_call_after_the_running_one_finished_runs_again():
    flight = k8s_node._SingleFlight()
    call = mock.Mock(side_effect=["first", "second"])
    assert flight.run("node-1", call) == "first"
    assert flight.run("node-1", call) == "second"
    # A failed call leaves nothing behind either.
    failing = mock.Mock(side_effect=RuntimeError("Job failed"))
    with pytest.raises(RuntimeError):
        flight.run("node-1", failing)
    assert flight.run("node-1", mock.Mock(return_value="third")) == "third"


def test_a_call_with_other_parameters_runs_on_its_own():
    flight = k8s_node._SingleFlight()
    entered, release = threading.Event(), threading.Event()

    def held():
        entered.set()
        assert release.wait(10)
        return "held"

    first = _started(lambda: flight.run("port-4420", held))
    assert entered.wait(10)
    try:
        assert flight.run("port-4421", lambda: "other") == "other"
    finally:
        release.set()
        first.join(10)


def test_the_endpoint_runs_the_start_once_and_answers_each_request():
    entered, release = threading.Event(), threading.Event()
    body = mock.Mock()
    body.model_dump_json.return_value = '{"rpc_port": 4420, "cluster_id": "a97eaa60"}'
    responses = []

    def start(_body):
        entered.set()
        assert release.wait(10)
        return Response('{"status": true, "results": "Pod created"}', mimetype="application/json")

    with mock.patch.object(k8s_node, "_start_spdk_process", side_effect=start) as started:
        first = _started(lambda: responses.append(k8s_node.spdk_process_start(body)))
        assert entered.wait(10)
        repeat = _started(lambda: responses.append(k8s_node.spdk_process_start(body)))
        repeat.join(0.2)
        release.set()
        first.join(10)
        repeat.join(10)

    assert started.call_count == 1
    assert len(responses) == 2
    assert responses[0] is not responses[1]
    assert [r.get_data() for r in responses] == [b'{"status": true, "results": "Pod created"}'] * 2
    assert [r.status_code for r in responses] == [200, 200]
