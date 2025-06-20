import functools

import pytest

import util


_OPTIONS = ['entrypoint', 'cluster', 'secret']


def pytest_addoption(parser):
    for opt in _OPTIONS:
        parser.addoption(f"--{opt}", action="store", required=True)


def pytest_generate_tests(metafunc):
    for opt in _OPTIONS:
        if opt in metafunc.fixturenames:
            metafunc.parametrize(
                opt,
                [metafunc.config.getoption(opt)] if hasattr(metafunc.config.option, opt) else [],
                scope='session',
            )


@pytest.fixture(scope='session')
def call(request):
    options = request.config.option

    if (missing_options := {opt for opt in _OPTIONS if not hasattr(options, opt)}):
        pytest.skip('Missing options: ' + ','.join(missing_options))

    return functools.partial(
            util.api_call,
            options.entrypoint,
            options.cluster,
            options.secret,
            log_func=print,
    )


@pytest.fixture(scope='module')
def pool(call, cluster):
    pool_uuid = call('POST', '/pool', data={'name': 'poolX', 'cluster_id': cluster, 'no_secret': True})
    yield pool_uuid
    call('DELETE', f'/pool/{pool_uuid}')


@pytest.fixture(scope='module')
def lvol(call, cluster, pool):
    pool_name = call('GET', f'/pool/{pool}')[0]['pool_name']
    lvol_uuid = call('POST', '/lvol', data={
        'name': 'lvolX',
        'size': '1G',
        'pool': pool_name}
    )
    yield lvol_uuid
    call('DELETE', f'/lvol/{lvol_uuid}')
    util.await_deletion(call, f'/lvol/{lvol_uuid}')
