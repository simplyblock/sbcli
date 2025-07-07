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
            options.secret,
            log_func=print,
    )


@pytest.fixture(scope='module')
def storage_pool(call, cluster):
    pool_uuid = call('POST', f'/clusters/{cluster}/storage-pools', data={'name': 'poolX', 'secret': False})
    yield pool_uuid
    call('DELETE', f'/clusters/{cluster}/storage-pools/{pool_uuid}')


@pytest.fixture(scope='module')
def volume(call, cluster, storage_pool):
    volume_uuid = call('POST', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes', data={
        'name': 'volumeX',
        'size': '2G',
    })
    yield volume_uuid
    call('DELETE', f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
    util.await_deletion(call, f'/clusters/{cluster}/storage-pools/{storage_pool}/volumes/{volume_uuid}')
