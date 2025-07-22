def test_list(call, cluster):
    call('GET', '/clusters/')


def test_get(call, cluster):
    call('GET', f'/clusters/{cluster}/')


def test_update(call, cluster):
    name = 'cluster_name'
    call('PUT', f'/clusters/{cluster}/', data={'name': name})
    assert call('GET', f'/clusters/{cluster}/')['name'] == name
