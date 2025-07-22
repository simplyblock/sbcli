def test_base(call):
    assert call('GET', '/') == 'Live'
