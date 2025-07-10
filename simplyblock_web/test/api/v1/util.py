def list_ids(call, type):
    return [
        obj['uuid']
        for obj
        in call('GET', f'/{type}/')
    ]
