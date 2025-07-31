def list_ids(call, path):
    return [
        item['id']
        for item
        in call('GET', path)
    ]
