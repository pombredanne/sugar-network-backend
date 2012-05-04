#!/usr/bin/env python

import time

import sugar_network


def main():
    client = sugar_network.Client('~')

    guids = [None] * 3
    titles = ['Title1', 'Title2', 'Title3']
    image_url = 'http://sugarlabs.org/assets/logo_black_01.png'

    print '-- Delete objects'
    for i in client.Context.cursor():
        client.Context.delete(i['guid'])

    def context_new(title):
        context = client.Context()
        context['type'] = 'activity'
        context['title'] = title
        context['summary'] = 'Description'
        context['description'] = 'Description'
        context.post()
        return context['guid']

    print '-- Create new objects'
    guids[0] = context_new(titles[0])
    assert guids[0]
    guids[1] = context_new(titles[1])
    assert guids[1] and guids[1] != guids[0]
    guids[2] = context_new(titles[2])
    assert guids[2] and guids[2] != guids[1] and guids[2] != guids[0]

    print '-- Browse using iterators'
    for i, obj in enumerate(client.Context.cursor()):
        assert i == obj.offset
        assert obj['guid'] == guids[i]

    print '-- Browse by offset'
    query = client.Context.cursor()
    for i in range(query.total):
        assert query[i]['guid'] == guids[i]

    print '-- Get objects directly'
    assert client.Context(guids[0], reply=['title'])['title'] == titles[0]
    assert client.Context(guids[1], reply=['title'])['title'] == titles[1]
    assert client.Context(guids[2], reply=['title'])['title'] == titles[2]

    print '-- Set BLOB property by string'
    client.Context(guids[1]).set_blob('icon', 'string')

    print '-- Set BLOB properties by url'
    client.Context(guids[2]).set_blob_by_url('icon', image_url)

    print '-- Get BLOB property'
    assert client.Context(guids[1]).get_blob('icon').read() == 'string'

    print '-- Query by property value'
    for obj in client.Context.cursor(title='Title2', reply=['guid', 'title']):
        assert obj['guid'] == guids[1]
        assert obj['title'] == titles[1]

    # Wait until server will update index,
    # fulltext search does not work for cahced changes
    time.sleep(3)

    print '-- Full text search query'
    query = client.Context.cursor('Title1 OR Title3', reply=['guid', 'title'])
    assert query.total == 2

    assert sorted([(guids[0], titles[0]), (guids[2], titles[2])]) == \
            sorted([(i['guid'], i['title']) for i in query])


if __name__ == '__main__':
    import os
    from local_document import env

    os.system('sugar-network-service -DD start ' \
              '--local-root=tmp ' \
              '--activities-root=tmp/Activities ' \
              '--api-url=http://localhost:8000')
    try:
        env.local_root.value = 'tmp'
        main()
    finally:
        os.system('sugar-network-service --local-root=tmp stop')
