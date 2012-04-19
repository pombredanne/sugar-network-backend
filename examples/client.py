#!/usr/bin/env python

import time
import urllib2
from cStringIO import StringIO

import sugar_network


def main():
    client = sugar_network.Client()

    guids = [None] * 3
    titles = ['Title1', 'Title2', 'Title3']
    image_url = 'http://sugarlabs.org/assets/logo_black_01.png'

    print '-- Delete objects'
    for i in client.Context.find():
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
    for i, obj in enumerate(client.Context.find()):
        assert i == obj.offset
        assert obj['guid'] == guids[i]

    print '-- Browse by offset'
    query = client.Context.find()
    for i in range(query.total):
        assert query[i]['guid'] == guids[i]

    print '-- Get objects directly'
    assert client.Context(guids[0])['title'] == titles[0]
    assert client.Context(guids[1])['title'] == titles[1]
    assert client.Context(guids[2])['title'] == titles[2]

    print '-- Set BLOB property by string'
    client.Context(guids[1]).blobs['icon'] = 'string'

    print '-- Set BLOB properties by url'
    client.Context(guids[2]).blobs['icon'].url = image_url

    print '-- Get BLOB property by portions'
    stream = StringIO()
    for chunk in client.Context(guids[2]).blobs['icon'].iter_content():
        stream.write(chunk)
    assert stream.getvalue() == urllib2.urlopen(image_url).read()

    print '-- Get BLOB property by string'
    assert client.Context(guids[1]).blobs['icon'].content == 'string'

    print '-- Query by property value'
    for obj in client.Context.find(title='Title2'):
        assert obj['guid'] == guids[1]
        assert obj['title'] == titles[1]

    # Wait until server will update index,
    # fulltext search does not work for cahced changes
    time.sleep(3)

    print '-- Full text search query'
    query = client.Context.find(query='Title1 OR Title3')
    assert query.total == 2

    assert sorted([(guids[0], titles[0]), (guids[2], titles[2])]) == \
            sorted([(i['guid'], i['title']) for i in query])


if __name__ == '__main__':
    import logging
    import gevent
    from local_document import env, ipc_server, cp

    logging.basicConfig(level=logging.DEBUG, filename='log')

    env.ipc_root.value = 'tmp/ipc'
    env.api_url.value = 'http://localhost:8000'
    env.local_data_root.value = 'tmp/db'

    def server():
        srv = ipc_server.Server(cp.CommandsProcessor())
        srv.serve_forever()

    gevent.spawn(server)
    gevent.sleep()

    main()
