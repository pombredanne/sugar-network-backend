#!/usr/bin/env python

import os
import time
import signal
import urllib2
from cStringIO import StringIO

import restful_document
import sugar_network as client
import sugar_network_server as server
from sugar_network import Context


def main():
    guids = [None] * 3
    titles = ['Title1', 'Title2', 'Title3']
    image_url = 'http://sugarlabs.org/assets/logo_black_01.png'

    print '-- Delete objects'
    for i in Context.find():
        Context.delete(i['guid'])

    def context_new(title):
        context = Context()
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
    for i, obj in enumerate(Context.find()):
        assert i == obj.offset
        assert obj['guid'] == guids[i]

    print '-- Browse by offset'
    query = Context.find()
    for i in range(query.total):
        assert query[i]['guid'] == guids[i]

    print '-- Get objects directly'
    assert Context(guids[0])['title'] == titles[0]
    assert Context(guids[1])['title'] == titles[1]
    assert Context(guids[2])['title'] == titles[2]

    print '-- Set BLOB property by stream'
    Context(guids[0]).blobs['icon'] = StringIO('stream')

    print '-- Set BLOB property by string'
    Context(guids[1]).blobs['icon'] = 'string'

    print '-- Set BLOB properties by url'
    Context(guids[2]).blobs['icon'].url = image_url

    print '-- Get BLOB property by portions'
    stream = StringIO()
    for chunk in Context(guids[2]).blobs['icon'].iter_content():
        stream.write(chunk)
    assert stream.getvalue() == urllib2.urlopen(image_url).read()

    print '-- Get BLOB property by string'
    assert Context(guids[1]).blobs['icon'].content == 'string'

    print '-- Query by property value'
    for obj in Context.find(title='Title2'):
        assert obj['guid'] == guids[1]
        assert obj['title'] == titles[1]

    print '-- Set property that will be treated differently for each requester'
    assert not Context(guids[0])['vote']
    context = Context(guids[0])
    context['vote'] = True
    context.post()
    assert Context(guids[0])['vote']

    # Wait until server will update index,
    # fulltext search does not work for cahced changes
    time.sleep(3)

    print '-- Full text search query'
    query = Context.find(query='Title1 OR Title3')
    assert query.total == 2
    assert query[0]['guid'] == guids[0]
    assert query[0]['title'] == titles[0]
    assert query[1]['guid'] == guids[2]
    assert query[1]['title'] == titles[2]


if __name__ == '__main__':
    server.debug.value = 3
    server.data_root.value = 'tmp/db'
    server.stats_root.value = 'tmp/stats'
    server.logdir.value = 'tmp/log'
    server.index_flush_threshold.value = 1
    server_pid = restful_document.fork(server.resources)

    client.api_url.value = \
            'http://%s:%s' % (server.host.value, server.port.value)
    client.cachedir.value = 'tmp/cache'

    try:
        main()
    finally:
        os.kill(server_pid, signal.SIGINT)
        os.waitpid(server_pid, 0)
