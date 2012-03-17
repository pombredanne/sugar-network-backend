#!/usr/bin/env python

import os
import time
import signal

import restful_document
import sugar_network as client
import sugar_network_server as server


def main():

    def context_new(title):
        context = client.Context()
        context['type'] = 'activity'
        context['title'] = title
        context['summary'] = 'Description'
        context['description'] = 'Description'
        context.post()

    print '-- Create new objects'
    context_new('Title1')
    context_new('Title2')
    context_new('Title3')

    print '-- Browse using iterators'
    for i in client.Context.find():
        print i.offset, i['guid'], i['title']

    print '-- Browse by offset'
    query = client.Context.find()
    for i in range(query.total):
        print i, query[i]['guid'], query[i]['title']

    print '-- Get objects directly'
    print client.Context(query[0]['guid'])['title']
    print client.Context(title='Title2')['title']
    print client.Context(title='Title3')['title']

    print '-- Query by property value'
    for i in client.Context.find(title='Title2'):
        print i.offset, i['guid'], i['title']

    # Wait until server will update index,
    # fulltext search does not work for cahced changes
    time.sleep(3)

    print '-- Full text search query'
    for i in client.Context.find(query='Title1 OR Title3'):
        print i.offset, i['guid'], i['title']

    print '-- Delete objects'
    for i in client.Context.find():
        client.Context.delete(i['guid'])


if __name__ == '__main__':
    server.debug.value = 3
    server.data_root.value = 'tmp/db'
    server.stats_root.value = 'tmp/stats'
    server.logdir.value = 'tmp/log'
    server.index_flush_threshold.value = 1
    server_pid = restful_document.fork(server.resources())

    client.api_url.value = \
            'http://%s:%s' % (server.host.value, server.port.value)
    try:
        main()
    finally:
        os.kill(server_pid, signal.SIGINT)
        os.waitpid(server_pid, 0)
