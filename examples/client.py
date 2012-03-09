#!/usr/bin/env python

import os
import signal

import restful_document
import sugar_network as client
import sugar_network_server as server


def main():

    def context_new(title):
        context = client.Context.new()
        context['type'] = 'activity'
        context['title'] = title
        context['description'] = 'Description'
        context['license'] = ['GPLv3+']
        context['author'] = [client.guid()]
        context.post()

    context_new('#1')
    context_new('#2')
    context_new('#3')

    query = client.Context.find()

    # Browse using iterators
    for i in query:
        print i.offset, i['guid'], i['title']

    # Browse by offset
    for i in range(query.total):
        print i, query[i]['guid'], query[i]['title']

    query.offset = -1
    for i in query:
        client.Context.delete(i['guid'])


if __name__ == '__main__':
    server.debug.value = 3
    server.data_root.value = 'tmp/db'
    server.stats_root.value = 'tmp/stats'
    server.logdir.value = 'tmp/log'
    server_pid = restful_document.fork(server.resources())

    client.api_url.value = \
            'http://%s:%s' % (server.host.value, server.port.value)
    try:
        main()
    finally:
        os.kill(server_pid, signal.SIGINT)
        os.waitpid(server_pid, 0)
