#!/usr/bin/env python

import os
import time
import signal

import restful_document
import sugar_network as client
import sugar_network_server as server


def main():
    context = client.Context.new()
    context['type'] = 'activity'
    context['title'] = 'Title %s' % time.time()
    context['description'] = 'Description'
    context['license'] = ['GPLv3+']
    context['author'] = [client.guid()]
    context.post()

    query = client.Context.find()
    for i in range(query.total):
        print query[i]['guid'], query[i]['title']

    client.Context.delete(context['guid'])


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
