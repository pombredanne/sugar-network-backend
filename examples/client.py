#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import signal
import logging
from os.path import exists

import sugar_network as sn
import sugar_network_server as server
from gevent.wsgi import WSGIServer


def main():
    context = sn.Context.new()
    context['type'] = 'activity'
    context['title'] = 'Title %s' % time.time()
    context['description'] = 'Description'
    context['license'] = ['GPLv3+']
    context['author'] = [sn.guid()]
    context.post()

    query = sn.Context.find()
    total = query.total
    for i in range(query.total):
        print query[i]['guid'], query[i]['title']

    sn.Context.delete(context['guid'])


def fork_server(port):
    child_pid = os.fork()
    if child_pid:
        time.sleep(3)
        return child_pid

    if not exists('tmp'):
        os.makedirs('tmp')
    logging.basicConfig(level=logging.DEBUG, filename='tmp/log')

    server.debug.value = 3
    server.data_root.value = 'tmp/db'
    server.stats_root.value = 'tmp/stats'

    node = server.Master(server.resources())
    httpd = WSGIServer(('localhost', port), server.Router(node))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.stop()
        node.close()
    os._exit(0)


if __name__ == '__main__':
    server_pid = fork_server(8888)
    sn.api_url.value = 'http://localhost:8888'
    try:
        main()
    finally:
        os.kill(server_pid, signal.SIGINT)
        os.waitpid(server_pid, 0)
