# Copyright (C) 2012 Aleksey Lim
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Wrap coroutine related procedures.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/coroutine.py$
$Date: 2012-06-27$

"""
# pylint: disable-msg=W0621

import logging

import gevent
import gevent.pool
import gevent.hub


#: Process one events loop round.
dispatch = gevent.sleep

#: Put the current coroutine to sleep for at least `seconds`.
sleep = gevent.sleep

#: Wait for the spawned events to finish.
joinall = gevent.joinall

# Avoid using async DNS resolver, c-ares fails on XO-1 on Fedora-14
# with ARES_ECONNREFUSED error; in any case, resolving is not an issue
# for Sugar Network workflow
gevent.hub.Hub.resolver_class = ['gevent.socket.BlockingResolver']

_group = gevent.pool.Group()
_logger = logging.getLogger('coroutine')


def spawn(callback, *args):
    return _group.spawn(callback, *args)


def shutdown():
    _group.kill()
    return _group.join()


def socket(*args, **kwargs):
    import gevent.socket
    return gevent.socket.socket(*args, **kwargs)


def select(rlist, wlist, xlist, timeout=None):
    import gevent.select
    return gevent.select.select(rlist, wlist, xlist, timeout)


def Server(*args, **kwargs):
    import gevent.server
    kwargs['spawn'] = spawn
    return gevent.server.StreamServer(*args, **kwargs)


def WSGIServer(*args, **kwargs):
    import gevent.wsgi

    class WSGIHandler(gevent.wsgi.WSGIHandler):

        def log_error(self, msg, *args):
            _logger.error(msg, *args)

        def log_request(self):
            pass

    kwargs['spawn'] = spawn
    if 'handler_class' not in kwargs:
        kwargs['handler_class'] = WSGIHandler
    return gevent.wsgi.WSGIServer(*args, **kwargs)


def Event():
    import gevent.event
    return gevent.event.Event()


def Queue(*args, **kwargs):
    import gevent.queue
    return gevent.queue.Queue(*args, **kwargs)


def Lock(*args, **kwargs):
    import gevent.coros
    return gevent.coros.Semaphore(*args, **kwargs)


def RLock(*args, **kwargs):
    import gevent.coros
    return gevent.coros.RLock(*args, **kwargs)


class Condition(object):

    def __init__(self):
        self._event = Event()
        self._value = None

    def wait(self, timeout=None):
        self._event.wait(timeout)
        return self._value

    def notify(self, value=None):
        self._value = value
        self._event.set()
        self._event.clear()


class AsyncCondition(object):

    def __init__(self):
        self._async = gevent.get_hub().loop.async()

    def wait(self):
        gevent.get_hub().wait(self._async)

    def notify(self):
        self._async.send()
