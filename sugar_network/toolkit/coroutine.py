# Copyright (C) 2012-2013 Aleksey Lim
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

"""Wrap coroutine related procedures."""

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

_group = gevent.pool.Group()
_logger = logging.getLogger('coroutine')
_wsgi_logger = logging.getLogger('wsgi')


def spawn(callback, *args):
    return _group.spawn(callback, *args)


def shutdown():
    _group.kill()
    return _group.join()


def reset_resolver():
    gevent.get_hub().resolver = None


def socket(*args, **kwargs):
    import gevent.socket
    return gevent.socket.socket(*args, **kwargs)


def gethostbyname(host):
    import gevent.socket
    return gevent.socket.gethostbyname(host)


def select(rlist, wlist, xlist, timeout=None):
    import gevent.select
    return gevent.select.select(rlist, wlist, xlist, timeout)


def signal(*args, **kwargs):
    return gevent.signal(*args, **kwargs)


def Server(*args, **kwargs):
    import gevent.server
    kwargs['spawn'] = spawn
    return gevent.server.StreamServer(*args, **kwargs)


def WSGIServer(*args, **kwargs):
    import gevent.wsgi

    class WSGIHandler(gevent.wsgi.WSGIHandler):

        def log_error(self, msg, *args):
            if args:
                msg = msg % args
            _wsgi_logger.error('%s %s', self.format_request(), msg)

        def log_request(self):
            _wsgi_logger.debug('%s', self.format_request())

    kwargs['spawn'] = Pool()
    if 'handler_class' not in kwargs:
        if logging.getLogger().level >= logging.DEBUG:
            WSGIHandler.log_request = lambda * args: None
        kwargs['handler_class'] = WSGIHandler
    return gevent.wsgi.WSGIServer(*args, **kwargs)


def Event():
    import gevent.event
    return gevent.event.Event()


def AsyncResult():
    import gevent.event
    return gevent.event.AsyncResult()


def Queue(*args, **kwargs):
    import gevent.queue
    return gevent.queue.Queue(*args, **kwargs)


def Lock(*args, **kwargs):
    import gevent.lock
    return gevent.lock.Semaphore(*args, **kwargs)


def RLock(*args, **kwargs):
    import gevent.lock
    return gevent.lock.RLock(*args, **kwargs)


class AsyncEvent(object):

    def __init__(self):
        self._async = gevent.get_hub().loop.async()

    def wait(self):
        gevent.get_hub().wait(self._async)

    def send(self):
        self._async.send()


class Empty(Exception):
    pass


class AsyncQueue(object):

    def __init__(self):
        self._queue = self._new_queue()
        self._async = gevent.get_hub().loop.async()
        self._aborted = False

    def put(self, *args, **kwargs):
        self._put(args, kwargs)
        self._async.send()

    def get(self):
        self._aborted = False
        while True:
            try:
                return self._get()
            except Empty:
                gevent.get_hub().wait(self._async)
                if self._aborted:
                    self._aborted = False
                    raise

    def abort(self):
        self._aborted = True
        self._async.send()

    def __iter__(self):
        while True:
            try:
                yield self.get()
            except Empty:
                break

    def __getattr__(self, name):
        return getattr(self._queue, name)

    def _new_queue(self):
        from Queue import Queue
        return Queue()

    def _put(self, args, kwargs):
        self._queue.put(*args, **kwargs)

    def _get(self):
        from Queue import Empty as empty
        try:
            return self._queue.get_nowait()
        except empty:
            raise Empty()


class Pool(gevent.pool.Pool):

    def spawn(self, *args, **kwargs):
        job = gevent.pool.Pool.spawn(self, *args, **kwargs)
        _group.add(job)
        return job

    # pylint: disable-msg=W0221
    def kill(self, *args, **kwargs):
        from gevent.queue import Empty
        try:
            gevent.pool.Pool.kill(self, *args, **kwargs)
        except Empty:
            # Avoid useless exception on empty poll
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.kill()


def _print_exception(context, klass, value, tb):
    self = gevent.hub.get_hub()
    if issubclass(klass, self.NOT_ERROR + self.SYSTEM_ERROR):
        return

    import traceback
    tb_repr = '\n'.join([i.rstrip()
            for i in traceback.format_exception(klass, value, tb)][:-1])
    del tb

    context_repr = None
    if context is None:
        context = 'Undefined'
    elif not isinstance(context, basestring):
        if isinstance(context, dict) and 'PATH_INFO' in context:
            context_repr = '%s%s' % \
                    (context['PATH_INFO'], context.get('QUERY_STRING') or '')
        try:
            context = self.format_context(context)
        except Exception:
            context = repr(context)
    error = 'Failed from %r context: %s' % \
            (context_repr or context[:40] + '..', value)

    logging_level = logging.getLogger().level
    if logging_level > logging.DEBUG:
        _logger.error(error)
    elif logging_level == logging.DEBUG:
        _logger.error('\n'.join([error, tb_repr]))
    else:
        _logger.error('\n'.join([error, context, tb_repr]))


gevent.hub.get_hub().print_exception = _print_exception
