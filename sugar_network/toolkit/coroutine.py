# Copyright (C) 2012-2014 Aleksey Lim
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

import os
import logging
from os.path import dirname, exists

import gevent
from gevent import hub
from gevent.pool import Pool as _Pool
from gevent.queue import Empty


#: Process one events loop round.
dispatch = gevent.sleep

#: Put the current coroutine to sleep for at least `seconds`.
sleep = gevent.sleep

#: Wait for the spawned events to finish.
joinall = gevent.joinall

#: Access to greenlet-local storage
this = None

hub.Hub.resolver_class = 'gevent.resolver_ares.Resolver'

_all_jobs = None
_logger = logging.getLogger('coroutine')
_wsgi_logger = logging.getLogger('wsgi')


def inject():
    from gevent import monkey

    monkey.patch_os()
    monkey.patch_time()
    monkey.patch_socket(dns=True, aggressive=True)
    monkey.patch_select(aggressive=True)
    monkey.patch_ssl()
    monkey.patch_subprocess()


def spawn(*args, **kwargs):
    return _all_jobs.spawn(*args, **kwargs)


def spawn_later(seconds, *args, **kwargs):
    return _all_jobs.spawn_later(*args, **kwargs)


def shutdown():
    _all_jobs.kill()
    return _all_jobs.join()


def reset_resolver():
    _logger.debug('Reset resolver')
    gevent.get_hub().resolver = None


def socket(*args, **kwargs):
    import gevent.socket
    return gevent.socket.socket(*args, **kwargs)


def listen_unix_socket(path, backlog=5, reuse_address=False, mode=None):
    # pylint: disable-msg=E1101
    from tempfile import NamedTemporaryFile
    import _socket

    if exists(path):
        if not reuse_address:
            raise RuntimeError('The socket address is in use')
        os.unlink(path)

    sock = socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    sock.setblocking(0)

    with NamedTemporaryFile(dir=dirname(path)) as tmp_path:
        pass
    sock.bind(tmp_path.name)
    if mode is not None:
        os.chmod(tmp_path.name, mode)
    try:
        os.rename(tmp_path.name, path)
    except Exception, error:
        sock.close()
        os.unlink(tmp_path.name)
        raise RuntimeError('Failed to create socket: %s' % error)
    sock.listen(backlog)

    def close():
        os.unlink(path)
        return orig_close()

    orig_close = sock.close
    sock.close = close

    return sock


def gethostbyname(host):
    import gevent.socket
    return gevent.socket.gethostbyname(host)


def select(rlist, wlist, xlist, timeout=None):
    import gevent.select
    return gevent.select.select(rlist, wlist, xlist, timeout)


def signal(*args, **kwargs):
    return gevent.signal(*args, **kwargs)


def fork():
    pid = os.fork()
    if pid:
        return _Child(pid)


def Server(*args, **kwargs):
    import gevent.server
    kwargs['spawn'] = spawn
    return gevent.server.StreamServer(*args, **kwargs)


def WSGIServer(*args, **kwargs):
    import gevent.wsgi

    class Server(gevent.wsgi.WSGIServer):

        http_log = kwargs.pop('http_log') if 'http_log' in kwargs else None

    class Handler(gevent.wsgi.WSGIHandler):

        def log_error(self, msg, *args):
            if args:
                msg = msg % args
            _wsgi_logger.error('%s %s', self.format_request(), msg)

        def log_request(self):
            logfile = server.http_log
            if logfile is not None:
                logfile.write(self.format_request())
                logfile.write('\n')

    kwargs['spawn'] = Pool()
    if 'handler_class' not in kwargs:
        kwargs['handler_class'] = Handler
    server = Server(*args, **kwargs)
    return server


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


class ThreadEvent(object):

    def __init__(self):
        self._async = gevent.get_hub().loop.async()

    def set(self):
        self._async.send()

    def wait(self):
        gevent.get_hub().wait(self._async)


class ThreadResult(object):

    def __init__(self):
        self._async = gevent.get_hub().loop.async()
        self._value = None

    def set(self, value):
        self._value = value
        self._async.send()

    def get(self):
        gevent.get_hub().wait(self._async)
        return self._value


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
        return self._queue.get_nowait()


class Pool(_Pool):

    def spawn(self, *args, **kwargs):
        job = self.greenlet_class(*args, **kwargs)
        job.local = _Local()
        if self is not _all_jobs:
            _all_jobs.add(job)
        self.start(job)
        return job

    def spawn_later(self, seconds, *args, **kwargs):
        job = self.greenlet_class(*args, **kwargs)
        job.local = _Local()
        if self is not _all_jobs:
            _all_jobs.add(job)
        job.start_later(seconds)
        self.add(job)
        return job

    # pylint: disable-msg=W0221
    def kill(self, *args, **kwargs):
        try:
            _Pool.kill(self, *args, **kwargs)
        except Empty:
            # Avoid useless exception on empty poll
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.kill()


class Spooler(object):
    """One-producer many-consumers events delivery.

    The delivery process supports lossless events feeding with guaranty that
    every consumer proccessed every event producer pushed.

    """

    def __init__(self):
        self._value = None
        self._waiters = 0
        self._ready = Event()
        self._notifying_done = Event()
        self._notifying_done.set()

    @property
    def waiters(self):
        return self._waiters

    def wait(self):
        self._notifying_done.wait()
        self._waiters += 1
        try:
            self._ready.wait()
            value = self._value
        finally:
            self._waiters -= 1
            if self._waiters == 0:
                self._ready.clear()
                self._notifying_done.set()
        return value

    def notify_all(self, value=None):
        while not self._notifying_done.is_set():
            self._notifying_done.wait()
        if not self._waiters:
            return
        self._notifying_done.clear()
        self._value = value
        self._ready.set()


class _Local(object):

    PROPERTY_NOT_SET = object()

    def __init__(self):
        self.attrs = set()
        self.properties = {}
        self.singletons = {}

        if hasattr(gevent.getcurrent(), 'local'):
            current = gevent.getcurrent().local
            for attr in current.attrs:
                self.attrs.add(attr)
                setattr(self, attr, getattr(current, attr))
            self.properties = current.properties
            self.singletons = current.singletons


class _LocalAccess(object):

    def __getattr__(self, name):
        local = gevent.getcurrent().local
        value = getattr(local, name)
        if value is _Local.PROPERTY_NOT_SET:
            value = local.properties[name]()
            setattr(local, name, value)
        return value

    def __setattr__(self, name, value):
        local = gevent.getcurrent().local
        local.attrs.add(name)
        if value is None and name in local.properties:
            value = _Local.PROPERTY_NOT_SET
        setattr(local, name, value)

    def add_property(self, name, getter):
        local = gevent.getcurrent().local
        local.properties[name] = getter
        setattr(local, name, _Local.PROPERTY_NOT_SET)

    def reset_property(self, name):
        local = gevent.getcurrent().local
        setattr(local, name, _Local.PROPERTY_NOT_SET)

    def add_property_singleton(self, name, cls, *args, **kwargs):
        local = gevent.getcurrent().local
        local.properties[name] = \
                lambda: local.singletons.get(name) or cls(*args, **kwargs)
        setattr(local, name, _Local.PROPERTY_NOT_SET)


class _Child(object):

    def __init__(self, pid):
        self.pid = pid
        self._watcher = None

    def watch(self, cb, *args, **kwargs):
        if self._watcher is not None:
            raise RuntimeError('Watching already started')
        loop = gevent.get_hub().loop
        loop.install_sigchld()
        self._watcher = loop.child(self.pid)
        self._watcher.start(self.__sigchld_cb, cb, args, kwargs)

    def wait(self):
        result = AsyncResult()
        self.watch(result.set)
        return result.get()

    def __sigchld_cb(self, cb, args, kwargs):
        self._watcher.stop()
        status = self._watcher.rstatus
        if os.WIFSIGNALED(status):
            returncode = -os.WTERMSIG(status)
        else:
            returncode = os.WEXITSTATUS(status)
        cb(returncode, *args, **kwargs)


def _print_exception(context, klass, value, tb):
    self = hub.get_hub()
    if issubclass(klass, self.NOT_ERROR + self.SYSTEM_ERROR):
        return

    import errno
    import traceback

    tb_repr = '\n'.join([i.rstrip()
            for i in traceback.format_exception(klass, value, tb)][:-1])
    del tb

    context_repr = None
    if context is None:
        context = 'Undefined'
    elif not isinstance(context, basestring):
        if isinstance(context, dict) and 'PATH_INFO' in context:
            context_repr = context['PATH_INFO']
            if 'QUERY_STRING' in context:
                context_repr += '?' + context['QUERY_STRING']
        try:
            context = self.format_context(context)
        except Exception:
            context = repr(context)
    error = 'Failed from %r context: %s' % \
            (context_repr or context[:40] + '..', value)

    logging_level = logging.getLogger().level
    if logging_level > logging.DEBUG or \
            isinstance(value, IOError) and value.errno == errno.EPIPE:
        _logger.error(error)
    elif logging_level == logging.DEBUG:
        _logger.error('\n'.join([error, tb_repr]))
    else:
        _logger.error('\n'.join([error, context, tb_repr]))


_all_jobs = Pool()
hub.get_hub().print_exception = _print_exception
gevent.getcurrent().local = gevent.get_hub().local = _Local()
this = _LocalAccess()
