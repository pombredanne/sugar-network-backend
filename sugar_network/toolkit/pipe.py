# Copyright (C) 2010-2013 Aleksey Lim
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

import os
import sys
import json
import struct
import signal
import logging
import threading
from os.path import exists, dirname, basename

from sugar_network import toolkit
from sugar_network.toolkit import coroutine


_logger = logging.getLogger('pipe')
_pipe = None
_trace = None


def feedback(state, **event):
    if _pipe is None:
        return
    event['state'] = state
    event = json.dumps(event)
    os.write(_pipe, struct.pack('i', len(event)))
    os.write(_pipe, event)


def trace(message, *args):
    global _trace
    if _trace is None:
        _trace = []
    if args:
        message = message % args
    _logger.debug(message)
    _trace.append(message)


def fork(callback, log_path=None, session=None, **kwargs):
    fd_r, fd_w = os.pipe()

    pid = os.fork()
    if pid:
        os.close(fd_w)
        _logger.debug('Fork %s%r with %s pid', callback, kwargs, pid)
        return _Pipe(pid, fd_r)

    os.close(fd_r)
    global _pipe
    _pipe = fd_w

    def thread_func():
        environ = {}
        if log_path:
            environ['log_path'] = _setup_logging(log_path)
        feedback('fork', session=session, environ=environ)
        try:
            callback(**kwargs)
        except Exception, error:
            feedback('failure', error_type=type(error).__name__,
                    error=str(error), environ={'trace': _trace})
            _logger.exception('%r(%r) failed', callback, kwargs)

    if session is None:
        session = {}
    # Avoid a mess with current thread coroutines
    thread = threading.Thread(target=thread_func)
    thread.start()
    thread.join()

    os.close(fd_w)
    sys.stdout.flush()
    sys.stderr.flush()
    # pylint: disable-msg=W0212
    os._exit(0)


class _Pipe(object):

    def __init__(self, pid, fd):
        self._pid = pid
        self._fd = fd
        self._session = {}
        self._environ = {}

    def fileno(self):
        return self._fd

    def read(self):
        if self._fd is None:
            return None

        event = None
        failed = False

        event_length = os.read(self._fd, struct.calcsize('i'))
        if event_length:
            event_length = struct.unpack('i', event_length)[0]
            event = json.loads(os.read(self._fd, event_length))
            if 'session' in event:
                self._session.update(event.pop('session'))
            if 'environ' in event:
                self._environ.update(event.pop('environ'))
            failed = (event['state'] == 'failure')

        if event is None or failed:
            status = 0
            try:
                __, status = os.waitpid(self._pid, 0)
            except OSError:
                pass
            if event is None:
                failure = _decode_exit_failure(status)
                if failure:
                    _logger.debug('Process %s failed: %s', self._pid, failure)
                    event = {'state': 'failure', 'error': failure}
                    failed = True
                else:
                    _logger.debug('Process %s successfully exited', self._pid)
                    event = {'state': 'exit'}
            os.close(self._fd)
            self._fd = None

        if failed:
            event.update(self._environ)
        event.update(self._session)

        return event

    def __iter__(self):
        try:
            while self._fd is not None:
                coroutine.select([self._fd], [], [])
                event = self.read()
                if event is None:
                    break
                yield event
        finally:
            if self._fd is not None:
                _logger.debug('Kill %s process', self._pid)
                os.kill(self._pid, signal.SIGTERM)
                while self.read() is not None:
                    pass


def _decode_exit_failure(status):
    failure = None
    if os.WIFEXITED(status):
        status = os.WEXITSTATUS(status)
        if status:
            failure = 'Exited with status %s' % status
    elif os.WIFSIGNALED(status):
        signum = os.WTERMSIG(status)
        if signum not in (signal.SIGINT, signal.SIGKILL, signal.SIGTERM):
            failure = 'Terminated by signal %s' % signum
    else:
        signum = os.WTERMSIG(status)
        failure = 'Undefined status with signal %s' % signum
    return failure


def _setup_logging(path):
    log_dir = dirname(path)
    if not exists(log_dir):
        os.makedirs(log_dir)
    path = toolkit.unique_filename(log_dir, basename(path) + '.log')

    logfile = file(path, 'a+')
    os.dup2(logfile.fileno(), sys.stdout.fileno())
    os.dup2(logfile.fileno(), sys.stderr.fileno())
    logfile.close()

    for handler in logging.getLogger().handlers:
        logging.getLogger().removeHandler(handler)
    logging.basicConfig(level=logging.getLogger().level,
            format='%(asctime)s %(levelname)s %(name)s: %(message)s')

    return path
