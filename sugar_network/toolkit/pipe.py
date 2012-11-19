# Copyright (C) 2010-2012 Aleksey Lim
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
import struct
import signal
import logging
import threading
import cPickle as pickle
from os.path import exists

from sugar_network import sugar
from active_toolkit import coroutine, util


_logger = logging.getLogger('pipe')
_pipe = None


def feedback(state, **event):
    if _pipe is None:
        return
    event['state'] = state
    event = pickle.dumps(event)
    os.write(_pipe, struct.pack('i', len(event)))
    os.write(_pipe, event)


def fork(callback, logname=None, session=None, **kwargs):
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
        if logname:
            session['log_path'] = _setup_logging(logname)
        feedback('fork', session=session)
        try:
            callback(**kwargs)
        except Exception, error:
            util.exception(_logger)
            feedback('failure', error=str(error))

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
        self._file = os.fdopen(fd)
        self._session = {}

    def fileno(self):
        return None if self._file is None else self._file.fileno()

    def read(self):
        if self._file is None:
            return None

        event_length = self._file.read(struct.calcsize('i'))
        if event_length:
            event_length = struct.unpack('i', event_length)[0]
            event = pickle.loads(self._file.read(event_length))
            if 'session' in event:
                self._session.update(event.pop('session') or {})
            event.update(self._session)
            return event

        status = 0
        try:
            __, status = os.waitpid(self._pid, 0)
        except OSError:
            pass
        failure = _decode_exit_failure(status)
        if failure:
            _logger.debug('Process %s failed: %s', self._pid, failure)
            event = {'state': 'failure', 'error': failure}
            event.update(self._session)
            return event
        else:
            _logger.debug('Process %s successfully exited', self._pid)
            self._file.close()
            self._file = None
            return None

    def __iter__(self):
        if self._file is None:
            return

        try:
            while True:
                coroutine.select([self._file.fileno()], [], [])
                event = self.read()
                if event is None:
                    break
                yield event
        finally:
            if self._file is not None:
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


def _setup_logging(context):
    log_dir = sugar.profile_path('logs')
    if not exists(log_dir):
        os.makedirs(log_dir)
    path = util.unique_filename(log_dir, context + '.log')

    logfile = file(path, 'a+')
    os.dup2(logfile.fileno(), sys.stdout.fileno())
    os.dup2(logfile.fileno(), sys.stderr.fileno())
    logfile.close()

    logging.basicConfig(level=logging.getLogger().level,
            format='%(asctime)s %(levelname)s %(name)s: %(message)s')

    return path
