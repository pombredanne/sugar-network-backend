# Copyright (C) 2010-2012, Aleksey Lim
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

# pylint: disable-msg=W0212

import os
import sys
import json
import signal
import shutil
import logging
import threading
from os.path import join, exists, basename
from gettext import gettext as _

from zeroinstall.injector import model
from zeroinstall.injector.requirements import Requirements

from zerosugar import solver
from zerosugar.config import config
from active_document import coroutine
from sugar_network import Client
from local_document import env, sugar, util, enforce


_logger = logging.getLogger('zerosugar')
_pipe = None


class Pipe(object):

    def __init__(self, pid, fd):
        self._pid = pid
        self._file = os.fdopen(fd)

    def fileno(self):
        return self._file.fileno()

    def read(self):
        event = self._file.readline()
        if not event:
            self._file.close()
            return None
        event = json.loads(event)
        phase = event.pop('phase')
        return phase, event

    def __iter__(self):
        with self._file as f:
            while True:
                coroutine.select([f.fileno()], [], [])
                event = f.readline()
                if not event:
                    break
                event = json.loads(event)
                phase = event.pop('phase')
                yield phase, event
                if phase == 'exec':
                    break

            try:
                __, status = os.waitpid(self._pid, 0)
                failure = _decode_exit_failure(status)
                if failure:
                    yield 'failure', {'error': failure}
            except OSError:
                pass


def launch(mountpoint, context, command='activity', args=None):
    return _fork(_launch, mountpoint, context, command, args)


def checkin(mountpoint, context, command='activity'):
    return _fork(_checkin, mountpoint, context, command)


def _fork(callback, *args):
    fd_r, fd_w = os.pipe()

    pid = os.fork()
    if pid:
        os.close(fd_w)
        return Pipe(pid, fd_r)

    os.close(fd_r)
    global _pipe
    _pipe = fd_w

    from sugar_network.bus import Bus
    Bus.connection = None

    # To avoid execution current thread coroutine
    thread = threading.Thread(target=callback, args=args)
    thread.start()
    thread.join()

    os.close(fd_w)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def _launch(mountpoint, context, command, args):
    _setup_logging(context)
    config.client = Client(mountpoint)

    if args is None:
        args = []

    try:
        solution = _make(context, command)
        cmd = solution.commands[0]
        args = cmd.path.split() + args

        _logger.info(_('Executing %s: %s'), solution.interface, args)
        _progress('exec')

        if command == 'activity':
            _activity_env(solution.top, os.environ)
        os.execvpe(args[0], args, os.environ)

    except Exception, error:
        util.exception(_logger)
        _progress('failure', error=str(error))
    finally:
        os._exit(0)


def _checkin(mountpoint, context, command):
    _setup_logging(context)
    config.client = Client(mountpoint)

    checkedin = []
    try:
        solution = _make(context, command)
        for sel, __, __ in solution.walk():
            dst_path = util.unique_filename(
                    env.activities_root.value, basename(sel.local_path))
            checkedin.append(dst_path)
            _logger.info(_('Checkin implementation to %r'), dst_path)
            util.cptree(sel.local_path, dst_path)
    except Exception, error:
        while checkedin:
            shutil.rmtree(checkedin.pop(), ignore_errors=True)
        util.exception(_logger)
        _progress('failure', error=str(error))
    finally:
        os._exit(0)


def _progress(phase, **kwargs):
    kwargs['phase'] = phase
    os.write(_pipe, json.dumps(kwargs))
    os.write(_pipe, '\n')


def _make(context, command):
    requirement = Requirements(context)
    requirement.command = command

    _progress('analyze', progress=-1)
    solution = solver.solve(requirement)
    enforce(solution.ready, solution.failure_reason)

    for sel, __, __ in solution.walk():
        if sel.local_path:
            continue

        enforce(sel.download_sources, \
                _('No sources to download implementation for %r context'),
                sel.interface)

        # TODO Per download progress
        _progress('download', progress=-1)

        impl = config.client.Implementation(sel.id)
        impl_path, __ = impl.get_blob_path('bundle')
        enforce(impl_path, _('Cannot download bundle'))

        dl = sel.download_sources[0]
        if dl.extract is not None:
            impl_path = join(impl_path, dl.extract)
        sel.local_path = impl_path

    return solution


def _activity_env(selection, environ):
    root = sugar.profile_path('data', selection.interface)

    for path in ['instance', 'data', 'tmp']:
        path = join(root, path)
        if not exists(path):
            os.makedirs(path)

    environ['SUGAR_BUNDLE_PATH'] = selection.local_path
    environ['SUGAR_BUNDLE_ID'] = selection.feed.context
    environ['SUGAR_BUNDLE_NAME'] = selection.feed.name
    environ['SUGAR_BUNDLE_VERSION'] = model.format_version(selection.version)
    environ['SUGAR_ACTIVITY_ROOT'] = root
    environ['PATH'] = '%s:%s' % \
            (join(selection.local_path, 'bin'), environ['PATH'])
    environ['PYTHONPATH'] = '%s:%s' % \
            (selection.local_path, environ['PYTHONPATH'])
    environ['SUGAR_LOCALEDIR'] = join(selection.local_path, 'locale')

    os.chdir(selection.local_path)


def _setup_logging(context):
    logs_dir = sugar.profile_path('logs')
    if not exists(logs_dir):
        os.makedirs(logs_dir)
    path = util.unique_filename(logs_dir, context + '.log')

    def stdfd(stream):
        if hasattr(stream, 'fileno'):
            return stream.fileno()
        else:
            # Sugar Shell wraps std streams
            return stream._stream.fileno()

    logfile = file(path, 'a+')
    os.dup2(logfile.fileno(), stdfd(sys.stdout))
    os.dup2(logfile.fileno(), stdfd(sys.stderr))
    logfile.close()

    debug = sugar.logger_level()
    if not debug:
        level = logging.WARNING
    elif debug == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)
    logging.basicConfig(level=level,
            format='%(asctime)s %(levelname)s %(name)s: %(message)s')


def _decode_exit_failure(status):
    failure = None
    if os.WIFEXITED(status):
        status = os.WEXITSTATUS(status)
        if status:
            failure = _('Exited with status %s') % status
    elif os.WIFSIGNALED(status):
        signum = os.WTERMSIG(status)
        if signum not in (signal.SIGINT, signal.SIGKILL, signal.SIGTERM):
            failure = _('Terminated by signal %s') % signum
    else:
        signum = os.WTERMSIG(status)
        failure = _('Undefined status with signal %s') % signum
    return failure
