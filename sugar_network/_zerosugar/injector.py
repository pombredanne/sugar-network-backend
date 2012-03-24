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

import os
import sys
import errno
import signal
import logging
from os.path import join, exists
from gettext import gettext as _

import dbus

from zeroinstall.injector import model
from zeroinstall.injector.requirements import Requirements

from sugar_network import sugar
from sugar_network._zerosugar import solver
from sugar_network.util import enforce


def launch(context, command='activity', jobject=None, args=None):
    _setup_logging(context)

    if args is None:
        args = []

    if command == 'activity':
        launcher = dbus.Interface(
                dbus.SessionBus().get_object(
                    'org.sugarlabs.shell.Launch',
                    '/org/sugarlabs/shell/Launch'),
                'org.sugarlabs.shell.Launch')
        seqno = launcher.Start(context, jobject or '')

        args.extend(['-a', str(seqno), '-b', context])
        if jobject:
            args.extend(['-o', jobject])

        try:
            _lauch(context, command, args,
                    lambda op, * args: getattr(launcher, op)(seqno, *args))
        except Exception:
            launcher.Failure(seqno)
            raise
        finally:
            launcher.Stop(seqno)
    else:
        _lauch(context, command, args, lambda * args: None)


def _lauch(context, command, args, launcher):
    requirement = Requirements(context)
    requirement.command = command

    launcher('Progress', 'analyze', -1)
    solution = solver.solve(requirement)
    enforce(solution.ready, solution.failure_reason)

    for __ in _download(solution):
        launcher('Progress', 'download', -1)

    command = solution.commands[0]
    args = command.path.split() + args
    if command.name == 'activity':
        _activity_env(solution.top, os.environ)
        os.chdir(solution.top.local_path)
    logging.info(_('Executing %s: %s'), solution.interface, args)

    launcher('Progress', 'exec', -1)
    pid = os.fork()
    if not pid:
        os.execvpe(args[0], args, os.environ)
        sys.exit(1)

    __, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        status = os.WEXITSTATUS(status)
        message = _('Exited with status %s') % status
        if status:
            launcher('Failure')
    elif os.WIFSIGNALED(status):
        signum = os.WTERMSIG(status)
        message = _('Terminated by signal %s') % signum
        if signum not in (signal.SIGINT, signal.SIGKILL, signal.SIGTERM):
            launcher('Failure')
    else:
        signum = os.WTERMSIG(status)
        message = _('Undefined status with signal %s') % signum
        launcher('Failure')
    logging.info(_('Exited %s: %s'), context, message)


def _download(solution):
    for sel, __, __ in solution.walk():
        enforce(sel.download_sources, \
                _('No sources to download implementation for "%s" context'),
                sel.interface)
        yield
        sel.download()


def _setup_logging(filename):
    logs_dir = sugar.profile_path('logs')
    if not exists(logs_dir):
        os.makedirs(logs_dir)

    log_no = 1
    while True:
        path = join(logs_dir, '%s-%s.log' % (filename, log_no))
        try:
            out_fd = os.open(path, os.O_EXCL | os.O_CREAT | os.O_WRONLY, 0644)
            break
        except OSError, error:
            if error.errno == errno.EEXIST:
                log_no += 1
            elif error.errno == errno.ENOSPC:
                # not the end of the world; let's try to keep going.
                return os.open('/dev/null', 'w')
            else:
                raise

    in_fd = os.open('/dev/null', os.O_RDONLY)
    os.dup2(in_fd, sys.stdin.fileno())
    os.close(in_fd)

    os.dup2(out_fd, sys.stdout.fileno())
    os.dup2(out_fd, sys.stderr.fileno())
    os.close(out_fd)


def _activity_env(selection, env):
    root = sugar.profile_path('data', selection.interface)

    for path in ['instance', 'data', 'tmp']:
        path = join(root, path)
        if not exists(path):
            os.makedirs(path)

    env['SUGAR_BUNDLE_PATH'] = selection.local_path
    env['SUGAR_BUNDLE_ID'] = selection.feed.url
    env['SUGAR_BUNDLE_NAME'] = selection.feed.name
    env['SUGAR_BUNDLE_VERSION'] = model.format_version(selection.version)
    env['SUGAR_ACTIVITY_ROOT'] = root
    env['PATH'] = '%s:%s' % (join(selection.local_path, 'bin'), env['PATH'])
    env['PYTHONPATH'] = '%s:%s' % (selection.local_path, env['PYTHONPATH'])
    env['SUGAR_LOCALEDIR'] = join(selection.local_path, 'locale')
