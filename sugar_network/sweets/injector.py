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
import logging
from os.path import join, exists
from gettext import gettext as _

from zeroinstall.injector.requirements import Requirements

from sugar_network import sugar
from sugar_network.sweets import solver
from sugar_network.util import enforce


def launch(context, command='activity', args=None):
    in_fd = os.open('/dev/null', os.O_RDONLY)
    os.dup2(in_fd, sys.stdin.fileno())
    os.close(in_fd)

    out_fd = _log_path(context)
    os.dup2(out_fd, sys.stdout.fileno())
    os.dup2(out_fd, sys.stderr.fileno())
    os.close(out_fd)

    requirement = Requirements(context)
    requirement.command = command

    solution = solver.solve(requirement)
    enforce(solution.ready, solution.failure_reason)
    make(solution)

    pid = execute(solution, args)
    __, condition = os.waitpid(pid, 0)

    if os.WIFEXITED(condition):
        status = os.WEXITSTATUS(condition)
        message = _('exited with status %s') % status
    elif os.WIFSIGNALED(condition):
        signum = os.WTERMSIG(condition)
        message = _('terminated by signal %s') % signum
    else:
        signum = os.WTERMSIG(condition)
        message = _('undefined status with signal %s') % signum
    logging.info(_('Exited %s: %s'), context, message)


def make(solution):
    for sel, __, __ in solution.walk():
        enforce(sel.download_sources, \
                _('No sources to download implementation for "%s" context'),
                sel.context)
        sel.download()


def execute(solution, args):
    selection = solution.top
    command = solution.commands[0]
    args = command.path.split() + (args or [])
    args.extend(['-a', sugar.uuid_new(), '-b', solution.context])

    if command.name == 'activity':
        _activity_env(selection, os.environ)
        os.chdir(selection.local_path)

    logging.info(_('Executing %s: %s'), solution.context, args)

    pid = os.fork()
    if not pid:
        os.execvpe(args[0], args, os.environ)
        sys.exit(1)

    return pid


def _log_path(context):
    logs_dir = sugar.profile_path('logs')
    if not exists(logs_dir):
        os.makedirs(logs_dir)

    log_no = 1
    while True:
        path = join(logs_dir, '%s-%s.log' % (context, log_no))
        try:
            return os.open(path, os.O_EXCL | os.O_CREAT | os.O_WRONLY, 0644)
        except OSError, error:
            if error.errno == errno.EEXIST:
                log_no += 1
            elif error.errno == errno.ENOSPC:
                # not the end of the world; let's try to keep going.
                return os.open('/dev/null', 'w')
            else:
                raise


def _activity_env(selection, env):
    root = sugar.profile_path('data', selection.context)

    for path in ['instance', 'data', 'tmp']:
        path = join(root, path)
        if not exists(path):
            os.makedirs(path)

    env['SUGAR_BUNDLE_PATH'] = selection.local_path
    env['SUGAR_BUNDLE_ID'] = selection.context
    env['SUGAR_ACTIVITY_ROOT'] = root
    env['PATH'] = '%s:%s' % (join(selection.local_path, 'bin'), env['PATH'])
    env['PYTHONPATH'] = '%s:%s' % (selection.local_path, env['PYTHONPATH'])
    env['SUGAR_LOCALEDIR'] = join(selection.local_path, 'locale')
