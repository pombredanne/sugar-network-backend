# Copyright (C) 2012, Aleksey Lim
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

"""Main process startup routines.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/process.py$
$Data: 2012-04-19$

"""

import os
import sys
import signal
import atexit
import logging
from os.path import join, abspath, exists
from gettext import gettext as _

from . import printf, optparse, util
enforce = util.enforce


debug = optparse.Option(
        _('debug logging level; multiple argument'),
        default=0, type_cast=int, short_option='-D', action='count')

foreground = optparse.Option(
        _('Do not send the process into the background'),
        default=False, type_cast=optparse.Option.bool_cast, short_option='-F',
        action='store_true')

logdir = optparse.Option(
        _('path to the directory to place log files'))

rundir = optparse.Option(
        _('path to the directory to place pid files'))


_LOGFILE_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


def run(name, start_cb, stop_cb, args):
    command = args.pop(0)

    if not debug.value:
        logging_level = logging.WARNING
    elif debug.value == 1:
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    logging_format = _LOGFILE_FORMAT
    if foreground.value or command not in ['start']:
        logging_format = '-- %s' % logging_format
    logging.basicConfig(level=logging_level, format=logging_format)

    server = _Server(name, start_cb, stop_cb, args)

    try:
        enforce(hasattr(server, 'cmd_' + command),
                _('Unknown command "%s"') % command)
        exit(getattr(server, 'cmd_' + command)() or 0)
    except Exception:
        printf.exception(_('Aborted %s'), name)
        exit(1)
    finally:
        printf.flush_hints()


class _Server(object):

    def __init__(self, name, start_cb, stop_cb, args):
        self._name = name
        self._start_cb = start_cb
        self._stop_cb = stop_cb
        self._args = args

    def cmd_config(self):
        if self._args:
            opt = self._args.pop(0)
            enforce(opt in optparse.Option.items,
                    _('Unknown option "%s"'), opt)
            exit(0 if bool(optparse.Option.items[opt].value) else 1)
        else:
            print '\n'.join(optparse.Option.export())

    def cmd_start(self):
        pidfile, pid = self._check_for_instance()
        if pid:
            printf.info(_('%s is already run with pid %s'), self._name, pid)
            return 1
        if foreground.value:
            self._launch()
        else:
            if not exists(logdir.value):
                os.makedirs(logdir.value)
            enforce(os.access(logdir.value, os.W_OK),
                    _('No write access to %s'), logdir.value)
            if not exists(rundir.value):
                os.makedirs(rundir.value)
            enforce(os.access(rundir.value, os.W_OK),
                    _('No write access to %s'), rundir.value)
            self._forward_stdout()
            self._daemonize(pidfile)
        return 0

    def cmd_stop(self):
        __, pid = self._check_for_instance()
        if pid:
            os.kill(pid, signal.SIGTERM)
            return 0
        else:
            printf.info(_('%s is not run'), self._name)
            return 1

    def cmd_status(self):
        __, pid = self._check_for_instance()
        if pid:
            printf.info(_('%s started'), self._name)
            return 0
        else:
            printf.info(_('%s stopped'), self._name)
            return 1

    def cmd_reload(self):
        __, pid = self._check_for_instance()
        if not pid:
            printf.info(_('%s is not run'), self._name)
            return 1
        os.kill(pid, signal.SIGHUP)
        logging.info(_('Reload %s process'), self._name)

    def _launch(self):
        logging.info(_('Start %s'), self._name)

        def sigterm_cb(signum, frame):
            logging.info(_('Got signal %s to stop %s'), signum, self._name)
            if self._stop_cb is not None:
                self._stop_cb()

        def sighup_cb(signum, frame):
            logging.info(_('Reload %s on SIGHUP signal'), self._name)
            self._forward_stdout()

        signal.signal(signal.SIGINT, sigterm_cb)
        signal.signal(signal.SIGTERM, sigterm_cb)
        signal.signal(signal.SIGHUP, sighup_cb)

        self._start_cb()

    def _check_for_instance(self):
        pid = None
        pidfile = join(rundir.value, '%s.pid' % self._name)
        if exists(pidfile):
            try:
                pid = int(file(pidfile).read().strip())
                os.getpgid(pid)
            except (ValueError, OSError):
                pid = None
        return pidfile, pid

    def _daemonize(self, pid_path):
        pid_path = abspath(pid_path)

        if os.fork() > 0:
            # Exit parent of the first child
            return

        # Decouple from parent environment
        os.chdir(os.sep)
        os.setsid()

        if os.fork() > 0:
            # Exit from second parent
            # pylint: disable-msg=W0212
            os._exit(0)

        # Redirect standard file descriptors
        if not sys.stdin.closed:
            stdin = file('/dev/null')
            os.dup2(stdin.fileno(), sys.stdin.fileno())

        pidfile = file(pid_path, 'w')
        pidfile.write(str(os.getpid()))
        pidfile.close()
        atexit.register(lambda: os.remove(pid_path))

        try:
            self._launch()
        except Exception:
            logging.exception(_('Aborted %s'), self._name)
            status = 1
        else:
            logging.info(_('Stopped %s'), self._name)
            status = 0

        exit(status)

    def _forward_stdout(self):
        if not exists(logdir.value):
            os.makedirs(logdir.value)
        log_path = abspath(join(logdir.value, '%s.log' % self._name))
        sys.stdout.flush()
        sys.stderr.flush()
        logfile = file(log_path, 'a+')
        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        logfile.close()
