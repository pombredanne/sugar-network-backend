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
$File: src/application.py$
$Data: 2012-05-23$

"""

import os
import sys
import signal
import atexit
import logging
import textwrap
from optparse import OptionParser
from os.path import join, abspath, exists, basename
from gettext import gettext as _

from . import printf, optparse, util
enforce = util.enforce


debug = optparse.Option(
        _('debug logging level; multiple argument'),
        default=0, type_cast=int, short_option='-D', action='count',
        name='debug')

foreground = optparse.Option(
        _('Do not send the application into the background'),
        default=False, type_cast=optparse.Option.bool_cast, short_option='-F',
        action='store_true', name='foreground')

logdir = optparse.Option(
        _('path to the directory to place log files'),
        name='logdir')

rundir = optparse.Option(
        _('path to the directory to place pid files'),
        name='rundir')


_LOGFILE_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


def command(description, name=None):

    def decorator(func):
        func._is_command = True
        func.name = name
        func.description = description
        return func

    return decorator


class Application(object):

    def __init__(self, name, description=None, version=None, epilog=None,
            where=None, **parse_args):
        self.args = None
        self.name = name

        self._commands = {}
        for attr in dir(self):
            attr = getattr(self, attr)
            if hasattr(attr, '_is_command'):
                self._commands[attr.name or attr.__name__] = attr

        parser = OptionParser(usage='%prog [OPTIONS]', description=description,
                add_help_option=False)

        if version:
            parser.add_option('-V', '--version',
                    help=_('show version number and exit'),
                    action='version')
            parser.print_version = lambda: sys.stdout.write('%s\n' % version)

        parser.add_option('-h', '--help',
                help=_('show this help message and exit'),
                action='store_true')

        options, self.args = optparse.Option.parse_args(parser, **parse_args)

        def print_desc(term, desc):
            text = []
            for line in desc.split('\n'):
                text.extend(textwrap.wrap(line, 54))
            text = ('\n' + ' ' * 24).join(text)
            print '  %-22s%s' % (term, text)

        def print_commands():
            if not self._commands:
                return
            print ''
            print _('Commands') + ':'
            for name, attr in sorted(self._commands.items(),
                    lambda x, y: cmp(x[0], y[0])):
                print_desc(name, attr.description)

        if not self.args and not options.help:
            prog = basename(sys.argv[0])
            print 'Usage: %s [OPTIONS] [COMMAND]' % parser.prog
            print '       %s -h|--help' % prog
            print
            print description
            print_commands()
            if epilog:
                print ''
                print epilog
            exit(0)

        if options.help:
            parser.print_help()
            print_commands()
            if where:
                print ''
                print _('Where') + ':'
                for term in sorted(where):
                    print_desc(term, where[term])
            if epilog:
                print ''
                print epilog
            exit(0)

    def start(self):
        cmd_name = self.args.pop(0)

        if not debug.value:
            logging_level = logging.WARNING
        elif debug.value == 1:
            logging_level = logging.INFO
        else:
            logging_level = logging.DEBUG
        logging_format = _LOGFILE_FORMAT

        root_logger = logging.getLogger('')
        for i in root_logger.handlers:
            root_logger.removeHandler(i)

        logging.basicConfig(level=logging_level, format=logging_format)

        if optparse.Option.config_files:
            logging.info(_('Load configuration from %s file(s)'),
                    ', '.join(optparse.Option.config_files))

        try:
            cmd = self._commands.get(cmd_name)
            enforce(cmd is not None, _('Unknown command "%s"') % cmd_name)
            exit(cmd() or 0)
        except Exception:
            printf.exception(_('Aborted %s'), self.name)
            exit(1)
        finally:
            printf.flush_hints()

    @command(_('output current configuration'), name='config')
    def _cmd_config(self):
        if self.args:
            opt = self.args.pop(0)
            enforce(opt in optparse.Option.items,
                    _('Unknown option "%s"'), opt)
            exit(0 if bool(optparse.Option.items[opt].value) else 1)
        else:
            print '\n'.join(optparse.Option.export())


class Daemon(Application):

    _logdir = None
    _rundir = None

    def run(self):
        raise NotImplementedError()

    def shutdown(self):
        pass

    def epilog(self):
        pass

    def start(self):
        self._logdir = logdir.value or '/var/log/' + self.name
        self._rundir = rundir.value or '/var/run/' + self.name
        Application.start(self)

    @command(_('start in daemon mode'), name='start')
    def _cmd_start(self):
        pidfile, pid = self._check_for_instance()
        if pid:
            printf.info(_('%s is already run with pid %s'), self.name, pid)
            return 1
        if foreground.value:
            self._launch()
        else:
            if not exists(self._logdir):
                os.makedirs(self._logdir)
            enforce(os.access(self._logdir, os.W_OK),
                    _('No write access to %s'), self._logdir)
            if not exists(self._rundir):
                os.makedirs(self._rundir)
            enforce(os.access(self._rundir, os.W_OK),
                    _('No write access to %s'), self._rundir)
            self._forward_stdout()
            self._daemonize(pidfile)
        return 0

    @command(_('stop daemon'), name='stop')
    def _cmd_stop(self):
        __, pid = self._check_for_instance()
        if pid:
            os.kill(pid, signal.SIGTERM)
            return 0
        else:
            printf.info(_('%s is not run'), self.name)
            return 1

    @command(_('check for launched daemon'), name='status')
    def _cmd_status(self):
        __, pid = self._check_for_instance()
        if pid:
            printf.info(_('%s started'), self.name)
            return 0
        else:
            printf.info(_('%s stopped'), self.name)
            return 1

    @command(_('reopen log files in daemon mode'), name='reload')
    def _cmd_reload(self):
        __, pid = self._check_for_instance()
        if not pid:
            printf.info(_('%s is not run'), self.name)
            return 1
        os.kill(pid, signal.SIGHUP)
        logging.info(_('Reload %s process'), self.name)

    def _launch(self):
        logging.info(_('Start %s'), self.name)

        def sigterm_cb(signum, frame):
            logging.info(_('Got signal %s to stop %s'), signum, self.name)
            self.shutdown()

        def sighup_cb(signum, frame):
            logging.info(_('Reload %s on SIGHUP signal'), self.name)
            self._forward_stdout()

        signal.signal(signal.SIGINT, sigterm_cb)
        signal.signal(signal.SIGTERM, sigterm_cb)
        signal.signal(signal.SIGHUP, sighup_cb)

        try:
            self.run()
        finally:
            self.epilog()

    def _check_for_instance(self):
        pid = None
        pidfile = join(self._rundir, '%s.pid' % self.name)
        if exists(pidfile):
            try:
                pid = int(file(pidfile).read().strip())
                os.getpgid(pid)
                if basename(sys.argv[0]) not in _get_cmdline(pid):
                    pid = None
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
            logging.exception(_('Aborted %s'), self.name)
            status = 1
        else:
            logging.info(_('Stopped %s'), self.name)
            status = 0

        exit(status)

    def _forward_stdout(self):
        log_path = abspath(join(self._logdir, '%s.log' % self.name))
        sys.stdout.flush()
        sys.stderr.flush()
        logfile = file(log_path, 'a+')
        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        logfile.close()


def _get_cmdline(pid):
    with file('/proc/%s/cmdline' % pid) as f:
        return f.read()
