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

"""Main process startup routines."""

import os
import sys
import time
import signal
import logging
import textwrap
from optparse import OptionParser
from os.path import join, abspath, exists, basename
from gettext import gettext as _

from sugar_network.toolkit import Option
from sugar_network.toolkit import coroutine, printf, init_logging, enforce


debug = Option(
        _('debug logging level; multiple argument'),
        default=0, type_cast=int, short_option='-D', action='count',
        name='debug')

foreground = Option(
        _('do not send the application into the background'),
        default=False, type_cast=Option.bool_cast, short_option='-F',
        action='store_true', name='foreground')

replace = Option(
        'if application is already launched, replace it by new instance',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='replace')

no_hints = Option(
        _('suppress suggesting hints'),
        default=False, short_option='-H', action='store_true',
        name='no-hints')

logdir = Option(
        'path to a directory to place log files',
        name='logdir', default='/var/log')

rundir = Option(
        'path to a directory to place pid files',
        name='rundir')


def command(description='', name=None, args=None, hidden=False,
        interspersed_args=True, **options):

    def decorator(func):
        func.is_command = True
        func.name = name
        func.description = description
        func.args = args
        func.options = options
        func.hidden = hidden
        func.interspersed_args = interspersed_args
        return func

    return decorator


class Application(object):

    def __init__(self, name, description=None, version=None, epilog=None,
            where=None, **kwargs):
        self._rundir = None
        self.args = None
        self.name = name
        self._commands = {}

        stop_args = []
        for name in dir(self.__class__):
            attr = getattr(self.__class__, name)
            if not hasattr(attr, 'is_command') or \
                    (attr.name == 'config' and 'config_files' not in kwargs):
                continue
            cmd_name = attr.name or name
            cmd = self._commands[cmd_name] = getattr(self, name)
            if not cmd.interspersed_args:
                stop_args.append(cmd_name)

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

        options, self.args = Option.parse_args(parser, stop_args=stop_args,
                **kwargs)

        def print_desc(term, desc):
            text = []
            for num, line in enumerate(desc.split('\n')):
                if num == 0:
                    for i in line:
                        if i.isalpha() and not i.isupper():
                            break
                    else:
                        term += ' ' + line
                        continue
                text.extend(textwrap.wrap(line, 54))
            if len(term) < 22:
                sys.stdout.write('  %-22s' % term)
            else:
                text.insert(0, '')
                sys.stdout.write('  %s' % term)
            print ('\n' + ' ' * 24).join(text)

        def print_commands():
            if not self._commands:
                return
            print ''
            print _('Commands') + ':'
            for name, attr in sorted(self._commands.items(),
                    lambda x, y: cmp(x[0], y[0])):
                if attr.hidden:
                    continue
                if attr.args:
                    name += ' ' + attr.args
                print_desc(name, attr.description)

        if not self.args and not options.help:
            prog = basename(sys.argv[0])
            print _('Usage') + ': %s [OPTIONS] [COMMAND]' % prog
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

        init_logging(debug.value)

    def prolog(self):
        pass

    def epilog(self):
        pass

    def start(self):
        cmd_name = self.args.pop(0)
        try:
            cmd = self._commands.get(cmd_name)
            enforce(cmd is not None, 'Unknown command "%s"' % cmd_name)

            if Option.config_files:
                logging.info('Load configuration from %s file(s)',
                        ', '.join(Option.config_files))

            self.prolog()
            self._rundir = abspath(rundir.value or '/var/run/' + self.name)

            if cmd.options.get('keep_stdout') and not foreground.value:
                self.reopen_logs()
            exit(cmd() or 0)
        except Exception:
            printf.exception('%s %s', _('Aborted'), self.name)
            exit(1)
        finally:
            self.epilog()
            if not no_hints.value:
                printf.flush_hints()

    def check_for_instance(self):
        pid = None
        pidfile = join(self._rundir, '%s.pid' % self.name)
        if exists(pidfile):
            try:
                pid = int(file(pidfile).read().strip())
                os.getpgid(pid)
                if basename(sys.argv[0]) not in _get_cmdline(pid):
                    # In case if pidfile was not removed after reboot
                    # and another process launched with pidfile's pid
                    pid = None
            except (ValueError, OSError):
                pid = None
        return pid

    def ensure_pidfile(self):
        if not exists(self._rundir):
            os.makedirs(self._rundir)
        pidfile_path = join(self._rundir, '%s.pid' % self.name)
        return file(pidfile_path, 'w')

    def new_instance(self):
        with self.ensure_pidfile() as f:
            f.write(str(os.getpid()))
            return f.name

    @command('output current configuration', name='config')
    def _cmd_config(self):
        if self.args:
            opt = self.args.pop(0)
            enforce(opt in Option.items, 'Unknown option "%s"', opt)
            exit(0 if bool(Option.items[opt].value) else 1)
        else:
            print Option.help()

    def reopen_logs(self):
        log_dir = abspath(logdir.value)
        if not exists(log_dir):
            os.makedirs(log_dir)

        log_path = join(log_dir, '%s.log' % self.name)
        logfile = file(log_path, 'a+')

        # printf should still output to original streams
        printf.stdout = os.fdopen(os.dup(sys.stdout.fileno()), 'w')
        printf.stderr = os.fdopen(os.dup(sys.stderr.fileno()), 'w')

        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        logfile.close()


class Daemon(Application):

    _accept_pipe = None

    def run(self):
        raise NotImplementedError()

    def shutdown(self):
        pass

    @command('start in daemon mode', name='start', keep_stdout=True)
    def cmd_start(self):
        while True:
            pid = self.check_for_instance()
            if not pid:
                break
            if not replace.value:
                printf.info('%s is already run with pid %s', self.name, pid)
                return 1
            try:
                printf.info('Kill previous %r instance', pid)
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
            time.sleep(.5)

        if foreground.value:
            self._launch()
        else:
            with self.ensure_pidfile():
                pass
            self._daemonize()

        return 0

    @command('stop daemon', name='stop')
    def cmd_stop(self):
        pid = self.check_for_instance()
        if pid:
            os.kill(pid, signal.SIGTERM)
            return 0
        else:
            printf.info('%s is not run', self.name)
            return 1

    @command('check for launched daemon', name='status')
    def cmd_status(self):
        pid = self.check_for_instance()
        if pid:
            printf.info('%s started', self.name)
            return 0
        else:
            printf.info('%s stopped', self.name)
            return 1

    @command('reopen log files in daemon mode', name='reload')
    def cmd_reload(self):
        pid = self.check_for_instance()
        if not pid:
            printf.info('%s is not run', self.name)
            return 1
        os.kill(pid, signal.SIGHUP)
        logging.info('Reload %s process', self.name)

    def accept(self):
        if self._accept_pipe is not None:
            os.close(self._accept_pipe)
            self._accept_pipe = None

    def _launch(self):
        logging.info('Start %s', self.name)

        def sigterm_cb(signum):
            logging.info('Got signal %s to stop %s', signum, self.name)
            self.shutdown()

        def sighup_cb():
            logging.info('Reload %s on SIGHUP signal', self.name)
            self.reopen_logs()

        coroutine.signal(signal.SIGINT, sigterm_cb, signal.SIGINT)
        coroutine.signal(signal.SIGTERM, sigterm_cb, signal.SIGTERM)
        coroutine.signal(signal.SIGHUP, sighup_cb)

        pid_path = self.new_instance()
        try:
            self.run()
        finally:
            os.unlink(pid_path)

    def _daemonize(self):
        accept_pipe = os.pipe()
        if os.fork() > 0:
            os.close(accept_pipe[1])
            os.read(accept_pipe[0], 1)
            # Exit parent of the first child
            return

        os.close(accept_pipe[0])
        self._accept_pipe = accept_pipe[1]

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

        try:
            self._launch()
        except Exception:
            logging.exception('Aborted %s', self.name)
            status = 1
        else:
            logging.info('Stopped %s', self.name)
            status = 0

        exit(status)


def _get_cmdline(pid):
    with file('/proc/%s/cmdline' % pid) as f:
        return f.read()
