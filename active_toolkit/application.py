# Copyright (C) 2012 Aleksey Lim
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
$Date: 2012-07-22$

"""

import os
import sys
import signal
import logging
import textwrap
from optparse import OptionParser
from os.path import join, abspath, exists, basename

from .options import Option
from . import printf, util
enforce = util.enforce


debug = Option(
        'debug logging level; multiple argument',
        default=0, type_cast=int, short_option='-D', action='count',
        name='debug')

foreground = Option(
        'do not send the application into the background',
        default=False, type_cast=Option.bool_cast, short_option='-F',
        action='store_true', name='foreground')

logdir = Option(
        'path to the directory to place log files',
        name='logdir')

rundir = Option(
        'path to the directory to place pid files',
        name='rundir')


_LOGFILE_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


def command(description, name=None, **options):

    def decorator(func):
        func._is_command = True
        func.name = name
        func.description = description
        func.options = options
        return func

    return decorator


class Application(object):

    def __init__(self, name, description=None, version=None, epilog=None,
            where=None, **parse_args):
        self._rundir = None
        self.args = None
        self.name = name

        self._commands = {}
        for attr in dir(self):
            attr = getattr(self, attr)
            if hasattr(attr, '_is_command') and \
                    (attr.name != 'config' or 'config_files' in parse_args):
                self._commands[attr.name or attr.__name__] = attr

        parser = OptionParser(usage='%prog [OPTIONS]', description=description,
                add_help_option=False)

        if version:
            parser.add_option('-V', '--version',
                    help='show version number and exit',
                    action='version')
            parser.print_version = lambda: sys.stdout.write('%s\n' % version)

        parser.add_option('-h', '--help',
                help='show this help message and exit',
                action='store_true')

        options, self.args = Option.parse_args(parser, **parse_args)

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
            if len(term) < 24:
                sys.stdout.write('  %-22s' % term)
            else:
                text.insert(0, '')
                sys.stdout.write('  %s' % term)
            print ('\n' + ' ' * 24).join(text)

        def print_commands():
            if not self._commands:
                return
            print ''
            print 'Commands:'
            for name, attr in sorted(self._commands.items(),
                    lambda x, y: cmp(x[0], y[0])):
                print_desc(name, attr.description)

        if not self.args and not options.help:
            prog = basename(sys.argv[0])
            print 'Usage: %s [OPTIONS] [COMMAND]' % prog
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
                print 'Where:'
                for term in sorted(where):
                    print_desc(term, where[term])
            if epilog:
                print ''
                print epilog
            exit(0)

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

    def start(self):
        self._rundir = abspath(rundir.value or '/var/run/' + self.name)

        cmd_name = self.args.pop(0)
        try:
            cmd = self._commands.get(cmd_name)
            enforce(cmd is not None, 'Unknown command "%s"' % cmd_name)

            if Option.config_files:
                logging.info('Load configuration from %s file(s)',
                        ', '.join(Option.config_files))

            if cmd.options.get('keep_stdout') and not foreground.value:
                self._keep_stdout()

            exit(cmd() or 0)
        except Exception:
            printf.exception('Aborted %s', self.name)
            exit(1)
        finally:
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

    def ensure_pidfile_path(self):
        if not exists(self._rundir):
            os.makedirs(self._rundir)
        enforce(os.access(self._rundir, os.W_OK),
                'No write access to %r to store pidfile', self._rundir)

    def new_instance(self):
        self.ensure_pidfile_path()
        pidfile_path = join(self._rundir, '%s.pid' % self.name)
        with file(pidfile_path, 'w') as f:
            f.write(str(os.getpid()))
        return pidfile_path

    @command('output current configuration', name='config')
    def _cmd_config(self):
        if self.args:
            opt = self.args.pop(0)
            enforce(opt in Option.items, 'Unknown option "%s"', opt)
            exit(0 if bool(Option.items[opt].value) else 1)
        else:
            print '\n'.join(Option.export())

    def _keep_stdout(self):
        log_dir = logdir.value or '/var/log'
        if not exists(log_dir):
            os.makedirs(log_dir)
        enforce(os.access(log_dir, os.W_OK), 'No write access to %s', log_dir)
        log_path = abspath(join(log_dir, '%s.log' % self.name))
        sys.stdout.flush()
        sys.stderr.flush()
        logfile = file(log_path, 'a+')
        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        logfile.close()


class Daemon(Application):

    def run(self):
        raise NotImplementedError()

    def shutdown(self):
        pass

    def epilog(self):
        pass

    @command('start in daemon mode', name='start', keep_stdout=True)
    def _cmd_start(self):
        pid = self.check_for_instance()
        if pid:
            printf.info('%s is already run with pid %s', self.name, pid)
            return 1
        if foreground.value:
            self._launch()
        else:
            self.ensure_pidfile_path()
            self._daemonize()
        return 0

    @command('stop daemon', name='stop')
    def _cmd_stop(self):
        pid = self.check_for_instance()
        if pid:
            os.kill(pid, signal.SIGTERM)
            return 0
        else:
            printf.info('%s is not run', self.name)
            return 1

    @command('check for launched daemon', name='status')
    def _cmd_status(self):
        pid = self.check_for_instance()
        if pid:
            printf.info('%s started', self.name)
            return 0
        else:
            printf.info('%s stopped', self.name)
            return 1

    @command('reopen log files in daemon mode', name='reload')
    def _cmd_reload(self):
        pid = self.check_for_instance()
        if not pid:
            printf.info('%s is not run', self.name)
            return 1
        os.kill(pid, signal.SIGHUP)
        logging.info('Reload %s process', self.name)

    def _launch(self):
        logging.info('Start %s', self.name)

        def sigterm_cb(signum, frame):
            logging.info('Got signal %s to stop %s', signum, self.name)
            self.shutdown()

        def sighup_cb(signum, frame):
            logging.info('Reload %s on SIGHUP signal', self.name)
            self._keep_stdout()

        signal.signal(signal.SIGINT, sigterm_cb)
        signal.signal(signal.SIGTERM, sigterm_cb)
        signal.signal(signal.SIGHUP, sighup_cb)

        try:
            self.run()
        finally:
            self.epilog()

    def _daemonize(self):
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

        pid_path = self.new_instance()
        try:
            self._launch()
        except Exception:
            logging.exception('Aborted %s', self.name)
            status = 1
        else:
            logging.info('Stopped %s', self.name)
            status = 0
        finally:
            os.unlink(pid_path)

        exit(status)


def _get_cmdline(pid):
    with file('/proc/%s/cmdline' % pid) as f:
        return f.read()
