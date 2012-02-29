# Copyright (C) 2011-2012, Aleksey Lim
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

"""Swiss knife module.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/util.py$
$Data: 2012-02-23$

"""

import os
import re
import sys
import fcntl
import shutil
import atexit
import logging
import tempfile
import datetime
import subprocess
from os.path import exists, join, islink, isdir, dirname, basename, lexists
from os.path import abspath, expanduser
from gettext import gettext as _

try:
    import json
    if not hasattr(json, 'dumps'):
        raise ImportError()
except ImportError:
    import simplejson as json


def enforce(condition, error=None, *args):
    """Make an assertion in runtime.

    In comparing with `assert`, it will all time present in the code.
    Just a bit of syntax sugar.

    :param condition:
        the condition to assert; if not False then return,
        otherse raise an RuntimeError exception
    :param error:
        error message to pass to RuntimeError object
        or Exception class to raise
    :param args:
        optional '%' arguments for the `error`

    """
    if condition:
        return

    if isinstance(error, type):
        exception_class = error
        if args:
            error = args[0]
            args = args[1:]
        else:
            error = None
    else:
        exception_class = RuntimeError

    if args:
        error = error % args
    elif not error:
        # pylint: disable-msg=W0212
        frame = sys._getframe(1)
        error = _('Runtime assertion failed at %s:%s') % \
                (frame.f_globals['__file__'], frame.f_lineno - 1)

    raise exception_class(error)


def exception(*args):
    """Log about exception on low log level.

    That might be useful for non-critial exception. Input arguments are the
    same as for `logging.exception` function.

    :param args:
        optional arguments to pass to logging function;
        the first argument might be a `logging.Logger` to use instead of
        using direct `logging` calls

    """
    if args and isinstance(args[0], logging.Logger):
        logger = args[0]
        args = args[1:]
    else:
        logger = logging

    klass, error, tb = sys.exc_info()

    import traceback
    tb = [i.rstrip() for i in traceback.format_exception(klass, error, tb)]

    error = str(error) or _('Something weird happened')
    if args:
        if len(args) == 1:
            message = args[0]
        else:
            message = args[0] % args[1:]
        error = '%s: %s' % (message, error)

    logger.error(error)
    logger.debug('\n'.join(tb))


def assert_call(cmd, stdin=None, **kwargs):
    """Variant of `call` method with raising exception of errors.

    :param cmd:
        commad to execute, might be string or argv list
    :param stdin:
        text that will be used as an input for executed process

    """
    return call(cmd, stdin=stdin, asserts=True, **kwargs)


def call(cmd, stdin=None, asserts=False, raw=False, error_cb=None, **kwargs):
    """Convenient wrapper around subprocess call.

    Note, this function is intended for processes that output finite
    and not big amount of text.

    :param cmd:
        commad to execute, might be string or argv list
    :param stdin:
        text that will be used as an input for executed process
    :param asserts:
        whether to raise `RuntimeError` on fail execution status
    :param error_cb:
        call callback(stderr) on getting error exit status from the process
    :returns:
        `None` on errors, otherwise `str` value of stdout

    """
    stdout, stderr = None, None
    returncode = 1
    try:
        logging.debug('Exec %r', cmd)
        process = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                stdout=subprocess.PIPE, stdin=subprocess.PIPE, **kwargs)
        if stdin is not None:
            process.stdin.write(stdin)
            process.stdin.close()
        # Avoid using Popen.communicate()
        # http://bugs.python.org/issue4216#msg77582
        process.wait()
        stdout = _nb_read(process.stdout)
        stderr = _nb_read(process.stderr)
        if not raw:
            stdout = stdout.strip()
            stderr = stderr.strip()
        returncode = process.returncode
        enforce(returncode == 0, _('Exit status is an error'))
        logging.debug('Successfully executed stdout=%r stderr=%r',
                stdout.split('\n'), stderr.split('\n'))
        return stdout
    except Exception, error:
        logging.debug('Failed to execute error="%s" stdout=%r stderr=%r',
                error, str(stdout).split('\n'), str(stderr).split('\n'))
        if asserts:
            raise RuntimeError(_('Failed to execute %r command: %s') % \
                    (cmd, error))
        elif error_cb is not None:
            error_cb(returncode, stdout, stderr)


def rmtree(path, ignore_errors=True, **kwargs):
    """Remove directory with all its content.

    Function will check if owner has permissions for removing directories
    (it makes sense for 0install implementaion caches).

    :param path:
        path to the directory to remove
    :param ignore_errors:
        ignore all errors while removing
    :returns:
        `None` on errors, otherwise `str` value of stdout

    """
    if isdir(path):

        def fix_dir(path):
            # 0install removes owner permissions
            stat = os.stat(path).st_mode & 0777
            if stat & 0700 != 0700:
                os.chmod(path, stat | 0700)

        fix_dir(path)
        for root, dirs, __ in os.walk(path):
            for i in dirs:
                fix_dir(join(root, i))

        shutil.rmtree(path, ignore_errors=ignore_errors, **kwargs)
    elif lexists(path):
        os.unlink(path)


def cptree(src, dst):
    """Efficient version of copying directories.

    Function will try to make hard links for copying files at first and
    will fallback to regular copying overwise.

    :param src:
        path to the source directory
    :param dst:
        path to the new directory

    """
    if abspath(src) == abspath(dst):
        return

    do_copy = []

    def link(src, dst):
        if not exists(dirname(dst)):
            os.makedirs(dirname(dst))

        if islink(src):
            link_to = os.readlink(src)
            os.symlink(link_to, dst)
        elif isdir(src):
            cptree(src, dst)
        else:
            if do_copy:
                shutil.copy(src, dst)
            else:
                try:
                    os.link(src, dst)
                except OSError:
                    do_copy.append(True)
                    shutil.copy(src, dst)
            shutil.copystat(src, dst)

    if isdir(src):
        for root, __, files in os.walk(src):
            dst_root = join(dst, root[len(src):].lstrip(os.sep))
            if not exists(dst_root):
                os.makedirs(dst_root)
            for i in files:
                link(join(root, i), join(dst_root, i))
    else:
        link(src, dst)


def new_file(path, mode=None):
    """Atomic new file creation.

    Method will create temporaty file in the same directory as the specified
    one. When file object associated with this temporaty file will be closed,
    temporaty file will be renamed to the final destination.

    :param path:
        path to save final file to
    :param mode:
        mode for new file
    :returns:
        file object

    """
    tmp_path = TempFilePath(dir=dirname(path), prefix=basename(path))

    result = _NewFile(tmp_path, 'w')
    if mode:
        os.fchmod(result.fileno(), mode)
    result.tmp_path = tmp_path
    result.dst_path = path

    return result


def get_frame(frame_no):
    """Return Python call stack frame.

    The reason to have this wrapper is that this stack information is a private
    data and might depend on Python implementaion.

    :param frame_no:
        number of stack frame starting from caller's stack position
    :returns:
        frame object

    """
    # +1 since the calling `get_frame` adds one more frame
    # pylint: disable-msg=W0212
    return sys._getframe(frame_no + 1)


def utcnow():
    """Return local time in UTC.

    Support testing workflow on multi processes level.

    :returns:
        `datetime.datetime.utcnow()` value

    """
    direct_time_path = '/tmp/.utcnow'
    if exists(direct_time_path):
        ts = os.stat(direct_time_path).st_mtime
        return datetime.datetime.fromtimestamp(ts)
    else:
        return datetime.datetime.utcnow()


def _set_utcnow(value):
    direct_time_path = '/tmp/.utcnow'
    file(direct_time_path, 'w').close()
    os.utime(direct_time_path, (value, value))


def _unset_utcnow():
    direct_time_path = '/tmp/.utcnow'
    if exists(direct_time_path):
        os.unlink(direct_time_path)


class Option(object):
    """Configuration option.

    `Option` object will be used as command-line argument and
    configuration file option. All these objects will be automatically
    collected from `sugar_server.env` module and from `etc` module from
    all services.

    """
    #: Collected by `Option.seek()` options in original order.
    unsorted_items = []
    #: Collected by `Option.seek()` options by name.
    items = {}
    #: Collected by `Option.seek()` options by section.
    sections = {}
    _config = None

    def __init__(self, description=None, default=None, short_option=None,
            type_cast=None, type_repr=None, action=None):
        """
        :param description:
            description string
        :param default:
            default value for the option
        :param short_option:
            value in for of `-<char>` to use as a short option for command-line
            parser
        :param type_cast:
            function that will be uses to type cast to option type
            while setting option value
        :param type_repr:
            function that will be uses to type cast from option type
            while converting option value to string
        :param action:
            value for `action` argument of `OptionParser.add_option()`

        """
        if default is not None and type_cast is not None:
            default = type_cast(default)
        self._value = default
        self.description = description
        self.type_cast = type_cast
        self.type_repr = type_repr
        self.short_option = short_option or ''
        self.action = action
        self.section = None
        self.name = None
        self.attr_name = None

    @property
    def long_option(self):
        """Long command-line argument name."""
        return '--%s' % self.name

    # pylint: disable-msg=E0202
    @property
    def value(self):
        """Get option raw value."""
        return self._value

    # pylint: disable-msg=E1101, E0102, E0202
    @value.setter
    def value(self, x):
        """Set option value.

        The `Option.type_cast` function will be used for type casting specified
        value to option.

        """
        if x is None:
            self._value = None
        elif self.type_cast is not None:
            self._value = self.type_cast(x)
        else:
            self._value = str(x) or None

    @staticmethod
    def seek(section, mod=None):
        """Collect `Option` objects.

        Function will populate `Option.unsorted_items`, `Option.items` and
        `Option.sections` values. Call this function before any usage
        of `Option` objects.

        :param section:
            arbitrary name to group options per section
        :param mod:
            mdoule object to search for `Option` objects;
            if omited, use caller's module

        """
        if mod is None:
            mod_name = get_frame(1).f_globals['__name__']
            mod = sys.modules[mod_name]

        for name in sorted(dir(mod)):
            attr = getattr(mod, name)
            # Options might be from different `util` modules
            if not (type(attr).__name__ == 'Option' and \
                    type(attr).__module__.split('.')[-1] == 'util'):
                continue

            attr.attr_name = name
            attr.name = name.replace('_', '-')
            attr.module = mod
            attr.section = section

            Option.unsorted_items.append(attr)
            Option.items[attr.name] = attr
            if section not in Option.sections:
                Option.sections[section] = {}
            Option.sections[section][attr.name] = attr

    @staticmethod
    def bind(parser, config_files=None, notice=None):
        """Initilize option usage.

        Call this function after invoking `Option.seek()`.

        :param parser:
            if not `None`, `OptionParser` object to export,
            collected by `Option.seek` options, to
        :param config_files:
            list of paths to files that will be used to read default
            option values; this value will initiate `Option.config` variable
        :param notice:
            optional notice to print with arguments' description

        """
        if config_files:
            Option._config = Option()
            Option._config.name = 'config'
            Option._config.attr_name = 'config'
            Option._config.description = \
                    _('colon separated list of paths to alternative ' \
                    'configuration file(s)')
            Option._config.short_option = '-c'
            Option._config.type_cast = \
                    lambda x: [i for i in re.split('[\s:;,]+', x) if i]
            Option._config.type_repr = \
                    lambda x: ':'.join(x)
            Option._config.value = ':'.join(config_files)

        for prop in [Option._config] + Option.items.values():
            desc = prop.description
            if prop.value is not None:
                desc += ' [%s]' % prop
            if notice:
                desc += '; ' + notice
            if parser is not None:
                parser.add_option(prop.short_option, prop.long_option,
                        action=prop.action, help=desc)

    @staticmethod
    def merge(options, config_files=None):
        """Combine default values with command-line arguments and config files.

        Call this function after invoking `Option.bind()`.

        :param options:
            the first value from a tuple returned by
            `OptionParser.parse_args()` function
        :param config_files:
            list of either config paths or `ConfigParser` objects to get
            default option values

        """
        from ConfigParser import ConfigParser

        if config_files is None:
            if Option._config is None:
                raise RuntimeError(_('Method Option.merge was not called or ' \
                        'its config_files argument was None'))
            config_files = Option._config.value

        configs = [ConfigParser()]
        for config_file in config_files:
            if isinstance(config_file, ConfigParser):
                configs.append(config_file)
            elif exists(expanduser(config_file)):
                configs[0].read(expanduser(config_file))

        for prop in Option.items.values():
            if hasattr(options, prop.attr_name) and \
                    getattr(options, prop.attr_name) is not None:
                prop.value = getattr(options, prop.attr_name)
            else:
                for config in configs:
                    if config.has_option(prop.section, prop.name):
                        prop.value = config.get(prop.section, prop.name)

    @staticmethod
    def export():
        """Current configuration in human readable form.

        :returns:
            list of lines

        """
        import textwrap

        lines = []
        sections = set()

        for prop in Option.unsorted_items:
            if prop.section not in sections:
                if sections:
                    lines.append('')
                lines.append('[%s]' % prop.section)
                sections.add(prop.section)
            lines.append('\n'.join(
                    ['# %s' % i for i in textwrap.wrap(prop.description, 78)]))
            value = '\n\t'.join(str(prop).split('\n'))
            lines.append('%s = %s' % (prop.name, value))

        return lines

    @staticmethod
    def bool_cast(x):
        if not x or str(x).strip().lower() in ['', 'false', 'none']:
            return False
        else:
            return bool(x)

    @staticmethod
    def list_cast(x):
        if isinstance(x, str) or isinstance(x, unicode):
            return [i for i in x.strip().split(':') if i]
        else:
            return x

    @staticmethod
    def list_repr(x):
        return ':'.join(x)

    def __str__(self):
        if self.value is None:
            return ''
        else:
            if self.type_repr is None:
                return str(self.value)
            else:
                return self.type_repr(self.value)

    def __unicode__(self):
        return self.__str__()


class Command(object):
    """Service command.

    `Command` is a way to have custom sub-commands in services. All these
    objects will be automatically collected from `etc` module
    from all services.

    """
    #: Collected by `Command.seek()` commands by name.
    items = {}
    #: Collected by `Command.seek()` commands by section.
    sections = {}

    def __init__(self, description=None, cmd_format=None):
        """
        :param description:
            command description
        :param cmd_format:
            part of description to explain additional command arguments

        """
        self.description = description or ''
        self.cmd_format = cmd_format or ''
        self.name = None
        self.attr_name = None

    @staticmethod
    def seek(section, mod=None):
        """Collect `Command` objects.

        Function will populate `Command.items` and `Command.sections` values.
        Call this function before any usage of `Command` objects.

        :param section:
            arbitrary name to group options per section
        :param mod:
            mdoule object to search for `Option` objects;
            if omited, use caller's module

        """
        if mod is None:
            mod_name = get_frame(1).f_globals['__name__']
            mod = sys.modules[mod_name]

        for name in sorted(dir(mod)):
            attr = getattr(mod, name)
            # Commands might be from different `util` modules
            if not (type(attr).__name__ == 'Command' and \
                    type(attr).__module__.split('.')[-1] == 'util'):
                continue

            attr.name = name.replace('_', '-')
            attr.attr_name = name
            attr.module = mod
            attr.section = section

            Command.items[attr.name] = attr
            if section not in Command.sections:
                Command.sections[section] = {}
            Command.sections[section][attr.name] = attr

    @staticmethod
    def call(mod, name, *args, **kwargs):
        """Call the command.

        Specfied module should contain a function with a name
        `CMD_<command-name>()`. All additional `Command.call()` arguments
        will be passed as-is to command implementaion function.

        :param mod:
            module to search for command implementaion
        :param name:
            command name
        :returns:
            what command implementaion returns

        """
        cmd = Command.items.get(name)
        enforce(cmd is not None, _('No such command, %s'), name)

        func_name = 'CMD_%s' % cmd.attr_name
        if not hasattr(mod, func_name):
            raise RuntimeError(_('No such command, %s, in module %s') % \
                    (name, mod.__name__))
        getattr(mod, func_name)(*args, **kwargs)

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.__str__()


class TempFilePath(unicode):
    """Auto removed temporary file.

    Right after creating `TempFilePath` object, temporaty file will be
    created. On `TempFilePath` object deleting, this file will be removed.
    The key difference with `tempfile.NamedTemporaryFile` is that
    `TempFilePath` doesn't keep open file descriptor with removing file
    right after closing it (though starting form Python 2.6,
    `tempfile.NamedTemporaryFile` supports `delete` argument).

    """

    def __new__(cls, path=None, text=None, **kwargs):
        """
        Function supports the same arguments as `tempfile.mkstemp`.

        :param path:
            instead of generating temporary file name, exact `path` value
            will be used
        :param text:
            content for newly created file

        """
        if path:
            if not exists(dirname(path)):
                os.makedirs(dirname(path))
            fd = None
        else:
            if 'dir' in kwargs:
                dir_value = kwargs['dir']
                if not exists(dir_value):
                    os.makedirs(dir_value)
            fd, path = tempfile.mkstemp(**kwargs)

        _temp_file_paths.add(path)

        if text is not None:
            if fd is None:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT)
            os.write(fd, text)

        if fd is not None:
            os.close(fd)

        return unicode.__new__(cls, path)

    def __del__(self):
        if _temp_file_paths and self in _temp_file_paths:
            _temp_file_paths.remove(self)
            if exists(self):
                os.unlink(self)


class TempDir(unicode):
    """Auto removed temporary directory.

    Right after creating `TempDir` object, temporaty directory will be
    created. On `TempDir` object deleting, this directory will be removed.

    """

    def __new__(cls, path=None, **kwargs):
        """
        Function supports the same arguments as `tempfile.mkdtemp`.

        :param path:
            instead of generating temporary file name, exact `path` value
            will be used

        """
        if path is not None:
            if not exists(path):
                os.makedirs(path)
        else:
            if 'dir' in kwargs:
                dir_value = kwargs['dir']
                if not exists(dir_value):
                    os.makedirs(dir_value)
            path = tempfile.mkdtemp(**kwargs)

        _temp_dirs.add(path)

        return unicode.__new__(cls, path)

    def __del__(self):
        if _temp_dirs and self in _temp_dirs:
            _temp_dirs.remove(self)
            if exists(self):
                rmtree(self)

    def persist(self):
        """Do not auto remove this directory."""
        if self in _temp_dirs:
            _temp_dirs.remove(self)


class _NewFile(file):

    dst_path = None
    tmp_path = None

    def close(self):
        file.close(self)
        if self.tmp_path is not None:
            os.rename(self.tmp_path, self.dst_path)
            self.tmp_path = None

    def __del__(self):
        self.tmp_path = None


def _cleanup_temp_files():
    for path in _temp_file_paths:
        if exists(path):
            os.unlink(path)

    for path in _temp_dirs:
        if exists(path):
            rmtree(path)


_temp_file_paths = set()
_temp_dirs = set()
atexit.register(_cleanup_temp_files)


def _nb_read(stream):
    if stream is None:
        return ''
    fd = stream.fileno()
    orig_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, orig_flags | os.O_NONBLOCK)
        return stream.read()
    except Exception:
        return ''
    finally:
        fcntl.fcntl(fd, fcntl.F_SETFL, orig_flags)
