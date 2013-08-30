# Copyright (C) 2011-2013 Aleksey Lim
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
import errno
import shutil
import logging
import tempfile
import collections
from os.path import exists, join, islink, isdir, dirname, basename, abspath
from os.path import lexists, isfile

from sugar_network.toolkit.options import Option


BUFFER_SIZE = 1024 * 10


cachedir = Option(
        'path to a directory to keep cached files; such files '
        'might take considerable number of bytes',
        default='/var/cache/sugar-network', name='cachedir')

_logger = logging.getLogger('toolkit')


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
        error = 'Runtime assertion failed at %s:%s' % \
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

    error_message = str(error) or '%s exception' % type(error).__name__
    if args:
        if len(args) == 1:
            message = args[0]
        else:
            message = args[0] % args[1:]
        error_message = '%s: %s' % (message, error_message)

    logger.error(error_message)
    logger.debug('\n'.join(tb))


def default_lang():
    """Default language to fallback for localized strings.

    :returns:
        string in format of HTTP's Accept-Language, e.g., `en-gb`.

    """
    global _default_lang

    if _default_lang is None:
        import locale
        lang = locale.getdefaultlocale()[0]
        if not lang or lang == 'C':
            _default_lang = 'en'
        else:
            lang, region = lang.lower().split('_')
            if lang == region:
                _default_lang = lang
            else:
                _default_lang = '-'.join([lang, region])

    return _default_lang


def gettext(value, accept_language=None):
    if not value:
        return ''
    if not isinstance(value, dict):
        return value

    if accept_language is None:
        accept_language = [default_lang()]
    elif isinstance(accept_language, basestring):
        accept_language = [accept_language]
    accept_language.append('en')

    stripped_value = None
    for lang in accept_language:
        result = value.get(lang)
        if result is not None:
            return result

        prime_lang = lang.split('-')[0]
        if prime_lang != lang:
            result = value.get(prime_lang)
            if result is not None:
                return result

        if stripped_value is None:
            stripped_value = {}
            for k, v in value.items():
                if '-' in k:
                    stripped_value[k.split('-', 1)[0]] = v
        result = stripped_value.get(prime_lang)
        if result is not None:
            return result

    return value[min(value.keys())]


def uuid():
    """Generate GUID value.

    Function will tranform `uuid.uuid1()` result to leave only alnum symbols.
    The reason is reusing the same resulting GUID in different cases, e.g.,
    for Telepathy names where `-` symbols are not permitted.

    :returns:
        GUID string value

    """
    from uuid import uuid1
    return ''.join(str(uuid1()).split('-'))


def init_logging(debug_level=None, **kwargs):
    # pylint: disable-msg=W0212

    logging.addLevelName(9, 'TRACE')
    logging.addLevelName(8, 'HEARTBEAT')

    if debug_level is None:
        logging_level = logging.getLogger().level
    else:
        logging_level = 0
        if debug_level < 3:
            if debug_level <= 0:
                logging_level = logging.WARNING
            elif debug_level == 1:
                logging_level = logging.INFO
            elif debug_level == 2:
                logging_level = logging.DEBUG
        elif debug_level < 4:
            logging_level = 9
        else:
            logging_level = 8

    def disable_logger(loggers):
        for log_name in loggers:
            logger = logging.getLogger(log_name)
            logger.propagate = False
            logger.addHandler(_NullHandler())

    logging.Logger.trace = lambda self, message, *args, **kwargs: None
    logging.Logger.heartbeat = lambda self, message, *args, **kwargs: None

    if logging_level <= 8:
        logging.Logger.trace = lambda self, message, *args, **kwargs: \
                self._log(9, message, args, **kwargs)
        logging.Logger.heartbeat = lambda self, message, *args, **kwargs: \
                self._log(8, message, args, **kwargs)
    elif logging_level == 9:
        logging.Logger.trace = lambda self, message, *args, **kwargs: \
                self._log(9, message, args, **kwargs)
        disable_logger(['sugar_stats'])
    else:
        disable_logger([
            'requests.packages.urllib3.connectionpool',
            'requests.packages.urllib3.poolmanager',
            'requests.packages.urllib3.response',
            'requests.packages.urllib3',
            'inotify',
            'netlink',
            'sugar_stats',
            '0install',
            ])

    root_logger = logging.getLogger('')
    for i in root_logger.handlers:
        root_logger.removeHandler(i)
    logging.basicConfig(level=logging_level,
            format='%(asctime)s %(levelname)s %(name)s: %(message)s',
            **kwargs)


def ensure_key(path):
    import hashlib
    if not exists(path):
        if 'SSH_ASKPASS' in os.environ:
            # Otherwise ssh-keygen will popup auth dialogs on registeration
            del os.environ['SSH_ASKPASS']
        if not exists(dirname(path)):
            os.makedirs(dirname(path))
        _logger.info('Create DSA key')
        assert_call([
            '/usr/bin/ssh-keygen', '-q', '-t', 'dsa', '-f', path,
            '-C', '', '-N', ''])
    key = pubkey(path).split()[1]
    return str(hashlib.sha1(key).hexdigest())


def pubkey(path):
    with file(path + '.pub') as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('ssh-'):
                return line
    raise RuntimeError('No valid DSA public keys in %r' % path)


def iter_file(*path):
    with file(join(*path), 'rb') as f:
        while True:
            chunk = f.read(BUFFER_SIZE)
            if not chunk:
                return
            yield chunk


def readline(stream, limit=None):
    line = bytearray()
    while limit is None or len(line) < limit:
        char = stream.read(1)
        if not char:
            break
        line.append(char)
        if char == '\n':
            break
    return bytes(line)


def default_route_exists():
    with file('/proc/self/net/route') as f:
        # Skip header
        f.readline()
        while True:
            line = f.readline()
            if not line:
                break
            if int(line.split('\t', 2)[1], 16) == 0:
                return True


def spawn(cmd_filename, *args):
    _logger.trace('Spawn %s%r', cmd_filename, args)

    if os.fork():
        return

    os.execvp(cmd_filename, (cmd_filename,) + args)


def symlink(src, dst):
    if not isfile(src):
        _logger.debug('Cannot link %r to %r, source file is absent', src, dst)
        return

    _logger.trace('Link %r to %r', src, dst)

    if lexists(dst):
        os.unlink(dst)
    elif not exists(dirname(dst)):
        os.makedirs(dirname(dst))
    os.symlink(src, dst)


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
    import subprocess

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
        enforce(returncode == 0, 'Exit status is an error')
        logging.debug('Successfully executed stdout=%r stderr=%r',
                stdout.split('\n'), stderr.split('\n'))
        return stdout
    except Exception, error:
        logging.debug('Failed to execute error="%s" stdout=%r stderr=%r',
                error, str(stdout).split('\n'), str(stderr).split('\n'))
        if asserts:
            if type(cmd) not in (str, unicode):
                cmd = ' '.join(cmd)
            raise RuntimeError('Failed to execute "%s" command: %s' %
                    (cmd, error))
        elif error_cb is not None:
            error_cb(returncode, stdout, stderr)


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
    src = abspath(src)
    dst = abspath(dst)

    def link(src, dst):
        if not exists(dirname(dst)):
            os.makedirs(dirname(dst))

        if islink(src):
            link_to = os.readlink(src)
            os.symlink(link_to, dst)
        elif isdir(src):
            cptree(src, dst)
        elif do_copy:
            # The first hard link was not set, do regular copying for the rest
            shutil.copy(src, dst)
        else:
            if exists(dst) and os.stat(src).st_ino == os.stat(dst).st_ino:
                return
            if os.access(src, os.W_OK):
                try:
                    os.link(src, dst)
                except OSError:
                    do_copy.append(True)
                    shutil.copy(src, dst)
                shutil.copystat(src, dst)
            else:
                # Avoid copystat from not current users
                shutil.copy(src, dst)

    if isdir(src):
        for root, __, files in os.walk(src):
            dst_root = join(dst, root[len(src):].lstrip(os.sep))
            if not exists(dst_root):
                os.makedirs(dst_root)
            for i in files:
                link(join(root, i), join(dst_root, i))
    else:
        link(src, dst)


def new_file(path, mode=0644):
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
    result = _NewFile(dir=dirname(path), prefix=basename(path))
    result.dst_path = path
    os.fchmod(result.fileno(), mode)
    return result


def unique_filename(root, filename):
    path = join(root, filename)
    if exists(path):
        name, suffix = os.path.splitext(filename)
        for dup_num in xrange(1, 255):
            path = join(root, name + '_' + str(dup_num) + suffix)
            if not exists(path):
                break
        else:
            raise RuntimeError('Cannot find unique filename for %r' %
                    join(root, filename))
    return path


class mkdtemp(str):

    def __new__(cls, **kwargs):
        if cachedir.value:
            if not exists(cachedir.value):
                os.makedirs(cachedir.value)
            kwargs['dir'] = cachedir.value
        result = tempfile.mkdtemp(**kwargs)
        return str.__new__(cls, result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self)


def TemporaryFile(*args, **kwargs):
    if cachedir.value:
        if not exists(cachedir.value):
            os.makedirs(cachedir.value)
        kwargs['dir'] = cachedir.value
    return tempfile.TemporaryFile(*args, **kwargs)


class NamedTemporaryFile(object):

    def __init__(self, *args, **kwargs):
        if cachedir.value:
            if not exists(cachedir.value):
                os.makedirs(cachedir.value)
            kwargs['dir'] = cachedir.value
        self._file = tempfile.NamedTemporaryFile(*args, **kwargs)

    def close(self):
        try:
            self._file.close()
        except OSError, error:
            if error.errno != errno.ENOENT:
                raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, name):
        return getattr(self._file, name)


class Seqno(object):
    """Sequence number counter with persistent storing in a file."""

    def __init__(self, path):
        """
        :param path:
            path to file to [re]store seqno value

        """
        self._path = path
        self._value = 0

        if exists(path):
            with file(path) as f:
                self._value = int(f.read().strip())

        self._orig_value = self._value

    @property
    def value(self):
        """Current seqno value."""
        return self._value

    def next(self):
        """Incerement seqno.

        :returns:
            new seqno value

        """
        self._value += 1
        return self._value

    def commit(self):
        """Store current seqno value in a file.

        :returns:
            `True` if commit was happened

        """
        if self._value == self._orig_value:
            return False
        with new_file(self._path) as f:
            f.write(str(self._value))
            f.flush()
            os.fsync(f.fileno())
        self._orig_value = self._value
        return True


class Sequence(list):
    """List of sorted and non-overlapping ranges.

    List items are ranges, [`start`, `stop']. If `start` or `stop`
    is `None`, it means the beginning or ending of the entire scale.

    """

    def __init__(self, value=None, empty_value=None):
        """
        :param value:
            default value to initialize range
        :param empty_value:
            if not `None`, the initial value for empty range

        """
        if empty_value is None:
            self._empty_value = []
        else:
            self._empty_value = [empty_value]

        if value:
            self.extend(value)
        else:
            self.clear()

    def __contains__(self, value):
        for start, end in self:
            if value >= start and (end is None or value <= end):
                return True
        else:
            return False

    @property
    def empty(self):
        """Is timeline in the initial state."""
        return self == self._empty_value

    def clear(self):
        """Reset range to the initial value."""
        self[:] = self._empty_value

    def stretch(self):
        """Remove all holes between the first and the last items."""
        if self:
            self[:] = [[self[0][0], self[-1][-1]]]

    def include(self, start, end=None):
        """Include specified range.

        :param start:
            either including range start or a list of
            (`start`, `end`) pairs
        :param end:
            including range end

        """
        if issubclass(type(start), collections.Iterable):
            for range_start, range_end in start:
                self._include(range_start, range_end)
        elif start is not None:
            self._include(start, end)

    def exclude(self, start, end=None):
        """Exclude specified range.

        :param start:
            either excluding range start or a list of
            (`start`, `end`) pairs
        :param end:
            excluding range end

        """
        if issubclass(type(start), collections.Iterable):
            for range_start, range_end in start:
                self._exclude(range_start, range_end)
        else:
            enforce(end is not None)
            self._exclude(start, end)

    def _include(self, range_start, range_end):
        if range_start is None:
            range_start = 1

        range_start_new = None
        range_start_i = 0

        for range_start_i, (start, end) in enumerate(self):
            if range_end is not None and start - 1 > range_end:
                break
            if (range_end is None or start - 1 <= range_end) and \
                    (end is None or end + 1 >= range_start):
                range_start_new = min(start, range_start)
                break
        else:
            range_start_i += 1

        if range_start_new is None:
            self.insert(range_start_i, [range_start, range_end])
            return

        range_end_new = range_end
        range_end_i = range_start_i
        for i, (start, end) in enumerate(self[range_start_i:]):
            if range_end is not None and start - 1 > range_end:
                break
            if range_end is None or end is None:
                range_end_new = None
            else:
                range_end_new = max(end, range_end)
            range_end_i = range_start_i + i

        del self[range_start_i:range_end_i]
        self[range_start_i] = [range_start_new, range_end_new]

    def _exclude(self, range_start, range_end):
        if range_start is None:
            range_start = 1
        enforce(range_end is not None)
        enforce(range_start <= range_end and range_start > 0,
                'Start value %r is less than 0 or not less than %r',
                range_start, range_end)

        for i, interval in enumerate(self):
            start, end = interval

            if end is not None and end < range_start:
                # Current `interval` is below new one
                continue

            if range_end is not None and range_end < start:
                # Current `interval` is above new one
                continue

            if end is None or end > range_end:
                # Current `interval` will exist after changing
                self[i] = [range_end + 1, end]
                if start < range_start:
                    self.insert(i, [start, range_start - 1])
            else:
                if start < range_start:
                    self[i] = [start, range_start - 1]
                else:
                    del self[i]

            if end is not None:
                range_start = end + 1
                if range_start < range_end:
                    self.exclude(range_start, range_end)
            break


class PersistentSequence(Sequence):

    def __init__(self, path, empty_value=None):
        Sequence.__init__(self, empty_value=empty_value)
        self._path = path

        if exists(self._path):
            with file(self._path) as f:
                self[:] = json.load(f)

    @property
    def mtime(self):
        if exists(self._path):
            return os.stat(self._path).st_mtime
        else:
            return 0

    def commit(self):
        dir_path = dirname(self._path)
        if dir_path and not exists(dir_path):
            os.makedirs(dir_path)
        with new_file(self._path) as f:
            json.dump(self, f)
            f.flush()
            os.fsync(f.fileno())


class Pool(object):
    """Stack that keeps its iterators correct after changing content."""

    QUEUED = 0
    ACTIVE = 1
    PASSED = 2

    def __init__(self):
        self._queue = collections.deque()

    def add(self, value):
        self.remove(value)
        self._queue.appendleft([Pool.QUEUED, value])

    def remove(self, value):
        for i, (state, existing) in enumerate(self._queue):
            if existing == value:
                del self._queue[i]
                return state

    def get_state(self, value):
        for state, existing in self._queue:
            if existing == value:
                return state

    def rewind(self):
        for i in self._queue:
            i[0] = Pool.QUEUED

    def __len__(self):
        return len(self._queue)

    def __iter__(self):
        for i in self._queue:
            state, value = i
            if state == Pool.PASSED:
                continue
            try:
                i[0] = Pool.ACTIVE
                yield value
            finally:
                i[0] = Pool.PASSED

    def __repr__(self):
        return str([i[1] for i in self._queue])


class _NullHandler(logging.Handler):

    def emit(self, record):
        pass


class _NewFile(object):

    dst_path = None

    def __init__(self, **kwargs):
        self._file = tempfile.NamedTemporaryFile(delete=False, **kwargs)

    @property
    def name(self):
        return self._file.name

    def close(self):
        self._file.close()
        if exists(self.name):
            os.rename(self.name, self.dst_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, name):
        return getattr(self._file.file, name)


def _nb_read(stream):
    import fcntl

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


_default_lang = None
