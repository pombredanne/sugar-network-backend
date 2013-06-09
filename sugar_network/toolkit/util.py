# Copyright (C) 2011-2012 Aleksey Lim
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

"""Swiss knife module."""

import os
import re
import json
import logging
import hashlib
import tempfile
import collections
from os.path import exists, join, islink, isdir, dirname, basename, abspath
from os.path import lexists, isfile

from sugar_network.toolkit import BUFFER_SIZE, cachedir, exception, enforce


_VERSION_RE = re.compile('-([a-z]*)')
_VERSION_MOD_TO_VALUE = {
        'pre': -2,
        'rc': -1,
        '': 0,
        'post': 1,
        'r': 1,
        }
_VERSION_VALUE_TO_MOD = {}

_logger = logging.getLogger('toolkit.util')


def init_logging(debug_level):
    # pylint: disable-msg=W0212

    logging.addLevelName(9, 'TRACE')
    logging.addLevelName(8, 'HEARTBEAT')

    logging.Logger.trace = lambda self, message, *args, **kwargs: None
    logging.Logger.heartbeat = lambda self, message, *args, **kwargs: None

    if debug_level < 3:
        _disable_logger([
            'requests.packages.urllib3.connectionpool',
            'requests.packages.urllib3.poolmanager',
            'requests.packages.urllib3.response',
            'requests.packages.urllib3',
            'inotify',
            'netlink',
            'sugar_stats',
            '0install',
            ])
    elif debug_level < 4:
        logging.Logger.trace = lambda self, message, *args, **kwargs: \
                self._log(9, message, args, **kwargs)
        _disable_logger(['sugar_stats'])
    else:
        logging.Logger.heartbeat = lambda self, message, *args, **kwargs: \
                self._log(8, message, args, **kwargs)


def ensure_key(path):
    if not exists(path):
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


def parse_version(version_string):
    """Convert a version string to an internal representation.

    The parsed format can be compared quickly using the standard Python
    functions. Adapted Zero Install version.

    :param version_string:
        version in format supported by 0install
    :returns:
        array of arrays of integers

    """
    if version_string is None:
        return None

    parts = _VERSION_RE.split(version_string)
    if parts[-1] == '':
        del parts[-1]  # Ends with a modifier
    else:
        parts.append('')
    enforce(parts, ValueError, 'Empty version string')

    length = len(parts)
    try:
        for x in range(0, length, 2):
            part = parts[x]
            if part:
                parts[x] = [int(i or '0') for i in part.split('.')]
            else:
                parts[x] = []  # (because ''.split('.') == [''], not [])
        for x in range(1, length, 2):
            parts[x] = _VERSION_MOD_TO_VALUE[parts[x]]
        return parts
    except ValueError as error:
        exception()
        raise RuntimeError('Invalid version format in "%s": %s' %
                (version_string, error))
    except KeyError as error:
        raise RuntimeError('Invalid version modifier in "%s": %s' %
                (version_string, error))


def format_version(version):
    """Convert internal version representation back to string."""
    if version is None:
        return None

    if not _VERSION_VALUE_TO_MOD:
        for mod, value in _VERSION_MOD_TO_VALUE.items():
            _VERSION_VALUE_TO_MOD[value] = mod

    version = version[:]
    length = len(version)

    for x in range(0, length, 2):
        version[x] = '.'.join([str(i) for i in version[x]])
    for x in range(1, length, 2):
        version[x] = '-' + _VERSION_VALUE_TO_MOD[version[x]]
    if version[-1] == '-':
        del version[-1]

    return ''.join(version)


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


def svg_to_png(src_path, dst_path, width, height):
    import rsvg
    import cairo

    svg = rsvg.Handle(src_path)

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    context = cairo.Context(surface)
    scale = min(
            float(width) / svg.props.width,
            float(height) / svg.props.height)
    context.scale(scale, scale)
    svg.render_cairo(context)

    surface.write_to_png(dst_path)


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
    import shutil

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


def TemporaryFile(*args, **kwargs):
    if cachedir.value:
        if not exists(cachedir.value):
            os.makedirs(cachedir.value)
        kwargs['dir'] = cachedir.value
    return tempfile.TemporaryFile(*args, **kwargs)


def NamedTemporaryFile(*args, **kwargs):
    if cachedir.value:
        if not exists(cachedir.value):
            os.makedirs(cachedir.value)
        kwargs['dir'] = cachedir.value
    return tempfile.NamedTemporaryFile(*args, **kwargs)


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


def _disable_logger(loggers):
    for log_name in loggers:
        logger = logging.getLogger(log_name)
        logger.propagate = False
        logger.addHandler(_NullHandler())
