# Copyright (C) 2011-2014 Aleksey Lim
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
from cStringIO import StringIO
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


def ascii(value):
    if not isinstance(value, basestring):
        return str(value)
    if isinstance(value, unicode):
        return value.encode('utf8')
    return value


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

    if logging_level <= 8:
        logging.Logger.trace = lambda self, message, *args, **kwargs: \
                self._log(9, message, args, **kwargs)
        logging.Logger.heartbeat = lambda self, message, *args, **kwargs: \
                self._log(8, message, args, **kwargs)
    elif logging_level == 9:
        logging.Logger.trace = lambda self, message, *args, **kwargs: \
                self._log(9, message, args, **kwargs)
    else:
        for log_name in (
                'requests.packages.urllib3.connectionpool',
                'requests.packages.urllib3.poolmanager',
                'requests.packages.urllib3.response',
                'requests.packages.urllib3',
                'inotify',
                'netlink',
                ):
            logger = logging.getLogger(log_name)
            logger.propagate = False
            logger.addHandler(_NullHandler())

    root_logger = logging.getLogger('')
    for i in root_logger.handlers:
        root_logger.removeHandler(i)
    logging.basicConfig(level=logging_level,
            format='%(asctime)s %(levelname)s %(name)s: %(message)s',
            **kwargs)

    def exception(self, *args):
        from traceback import format_exception

        klass, error, tb = sys.exc_info()
        tb = [i.rstrip() for i in format_exception(klass, error, tb)]
        error_message = str(error) or '%s exception' % type(error).__name__
        if args:
            if len(args) == 1:
                message = args[0]
            else:
                message = args[0] % args[1:]
            error_message = '%s: %s' % (message, error_message)

        self.error(error_message)
        self.debug('\n'.join(tb))

    logging.Logger.exception = exception


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
    dirpath = dirname(path)
    if not exists(dirpath):
        os.makedirs(dirpath)
    result = _NewFile(dir=dirpath, prefix=basename(path))
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

    def __new__(cls, *args, **kwargs):
        if 'dir' not in kwargs:
            kwargs['dir'] = cachedir.value
        if not exists(kwargs['dir']):
            os.makedirs(kwargs['dir'])
        result = tempfile.mkdtemp(*args, **kwargs)
        return str.__new__(cls, result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exists(self):
            shutil.rmtree(self)


def svg_to_png(data, w, h=None):
    import rsvg
    import cairo

    if h is None:
        h = w

    svg = rsvg.Handle(data=data)
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, w, h)
    context = cairo.Context(surface)

    scale = min(float(w) / svg.props.width, float(h) / svg.props.height)
    context.translate(
            int(w - svg.props.width * scale) / 2,
            int(h - svg.props.height * scale) / 2)
    context.scale(scale, scale)
    svg.render_cairo(context)

    result = StringIO()
    surface.write_to_png(result)
    result.seek(0)

    return result


def TemporaryFile(*args, **kwargs):
    if 'dir' not in kwargs:
        kwargs['dir'] = cachedir.value
    if not exists(kwargs['dir']):
        os.makedirs(kwargs['dir'])
    return tempfile.TemporaryFile(*args, **kwargs)


class NamedTemporaryFile(object):

    def __init__(self, *args, **kwargs):
        if 'dir' not in kwargs:
            kwargs['dir'] = cachedir.value
        if not exists(kwargs['dir']):
            os.makedirs(kwargs['dir'])
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


class Variable(list):

    def __init__(self, default=None):
        list.__init__(self, [default])

    @property
    def value(self):
        return self[0]

    @value.setter
    def value(self, value):
        self[0] = value

    def __contains__(self, key):
        return key in self[0]

    def __getitem__(self, key):
        return self[0].get(key)

    def __setitem__(self, key, value):
        self[0][key] = value

    def __delitem__(self, key):
        del self[0][key]

    def __getattr__(self, name):
        return getattr(self[0], name)


class Bin(object):
    """Store variable in a file."""

    def __init__(self, path, default_value=None):
        self._path = abspath(path)
        self.value = default_value

        if not self.reset():
            self.commit()

    @property
    def mtime(self):
        if exists(self._path):
            return int(os.stat(self._path).st_mtime)
        else:
            return 0

    def commit(self):
        """Store current value in a file."""
        with new_file(self._path) as f:
            json.dump(self.value, f)
            f.flush()
            os.fsync(f.fileno())

    def reset(self):
        if not exists(self._path):
            return False
        with file(self._path) as f:
            self.value = json.load(f)
        return True

    def __enter__(self):
        return self.value

    def __exit__(self, exc_type, exc_value, traceback):
        self.commit()

    def __contains__(self, key):
        return key in self.value

    def __getitem__(self, key):
        return self.value.get(key)

    def __setitem__(self, key, value):
        self.value[key] = value

    def __delitem__(self, key):
        del self.value[key]

    def __getattr__(self, name):
        return getattr(self.value, name)


class Seqno(Bin):
    """Sequence number counter with persistent storing in a file."""

    def __init__(self, path):
        """
        :param path:
            path to file to [re]store seqno value

        """
        Bin.__init__(self, path, 0)

    def next(self):
        """Incerement seqno.

        :returns:
            new seqno value

        """
        self.value += 1
        return self.value


class CaseInsensitiveDict(dict):

    def __contains__(self, key):
        return dict.__contains__(self, key.lower())

    def __getitem__(self, key):
        return self.get(key.lower())

    def __setitem__(self, key, value):
        return self.set(key.lower(), value)

    def __delitem__(self, key):
        self.remove(key.lower())

    def get(self, key, default=None):
        return dict.get(self, key, default)

    def set(self, key, value):
        dict.__setitem__(self, key, value)

    def remove(self, key):
        dict.__delitem__(self, key)


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
        self._file = NamedTemporaryFile(delete=False, **kwargs)

    @property
    def name(self):
        return self._file.name

    @name.setter
    def name(self, value):
        self.dst_path = value

    def tell(self):
        return self._file.file.tell()

    def close(self):
        self._file.close()
        if exists(self.name):
            if not exists(dirname(self.dst_path)):
                os.makedirs(dirname(self.dst_path))
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


logging.Logger.trace = lambda self, message, *args, **kwargs: None
logging.Logger.heartbeat = lambda self, message, *args, **kwargs: None
