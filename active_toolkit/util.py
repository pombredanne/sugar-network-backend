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

"""Swiss knife module.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/util.py$
$Date: 2012-08-05$

"""

import os
import sys
import logging
import subprocess
from os.path import exists, join, islink, isdir, dirname, basename, abspath


if sys.version_info[:3] >= (2, 6, 5):
    normalize_kwargs = lambda x: x
else:

    def normalize_kwargs(kwargs):
        # To workaround issue when unicode might appear in kwargs' keys
        # http://bugs.python.org/issue2646
        result = {}
        for key, value in kwargs.items():
            result[str(key)] = value
        return result


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


class _NewFile(object):

    dst_path = None

    def __init__(self, **kwargs):
        import tempfile
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
