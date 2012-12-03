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

import os
import json
import logging
import hashlib
import tempfile
import collections
from os.path import isfile, lexists, exists, dirname

import active_document as ad
from active_toolkit.options import Option
from active_toolkit import util


tmpdir = Option(
        'if specified, use this directory for temporary files; such files '
        'might take considerable number of bytes while downloading of '
        'synchronizing Sugar Network content',
        name='tmpdir')

_logger = logging.getLogger('toolkit')


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


def ensure_dsa_pubkey(path):
    if not exists(path):
        _logger.info('Create DSA server key')
        util.assert_call([
            '/usr/bin/ssh-keygen', '-q', '-t', 'dsa', '-f', path,
            '-C', '', '-N', ''])

    with file(path + '.pub') as f:
        for line in f:
            line = line.strip()
            if line.startswith('ssh-'):
                key = line.split()[1]
                return str(hashlib.sha1(key).hexdigest())

    raise RuntimeError('No valid DSA public key in %r' % path)


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


def NamedTemporaryFile(*args, **kwargs):
    if tmpdir.value:
        kwargs['dir'] = tmpdir.value
    return tempfile.NamedTemporaryFile(*args, **kwargs)


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
            ])
    elif debug_level < 4:
        logging.Logger.trace = lambda self, message, *args, **kwargs: \
                self._log(9, message, args, **kwargs)
        _disable_logger(['sugar_stats'])
    else:
        logging.Logger.heartbeat = lambda self, message, *args, **kwargs: \
                self._log(8, message, args, **kwargs)


class PersistentSequence(ad.Sequence):

    def __init__(self, path, empty_value=None):
        ad.Sequence.__init__(self, empty_value=empty_value)
        self._path = path

        if exists(self._path):
            with file(self._path) as f:
                self[:] = json.load(f)

    def commit(self):
        dir_path = dirname(self._path)
        if dir_path and not exists(dir_path):
            os.makedirs(dir_path)
        with util.new_file(self._path) as f:
            json.dump(self, f)
            f.flush()
            os.fsync(f.fileno())


class MutableStack(object):
    """Stack that keeps its iterators correct after changing content."""

    def __init__(self):
        self._queue = collections.deque()

    def add(self, value):
        self.remove(value)
        self._queue.appendleft([False, value])

    def remove(self, value):
        for i, (__, existing) in enumerate(self._queue):
            if existing == value:
                del self._queue[i]
                break

    def rewind(self):
        for i in self._queue:
            i[0] = False

    def __len__(self):
        return len(self._queue)

    def __iter__(self):
        return _MutableStackIterator(self._queue)

    def __repr__(self):
        return str([i[1] for i in self._queue])


class _MutableStackIterator(object):

    def __init__(self, queue):
        self._queue = queue

    def next(self):
        for i in self._queue:
            processed, value = i
            if not processed:
                i[0] = True
                return value
        raise StopIteration()


def _disable_logger(loggers):
    for log_name in loggers:
        logger = logging.getLogger(log_name)
        logger.propagate = False
        logger.addHandler(_NullHandler())


class _NullHandler(logging.Handler):

    def emit(self, record):
        pass
