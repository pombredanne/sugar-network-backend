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
import logging
import hashlib
from os.path import isfile, lexists, exists, dirname

from active_toolkit import util


_logger = logging.getLogger('toolkit')


def spawn(cmd_filename, *args):
    _logger.debug('Spawn %s%r', cmd_filename, args)

    if os.fork():
        return

    os.execvp(cmd_filename, (cmd_filename,) + args)


def symlink(src, dst):
    if not isfile(src):
        _logger.debug('Cannot link %r to %r, source file is absent', src, dst)
        return

    _logger.debug('Link %r to %r', src, dst)

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
