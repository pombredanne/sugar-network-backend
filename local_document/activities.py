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

import os
import hashlib
import logging
import tempfile
from os.path import join, exists, lexists, relpath, dirname, basename
from gettext import gettext as _

import sweets_recipe
from local_document import crawler, env, sugar
from active_toolkit import util


_logger = logging.getLogger('local_document.activities')


def path_to_guid(path):
    return hashlib.sha1(path).hexdigest()


def guid_to_path(impl_id):
    path = env.path('activities', 'checkins', impl_id)
    if lexists(path):
        return _read_checkin_path(path)


def checkins(context):
    root = _context_path(context, '')
    if not exists(root):
        return

    for filename in os.listdir(root):
        path = join(root, filename)
        if exists(path):
            yield os.readlink(path)


def monitor(volume, paths):
    _Monitor(volume, paths).serve_forever()


def populate(volume, paths, prefix):
    _Monitor(volume, paths, prefix).populate()


class _Monitor(object):

    def __init__(self, volume, paths, prefix=''):
        self._volume = volume
        self._paths = paths
        self._prefix = prefix

    def serve_forever(self):
        crawler.dispatch(self._paths, self.__found_cb, self.__lost_cb)

    def populate(self):
        crawler.populate(self._paths, self.__found_cb, self.__lost_cb)

    def __found_cb(self, impl_path):
        hashed_path, checkin_path = _checkin_path(impl_path)
        if exists(checkin_path):
            return

        _logger.debug('Checking in activity from %r', impl_path)

        try:
            spec = sweets_recipe.Spec(root=impl_path)
        except Exception, error:
            util.exception(_logger, _('Cannot read %r spec: %s'),
                    impl_path, error)
            return

        context = spec['Activity', 'bundle_id']
        directory = self._volume['context']
        if directory.exists(context):
            directory.update(context, {'keep_impl': 2})
        else:
            _logger.debug('Register unknown local activity, %r', context)

            directory.create_with_guid(context, {
                'type': 'activity',
                'title': spec['name'],
                'summary': spec['summary'],
                'description': spec['description'],
                'keep_impl': 2,
                'user': [sugar.uid()],
                })

            icon_path = join(spec.root, spec['icon'])
            if exists(icon_path):
                directory.set_blob(context, 'artifact_icon', icon_path)
                with tempfile.NamedTemporaryFile() as f:
                    _svg_to_png(icon_path, f.name, 32, 32)
                    directory.set_blob(context, 'icon', f.name)

        context_path = _ensure_context_path(context, hashed_path)
        if lexists(context_path):
            os.unlink(context_path)
        os.symlink(impl_path[len(self._prefix):], context_path)

        if lexists(checkin_path):
            os.unlink(checkin_path)
        env.ensure_path(checkin_path)
        os.symlink(relpath(context_path, dirname(checkin_path)), checkin_path)

    def __lost_cb(self, impl_path):
        __, checkin_path = _checkin_path(impl_path)
        if not lexists(checkin_path):
            return

        _logger.debug('Checking out activity from %r', impl_path)

        context_path = _read_checkin_path(checkin_path)
        context_dir = dirname(context_path)
        impls = set(os.listdir(context_dir)) - set([basename(context_path)])

        if not impls:
            context = basename(context_dir)
            directory = self._volume['context']
            if directory.exists(context):
                directory.update(context, {'keep_impl': 0})

        if lexists(context_path):
            os.unlink(context_path)
        os.unlink(checkin_path)


def _checkin_path(impl_path):
    hashed_path = path_to_guid(impl_path)
    return hashed_path, env.path('activities', 'checkins', hashed_path)


def _read_checkin_path(checkin_path):
    return join(dirname(checkin_path), os.readlink(checkin_path))


def _context_path(context, hashed_path):
    return env.path('activities', 'context', context, hashed_path)


def _ensure_context_path(context, hashed_path):
    return env.ensure_path('activities', 'context', context, hashed_path)


def _svg_to_png(src_path, dst_path, w, h):
    import rsvg
    import cairo

    svg = rsvg.Handle(src_path)

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, w, h)
    context = cairo.Context(surface)
    scale = min(float(w) / svg.props.width, float(h) / svg.props.height)
    context.scale(scale, scale)
    svg.render_cairo(context)

    surface.write_to_png(dst_path)
