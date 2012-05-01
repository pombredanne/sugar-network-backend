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
from os.path import join, exists, lexists, islink, relpath, dirname
from gettext import gettext as _

import sweets_recipe
from local_document import crawler, env


_logger = logging.getLogger('local_document.activities')


def checkouts(context):
    root = env.path('activities', 'context', context)
    if not exists(root):
        return

    for filename in os.listdir(root):
        path = join(root, filename)
        if not islink(path):
            continue

        spec_path = join(os.readlink(path), 'activity', 'activity.info')
        try:
            yield sweets_recipe.Spec(spec_path)
        except Exception, error:
            _logger.warning(_('Cannot read %r spec: %s'), spec_path, error)


def monitor(mounts):
    with _Monitor(mounts):
        crawler.dispatch([env.activities_root.value])


class _Monitor(object):

    def __init__(self, mounts):
        self._mounts = mounts

    def __enter__(self):
        crawler.found.connect(self.__found_cb)
        crawler.lost.connect(self.__lost_cb)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        crawler.found.disconnect(self.__found_cb)
        crawler.lost.disconnect(self.__lost_cb)

    def __found_cb(self, impl_path):
        hashed_path, checkin_path = _checkin_path(impl_path)
        if exists(checkin_path):
            return

        _logger.debug('Checking in activity from %r', impl_path)

        spec_path = join(impl_path, 'activity', 'activity.info')
        try:
            spec = sweets_recipe.Spec(spec_path)
        except Exception, error:
            _logger.warning(_('Cannot read %r spec: %s'), spec_path, error)
            return

        context = spec['Activity', 'bundle_id']
        OfflineContext = self._mounts['~'].resources['context']

        if not OfflineContext(context).exists:
            _logger.debug('Register unknown local activity, %r', context)
            OfflineContext.create_with_guid(context, {
                'type': 'activity',
                'title': spec['name'],
                'summary': spec['summary'],
                'description': spec['description'],
                })

        context_path = _context_path(context, hashed_path)
        if lexists(context_path):
            os.unlink(context_path)
        os.symlink(impl_path, context_path)

        if lexists(checkin_path):
            os.unlink(checkin_path)
        env.ensure_path(checkin_path)
        os.symlink(relpath(context_path, dirname(checkin_path)), checkin_path)

    def __lost_cb(self, impl_path):
        __, checkin_path = _checkin_path(impl_path)
        if not lexists(checkin_path):
            return

        _logger.debug('Checking out activity from %r', impl_path)

        context_path = join(dirname(checkin_path), os.readlink(checkin_path))
        if lexists(context_path):
            os.unlink(context_path)
        os.unlink(checkin_path)


def _checkin_path(impl_path):
    hashed_path = hashlib.sha1(impl_path).hexdigest()
    return hashed_path, env.path('activities', 'checkouts', hashed_path)


def _context_path(context, hashed_path):
    return env.ensure_path('activities', 'context', context, hashed_path)
