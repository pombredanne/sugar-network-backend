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

import logging
from gettext import gettext as _

import active_document as ad
from active_document import principal, SingleVolume, enforce
from local_document import cache, sugar, http


_logger = logging.getLogger('local_document.mounts')


class Mounts(dict):

    def __init__(self, root, resources_path):
        principal.user = sugar.uid()

        self._home_volume = SingleVolume(root, resources_path, {
            'context': [
                ad.ActiveProperty('keep',
                    prefix='LK', typecast=bool, default=False),
                ad.ActiveProperty('keep_impl',
                    prefix='LI', typecast=bool, default=False),
                ad.StoredProperty('position',
                    typecast=[int], default=(-1, -1)),
                ],
            })

        self['/'] = _RemoteMount(self._home_volume)
        self['~'] = _LocalMount(self._home_volume)

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, _('Unknown mountpoint %r'), mountpoint)
        return self.get(mountpoint)

    def call(self, request, response):
        mount = self[request.pop('mountpoint')]
        return mount.call(request, response)

    def close(self):
        self._home_volume.close()


class _LocalMount(object):

    def __init__(self, volume):
        self.volume = volume

    def call(self, request, response):
        if request.command == 'get_blob':
            return self._get_blob(**request)
        return ad.call(self.volume, request, response)

    def connect(self, callback):
        pass

    def _get_blob(self, document, guid, prop):
        stat = self.volume[document].stat_blob(guid, prop)
        if stat is None:
            return None
        return {'path': stat['path'], 'mime_type': stat['mime_type']}


class _RemoteMount(_LocalMount):

    def call(self, request, response):
        if request.command == 'set_keep':
            return self._set_keep(request['guid'], request['keep'])
        elif request.command == 'get_blob':
            return self._get_blob(**request)

        if type(request.command) is list:
            method, request['cmd'] = request.command
        else:
            method = request.command

        path = [request.pop('document')]
        if 'guid' in request:
            path.append(request.pop('guid'))
        if 'prop' in request:
            path.append(request.pop('prop'))

        return http.request(method, path, data=request.content,
                params=request, headers={'Content-Type': 'application/json'})

    def _set_keep(self, guid, keep):
        context = self.volume['context']
        if context.exists(guid):
            context.update(guid, {'keep': keep})
        elif keep:
            props = http.request('GET', ['context', guid])
            props['keep'] = keep
            context.create_with_guid(guid, props)

    def _get_blob(self, document, guid, prop):
        path, mime_type = cache.get_blob(document, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}