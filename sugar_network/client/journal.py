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
import sys
import time
import uuid
import random
import hashlib
import logging
from shutil import copyfileobj
from tempfile import NamedTemporaryFile

from sugar_network import db, client
from sugar_network.toolkit import enforce


_logger = logging.getLogger('client.journal')
_ds_root = client.profile_path('datastore')


def create_activity_id():
    data = '%s%s%s' % (
            time.time(),
            random.randint(10000, 100000),
            uuid.getnode())
    return hashlib.sha1(data).hexdigest()


def exists(guid):
    return os.path.exists(_ds_path(guid))


def get(guid, prop):
    path = _prop_path(guid, prop)
    if not os.path.exists(path):
        return None
    with file(path, 'rb') as f:
        return f.read()


class Commands(object):

    _ds = None

    def __init__(self):
        import dbus
        try:
            self._ds = dbus.Interface(
                dbus.SessionBus().get_object(
                    'org.laptop.sugar.DataStore',
                    '/org/laptop/sugar/DataStore'),
                'org.laptop.sugar.DataStore')
        except dbus.DBusException:
            _logger.info(
                    'Cannot connect to sugar-datastore, '
                    'Journal integration is disabled')

    @db.route('GET', '/journal')
    def journal(self, request, response):
        enforce(self._ds is not None, 'Journal is inaccessible')
        enforce(len(request.path) <= 3, 'Invalid request')

        if len(request.path) == 1:
            return self._find(request, response)
        elif len(request.path) == 2:
            return self._get(request, response)
        elif len(request.path) == 3:
            return self._get_prop(request, response)

    @db.route('PUT', '/journal')
    def journal_share(self, request, response):
        enforce(self._ds is not None, 'Journal is inaccessible')
        enforce(len(request.path) == 2 and request.get('cmd') == 'share',
                'Invalid request')

        guid = request.path[1]
        preview_path = _prop_path(guid, 'preview')
        enforce(os.access(preview_path, os.R_OK), 'No preview')
        data_path = _ds_path(guid, 'data')
        enforce(os.access(data_path, os.R_OK), 'No data')

        subrequest = db.Request(method='POST', document='artifact')
        subrequest.content = request.content
        subrequest.content_type = 'application/json'
        # pylint: disable-msg=E1101
        subguid = self.call(subrequest, response)

        subrequest = db.Request(method='PUT', document='artifact',
                guid=subguid, prop='preview')
        subrequest.content_type = 'image/png'
        with file(preview_path, 'rb') as subrequest.content_stream:
            self.call(subrequest, response)

        subrequest = db.Request(method='PUT', document='artifact',
                guid=subguid, prop='data')
        subrequest.content_type = get(guid, 'mime_type') or 'application/octet'
        with file(data_path, 'rb') as subrequest.content_stream:
            self.call(subrequest, response)

    def journal_update(self, guid, data=None, **kwargs):
        enforce(self._ds is not None, 'Journal is inaccessible')

        preview = kwargs.get('preview')
        if preview:
            if hasattr(preview, 'read'):
                preview = preview.read()
                if hasattr(preview, 'close'):
                    preview.close()
            elif isinstance(preview, dict):
                with file(preview['blob'], 'rb') as f:
                    preview = f.read()
            import dbus
            kwargs['preview'] = dbus.ByteArray(preview)

        if hasattr(data, 'read'):
            with NamedTemporaryFile(delete=False) as f:
                copyfileobj(data, f)
                data = f.name
                transfer_ownership = True
        elif isinstance(data, dict):
            data = data['blob']
            transfer_ownership = False
        elif data is not None:
            with NamedTemporaryFile(delete=False) as f:
                f.write(data)
                data = f.name
                transfer_ownership = True

        self._ds.update(guid, kwargs, data or '', transfer_ownership)

    def journal_delete(self, guid):
        enforce(self._ds is not None, 'Journal is inaccessible')
        self._ds.delete(guid)

    def _find(self, request, response):
        import dbus

        if 'order_by' in request:
            request['order_by'] = [request['order_by']]
        for key in ('offset', 'limit'):
            if key in request:
                request[key] = int(request[key])
        if 'reply' in request:
            reply = db.to_list(request.pop('reply'))
        else:
            reply = ['uid', 'title', 'description', 'preview']
        if 'preview' in reply:
            reply.remove('preview')
            has_preview = True
        else:
            has_preview = False
        for key in ('timestamp', 'filesize', 'creation_time'):
            value = request.get(key)
            if not value or '..' not in value:
                continue
            start, end = value.split('..', 1)
            value = {'start': start or '0', 'end': end or str(sys.maxint)}
            request[key] = dbus.Dictionary(value)
        if 'uid' not in reply:
            reply.append('uid')

        result, total = self._ds.find(request, reply, byte_arrays=True)

        for item in result:
            # Do not break SN like API
            guid = item['guid'] = item.pop('uid')
            if has_preview:
                item['preview'] = _preview_url(guid)

        response.content_type = 'application/json'
        return {'result': result, 'total': int(total)}

    def _get(self, request, response):
        guid = request.path[1]
        response.content_type = 'application/json'
        return {'guid': guid,
                'title': get(guid, 'title'),
                'description': get(guid, 'description'),
                'preview': _preview_url(guid),
                }

    def _get_prop(self, request, response):
        guid = request.path[1]
        prop = request.path[2]

        if prop == 'preview':
            return db.PropertyMetadata(blob=_prop_path(guid, prop),
                    mime_type='image/png')
        elif prop == 'data':
            return db.PropertyMetadata(blob=_ds_path(guid, 'data'),
                    mime_type=get(guid, 'mime_type') or 'application/octet')
        else:
            response.content_type = 'application/json'
            return get(guid, prop)


def _ds_path(guid, *args):
    return os.path.join(_ds_root, guid[:2], guid, *args)


def _prop_path(guid, prop):
    return _ds_path(guid, 'metadata', prop)


def _preview_url(guid):
    return 'http://localhost:%s/journal/%s/preview' % \
            (client.ipc_port.value, guid)
