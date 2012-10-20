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
import time
import uuid
import random
import hashlib
import logging
from tempfile import NamedTemporaryFile

import active_document as ad
from sugar_network import local
from sugar_network.toolkit import sugar, router
from active_toolkit.sockets import BUFFER_SIZE
from active_toolkit import enforce


_logger = logging.getLogger('local.journal')
_ds = None


def create_activity_id():
    data = '%s%s%s' % (
            time.time(),
            random.randint(10000, 100000),
            uuid.getnode())
    return hashlib.sha1(data).hexdigest()


def exists(guid):
    path = sugar.profile_path('datastore', guid[:2], guid)
    return os.path.exists(path)


def ds_path(guid, prop):
    return sugar.profile_path('datastore', guid[:2], guid, 'metadata', prop)


def get(guid, prop):
    path = ds_path(guid, prop)
    if not os.path.exists(path):
        return None
    with file(path, 'rb') as f:
        return f.read()


class Commands(object):

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
            self._ds = None

    @router.route('GET', '/journal')
    def journal(self, request, response):
        enforce(self._ds is not None, 'Journal is inaccessible')
        enforce(len(request.path) <= 3, 'Invalid request')

        def preview_url(guid):
            return 'http://localhost:%s/journal/%s/preview' % \
                    (local.ipc_port.value, guid)

        if len(request.path) == 1:
            if 'order_by' in request:
                request['order_by'] = [request['order_by']]
            items, total = self._ds.find(request,
                    ['uid', 'title', 'description'], byte_arrays=True)
            result = []
            for item in items:
                guid = str(item['uid'])
                result.append({
                        'guid': guid,
                        'title': str(item['title']),
                        'description': str(item['description']),
                        'preview': preview_url(guid),
                        })
            response.content_type = 'application/json'
            return {'result': result, 'total': int(total)}

        elif len(request.path) == 2:
            guid = request.path[1]
            response.content_type = 'application/json'
            return {'guid': guid,
                    'title': get(guid, 'title'),
                    'description': get(guid, 'description'),
                    'preview': preview_url(guid),
                    }

        elif len(request.path) == 3:
            guid = request.path[1]
            prop = request.path[2]
            if prop == 'preview':
                return ad.PropertyMeta(path=ds_path(guid, prop),
                        mime_type='image/png')
            else:
                response.content_type = 'application/json'
                return get(guid, prop)

    def journal_update(self, guid, title, description, preview, data):
        enforce(self._ds is not None, 'Journal is inaccessible')

        if hasattr(preview, 'read'):
            preview = preview.read()
            if hasattr(preview, 'close'):
                preview.close()
        else:
            with file(preview['path'], 'rb') as f:
                preview = f.read()

        if hasattr(data, 'read'):
            with NamedTemporaryFile(delete=False) as f:
                while True:
                    chunk = data.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                data = f.name
                transfer_ownership = True
        else:
            data = data['path']
            transfer_ownership = False

        self._ds.update(guid, {
            'title': title,
            'description': description,
            'preview': preview,
            }, data, transfer_ownership)
