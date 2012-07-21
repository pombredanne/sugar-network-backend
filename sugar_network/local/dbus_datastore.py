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
import tempfile
from cStringIO import StringIO
from os.path import exists

import dbus
from dbus.service import BusName, method, signal

from sugar_network.toolkit import sugar, dbus_thread
from active_toolkit import util


_SERVICE = 'org.laptop.sugar.DataStore'
_INTERFACE = 'org.laptop.sugar.DataStore'
_OBJECT_PATH = '/org/laptop/sugar/DataStore'

_MAP_PROPS = {
        # ds_prop: (sn_prop, sn2ds_convert, ds2sn_convert)
        'uid': ('guid', lambda x: x, str),
        'activity': ('context', lambda x: x, str),
        'activity_id': ('activity_id', lambda x: x, str),
        'title': ('title', lambda x: x, str),
        'description': ('description', lambda x: x, str),
        'keep': ('keep', lambda x: str(int(x)), lambda x: x and x != '0'),
        'mime_type': ('mime_type', lambda x: x, str),
        'tags': ('tags', lambda x: ' '.join(x), lambda x: str(x).split()),
        'filesize': ('filesize', lambda x: str(x or 0), lambda x: int(x or 0)),
        'creation_time': ('ctime', lambda x: str(x or 0), None),
        'timestamp': ('mtime', lambda x: str(x or 0), None),
        'mtime': ('mtime', lambda x:
            time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(x or 0)), None),
        }

_ALL_SN_PROPS = (
        'guid', 'context', 'keep', 'mime_type', 'title', 'description',
        'activity_id', 'filesize', 'traits', 'tags', 'ctime', 'mtime',
        )


class Datastore(dbus_thread.Service):

    def __init__(self):
        bus_name = BusName(_SERVICE, bus=dbus.SessionBus())
        dbus_thread.Service.__init__(self, bus_name, _OBJECT_PATH)

    def handle_event(self, event):
        if event.get('document') != 'artifact':
            return
        event_type = event['event']
        if event_type == 'create':
            self.Created(event['guid'])
        elif event_type == 'update':
            self.Updated(event['guid'])
        elif event_type == 'delete':
            self.Deleted(event['guid'])

    @signal(_INTERFACE, signature='s')
    def Created(self, uid):
        pass

    @signal(_INTERFACE, signature='s')
    def Updated(self, uid):
        pass

    @signal(_INTERFACE, signature='s')
    def Deleted(self, uid):
        pass

    @signal(_INTERFACE, signature='a{sv}')
    def Mounted(self, descriptior):
        pass

    @signal(_INTERFACE, signature='a{sv}')
    def Unmounted(self, descriptor):
        pass

    @signal(_INTERFACE)
    def Stopped(self):
        pass

    @method(_INTERFACE, in_signature='a{sv}sb', out_signature='s',
            async_callbacks=('reply_cb', 'error_cb'), byte_arrays=True)
    def create(self, props, file_path, transfer_ownership, reply_cb, error_cb):
        self._update(props, file_path, transfer_ownership, reply_cb, error_cb,
                method='POST')

    @method(_INTERFACE, in_signature='sa{sv}sb', out_signature='',
            async_callbacks=('reply_cb', 'error_cb'), byte_arrays=True)
    def update(self, uid, props, file_path, transfer_ownership,
            reply_cb, error_cb):
        self._update(props, file_path, transfer_ownership, reply_cb, error_cb,
                method='PUT', guid=uid)

    @method(_INTERFACE, in_signature='a{sv}as', out_signature='aa{sv}u',
            async_callbacks=('reply_cb', 'error_cb'))
    def find(self, request, properties, reply_cb, error_cb):

        def reply_guid(result):
            reply_cb([_encode_props(result, properties)], 1)

        if 'uid' in request:
            self.call(reply_guid, error_cb, method='GET', mountpoint='~',
                    document='artifact', guid=request['uid'],
                    reply=_decode_names(properties))
            return

        def reply(result):
            entries = []
            for i in result['result']:
                entries.append(_encode_props(i, properties))
            reply_cb(entries, result['total'])

        offset = request.get('offset')
        limit = request.get('limit')
        query = request.get('query')
        order_by = request.get('order_by')
        if order_by:
            if order_by[0] in ('+', '-'):
                sign = order_by[0]
                order_by = order_by[1:]
            else:
                sign = ''
            order_by = _decode_names([order_by])[0]
            if order_by == 'traits':
                order_by = None
            else:
                order_by = sign + order_by
        kwargs = _decode_props(request, process_traits=False)

        self.call(reply, error_cb, method='GET', mountpoint='~',
                document='artifact', reply=_decode_names(properties),
                offset=offset, limit=limit, query=query, order_by=order_by,
                **kwargs)

    @method(_INTERFACE, in_signature='s', out_signature='s',
            async_callbacks=('reply_cb', 'error_cb'))
    def get_filename(self, uid, reply_cb, error_cb):

        def reply(meta):
            if meta is None:
                path = ''
            else:
                fd, path = tempfile.mkstemp(
                        prefix=uid, dir=sugar.profile_path('data', ''))
                os.close(fd)
                os.unlink(path)
                util.cptree(meta['path'], path)
                os.chmod(path, 0604)
            reply_cb(path)

        self.call(reply, error_cb, method='GET', mountpoint='~',
                cmd='get_blob', document='artifact', guid=uid, prop='data')

    @method(_INTERFACE, in_signature='s', out_signature='a{sv}',
            async_callbacks=('reply_cb', 'error_cb'))
    def get_properties(self, uid, reply_cb, error_cb):

        def reply(blob, props):
            props = _encode_props(props, None)
            if blob is not None:
                with file(blob['path'], 'rb') as f:
                    props['preview'] = dbus.ByteArray(f.read())
            reply_cb(props)

        def get_preview(props):
            self.call(reply, error_cb, method='GET', mountpoint='~',
                    cmd='get_blob', document='artifact', guid=uid,
                    prop='preview', args=[props])

        self.call(get_preview, error_cb, method='GET', mountpoint='~',
                document='artifact', guid=uid, reply=_ALL_SN_PROPS)

    @method(_INTERFACE, in_signature='sa{sv}', out_signature='as',
            async_callbacks=('reply_cb', 'error_cb'))
    def get_uniquevaluesfor(self, propertyname, query, reply_cb, error_cb):
        prop = _decode_names([propertyname])[0]

        def reply(result):
            entries = [i[prop] for i in result['result']]
            reply_cb(entries)

        query['limit'] = 1024
        query['group_by'] = prop
        self.call(reply, error_cb, method='GET', mountpoint='~',
                document='artifact', reply=[prop], **query)

    @method(_INTERFACE, in_signature='s', out_signature='')
    def delete(self, uid):
        self.call(None, None, method='DELETE', mountpoint='~',
                document='artifact', guid=uid)

    @method(_INTERFACE, in_signature='', out_signature='aa{sv}')
    def mounts(self):
        return [{'id': 1}]

    @method(_INTERFACE, in_signature='sa{sv}', out_signature='s')
    def mount(self, uri, options=None):
        return ''

    @method(_INTERFACE, in_signature='s', out_signature='')
    def unmount(self, mountpoint_id):
        pass

    def _update(self, props, file_path, transfer_ownership, reply_cb, error_cb,
            **kwargs):
        blobs = []
        if 'preview' in props:
            preview = props.pop('preview')
            blobs.append({
                'prop': 'preview',
                'content_stream': StringIO(bytes(preview)),
                'content_length': len(preview),
                })
        if file_path and exists(file_path):
            blobs.append({
                'cmd': 'upload_blob',
                'prop': 'data',
                'path': file_path,
                'pass_ownership': transfer_ownership,
                })
            props['filesize'] = os.stat(file_path).st_size
        else:
            props['filesize'] = '0'

        self.call(self._set_blob, error_cb, mountpoint='~',
                document='artifact', content=_decode_props(props),
                args=[kwargs.get('guid'), blobs, reply_cb, error_cb], **kwargs)

    def _set_blob(self, reply, guid, blobs, reply_cb, error_cb):
        if not guid:
            guid = reply

        if blobs:
            self.call(self._set_blob, error_cb, method='PUT',
                    mountpoint='~', document='artifact', guid=guid,
                    args=[guid, blobs, reply_cb, error_cb], **blobs.pop())
        else:
            reply_cb(guid)


def _decode_names(names):
    result = []
    add_traits = False

    for ds_prop in names:
        if ds_prop in _MAP_PROPS:
            result.append(_MAP_PROPS[ds_prop][0])
        else:
            add_traits = True

    if add_traits:
        result.append('traits')
    return result


def _decode_props(props, process_traits=True):
    result = {}

    for ds_prop, (sn_prop, __, typecast) in _MAP_PROPS.items():
        if ds_prop in props:
            value = props.pop(ds_prop)
            if typecast is not None:
                result[sn_prop] = typecast(value)

    if process_traits and props:
        traits = result['traits'] = {}
        for key, value in props.items():
            traits[str(key)] = str(value)

    return result


def _encode_props(props, names):
    result = {}

    for ds_prop, (sn_prop, typecast, __) in _MAP_PROPS.items():
        if sn_prop in props:
            value = typecast(props[sn_prop])
            result[ds_prop] = value

    traits = props.get('traits') or {}
    if names is None:
        result.update(traits)
    else:
        for key, value in traits.items():
            if key in names:
                result[key] = value

    return result
