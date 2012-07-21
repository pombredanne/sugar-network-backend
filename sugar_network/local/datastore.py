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
import logging
from os.path import exists, join, isdir, isfile

from sugar_network import local
from sugar_network.toolkit import sugar
from active_toolkit import coroutine


MAP_PROPS = {
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

ALL_SN_PROPS = (
        'guid', 'context', 'keep', 'mime_type', 'title', 'description',
        'activity_id', 'filesize', 'traits', 'tags', 'ctime', 'mtime',
        )

_logger = logging.getLogger('local.datastore')


def populate(artifacts):
    sn_stamp = local.path('datastore.index_updated')
    ds_stamp = sugar.profile_path('datastore', 'index_updated')

    if exists(sn_stamp) == exists(ds_stamp) and (not exists(sn_stamp) or \
            os.stat(sn_stamp).st_mtime == os.stat(ds_stamp).st_mtime):
        _logger.debug('No stale changes found in sugar-datastore')
        return

    if not exists(ds_stamp):
        _logger.debug('sugar-datastore index was removed')
        os.unlink(sn_stamp)
        return

    _logger.info('Start populating Artifacts from sugar-datastore')

    root, dirs, __ = os.walk(sugar.profile_path('datastore')).next()
    for dirname in dirs:
        for guid in os.listdir(join(root, dirname)):
            coroutine.dispatch()

            metadata = join(root, dirname, guid, 'metadata')
            if not isdir(metadata) or artifacts.exists(guid):
                continue

            props = {'guid': guid,
                     'traits': {},
                     }
            for ds_prop in os.listdir(metadata):
                path = join(metadata, ds_prop)
                if not isfile(path):
                    continue
                if ds_prop == 'preview':
                    artifacts.set_blob(guid, 'preview', path)
                else:
                    with file(path, 'rb') as f:
                        value = f.read()
                    if ds_prop in MAP_PROPS:
                        sn_prop, __, typecast = MAP_PROPS[ds_prop]
                        if typecast is not None:
                            props[sn_prop] = typecast(value)
                        elif ds_prop == 'creation_time':
                            props['ctime'] = int(value)
                        elif ds_prop == 'timestamp':
                            props['mtime'] = int(value)
                    else:
                        props['traits'][ds_prop] = value
            artifacts.create(props)

            data = join(root, dirname, guid, 'data')
            if isfile(data):
                artifacts.set_blob(guid, 'data', data)

    with file(sn_stamp, 'w'):
        pass
    stamp = os.stat(ds_stamp).st_mtime
    os.utime(sn_stamp, (stamp, stamp))


def decode_names(names):
    result = []
    add_traits = False

    for ds_prop in names:
        if ds_prop in MAP_PROPS:
            result.append(MAP_PROPS[ds_prop][0])
        else:
            add_traits = True

    if add_traits:
        result.append('traits')
    return result


def decode_props(props, process_traits=True):
    result = {}

    for ds_prop, (sn_prop, __, typecast) in MAP_PROPS.items():
        if ds_prop in props:
            value = props.pop(ds_prop)
            if typecast is not None:
                result[sn_prop] = typecast(value)

    if process_traits and props:
        traits = result['traits'] = {}
        for key, value in props.items():
            traits[str(key)] = str(value)

    return result


def encode_props(props, names):
    result = {}

    for ds_prop, (sn_prop, typecast, __) in MAP_PROPS.items():
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
