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

import json
import logging
from os.path import exists, join

from sugar_network import local


_logger = logging.getLogger('local.cache')


def get_blob(document, guid, prop, seqno, node, download_cb):
    blob_path = join(local.local_root.value, 'cache', document, guid[:2],
            guid, prop)
    meta_path = blob_path + '.meta'
    meta = {}

    def download(cached_seqno):
        mime_type = download_cb(blob_path, cached_seqno)
        meta['mime_type'] = mime_type
        meta['seqno'] = seqno
        meta['volume'] = node
        with file(meta_path, 'w') as f:
            json.dump(meta, f)
        _logger.debug('Cache %s/%s/%s blob, meta=%r path=%r',
                document, guid, prop, meta, blob_path)

    if exists(meta_path):
        with file(meta_path) as f:
            meta = json.load(f)
        if meta.get('volume') != node:
            _logger.debug('Fetch %s/%s/%s blob, stale node %r -> %r',
                    document, guid, prop, meta.get('volume'), node)
            download(None)
        elif meta.get('seqno') < seqno:
            _logger.debug('Fetch %s/%s/%s blob, stale seqno %r -> %r',
                    document, guid, prop, meta.get('seqno'), seqno)
            download(meta['seqno'])
    else:
        _logger.debug('Fetch initial %s/%s/%s blob', document, guid, prop)
        download(None)

    if not exists(blob_path):
        return None
    meta['path'] = blob_path
    return meta
