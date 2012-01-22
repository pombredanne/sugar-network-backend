# Copyright (C) 2011-2012, Aleksey Lim
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
import uuid
import gzip
import logging
from os.path import join, exists
from gettext import gettext as _

from active_document import env, util
from active_document.sync import Synchronizer
from active_document.util import enforce


_HEADER_SIZE = 4096

_logger = logging.getLogger('ad.folder')


class _Folder(dict):

    def __init__(self, is_master, document_classes):
        self._is_master = is_master
        self._id = None
        self._synchronizers = {}

        enforce(env.data_root.value,
                _('The active_document.data_root.value is not set'))
        if not exists(env.data_root.value):
            os.makedirs(env.data_root.value)

        for cls in document_classes:
            cls.init()
            self._synchronizers[cls.metadata.name] = Synchronizer(cls.metadata)

        if env.index_write_queue.value > 0:
            from active_document import index_queue
            index_queue.init(document_classes)

        id_path = join(env.data_root.value, 'id')
        if exists(id_path):
            f = file(id_path)
            self._id = f.read().strip()
            f.close()
        else:
            self._id = str(uuid.uuid1())
            f = util.new_file(id_path)
            f.write(self._id)
            f.close()

        _logger.info(_('Open %s documents folder'), self.id)

    @property
    def id(self):
        return self._id

    def close(self):
        if env.index_write_queue.value > 0:
            from active_document import index_queue
            index_queue.close()

    def sync(self, volume_path, next_volume_cb=None):
        _logger.info(_('Syncing with %s directory'), volume_path)

        id_path = join(volume_path, self.id + '.gz')
        if exists(id_path):
            with _InPacket(id_path) as packet:
                for document, ack in packet.acks:
                    yield
                    self._synchronizers[document].process_ack(ack)
                packet.unlink()

        for filename in os.listdir(volume_path):
            with _InPacket(join(volume_path, filename)) as packet:
                for document, region, rows in packet.dumps:
                    synchronizer = self._synchronizers[document]
                    for row in rows:
                        yield
                        synchronizer.merge(row)
                    if region:
                        yield
                        synchronizer.process_region(region)

        syn, rows = self._synchronizers[self.id].create_syn()
        if syn is not None:
            with _OutPacket(id_path, next_volume_cb) as packet:
                yield
                packet.writeln(syn)
                for row in rows:
                    yield
                    packet.writeln(row)


class NodeFolder(_Folder):

    def __init__(self, document_classes):
        _Folder.__init__(self, False, document_classes)


class _InPacket(object):

    def __init__(self, path):
        self._path = path
        self._zip = gzip.GzipFile(path)
        self.sender = None
        self.receiver = None
        self._row = None

        header = self._read(size=_HEADER_SIZE, subject='Sugar Network Packet')
        if header is None:
            _logger.info(_('Skip not recognized packet file, %s'), path)
            self.close()
        else:
            _logger.info(_('Open packet file, %s: %r'), path, header)
            self.sender = header.get('sender')
            self.receiver = header.get('receiver')

    @property
    def opened(self):
        return self._zip is not None

    @property
    def syns(self):
        while True:
            row = self._read(type='syn')
            if row is None:
                break
            yield row['document'], row['syn']

    @property
    def acks(self):
        while True:
            row = self._read(type='ack')
            if row is None:
                break
            yield row['document'], row['ack']

    @property
    def dumps(self):

        def read_rows():
            while True:
                row = self._read(type='row')
                if row is None:
                    break
                yield row['row']

        while True:
            row = self._read(type='dump')
            if row is None:
                break
            yield row['document'], row['region'], read_rows()

    def close(self):
        if self._zip is not None:
            self._zip.close()
            self._zip = None

    def _read(self, size=None, **kwargs):
        try:
            if self._row is None:
                if not self.opened:
                    return
                row = self._zip.readline(size)
                if not row:
                    _logger.warning(_('EOF for packet file, %s'), self._path)
                    self.close()
                    return
                row = dict(json.loads(row))
            else:
                row = self._row

            for key, value in kwargs.items():
                if row.get(key) != value:
                    self._row = row
                    return None

            self._row = None
            return row

        except (IOError, ValueError, TypeError), error:
            _logger.warning(_('Malformed packet file, %s: %s'),
                    self._path, error)
            self.close()


class _OutPacket(object):
    pass
