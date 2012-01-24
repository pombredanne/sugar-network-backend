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
from os.path import join, exists, dirname, abspath
from gettext import gettext as _

import gevent

from active_document import env, util
from active_document.sync import Sync
from active_document.util import enforce


_HEADER_SIZE = 4096
_RESERVED_SIZE = 1024 * 1024

_logger = logging.getLogger('ad.folder')


class _Folder(dict):

    def __init__(self, is_master, document_classes):
        self._is_master = is_master
        self._id = None
        self._syncs = {}

        enforce(env.data_root.value,
                _('The active_document.data_root.value is not set'))
        if not exists(env.data_root.value):
            os.makedirs(env.data_root.value)

        for cls in document_classes:
            cls.init()
            self._syncs[cls.metadata.name] = Sync(cls.metadata)

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
                for row in packet.read_rows(type='ack'):
                    self._dispatch()
                    syncer = self._syncs.get(row['document'])
                    if syncer is None:
                        _logger.warning(
                                _('Unknown document "%s" in "%s" packet'),
                                row['document'], packet.path)
                        continue
                    syncer.process_ack(row['ack'])
            os.unlink(id_path)

        for filename in os.listdir(volume_path):
            with _InPacket(join(volume_path, filename)) as packet:
                for changeset in packet.read_rows(type='changeset'):
                    syncer = self._syncs.get(changeset['document'])
                    if syncer is None:
                        _logger.warning(
                                _('Unknown document "%s" in "%s" packet'),
                                changeset['document'], packet.path)
                        continue

                    def iterate():
                        seqno = None
                        for row in packet.read_rows(type='diff'):
                            self._dispatch()
                            if packet.sender == 'master':
                                seqno = row['seqno']
                            yield seqno, row['guid'], row['diff']

                    syncer.merge(iterate())

        try:
            with _OutPacket(id_path, next_volume_cb, sender=self.id) as packet:
                for document, syncer in self._syncs.items():
                    syn, patch = syncer.create_syn()
                    self._dispatch()
                    packet.write_row(type='syn', document=document, syn=syn)
                    for seqno, guid, diff in patch:
                        self._dispatch()
                        packet.write_row(type='diff', document=document,
                                seqno=seqno, guid=guid, diff=diff)
        except IOError, error:
            _logger.warning(_('Packet was not fully uploaded to "%s": %s'),
                    volume_path, error)

        for syncer in self._syncs.values():
            self._dispatch()
            syncer.flush()

    def _dispatch(self):
        gevent.sleep()


class NodeFolder(_Folder):

    def __init__(self, document_classes):
        _Folder.__init__(self, False, document_classes)


class _InPacket(object):

    def __init__(self, path):
        self.path = path
        self._zip = gzip.GzipFile(path)
        self.sender = None
        self.receiver = None
        self._row = None

        header = self._read(size=_HEADER_SIZE, subject='Sugar Network Packet')
        if header is None:
            _logger.info(_('Skip not recognized input packet file, %s'), path)
            self.close()
        else:
            _logger.info(_('Open input packet file, %s: %r'), path, header)
            self.sender = header.get('sender')
            self.receiver = header.get('receiver')

    @property
    def opened(self):
        return self._zip is not None

    def read_rows(self, **kwargs):
        while True:
            row = self._read(**kwargs)
            if row is None:
                break
            yield row

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
                    _logger.warning(_('EOF for packet file, %s'), self.path)
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
            _logger.warning(_('Malformed input packet file "%s": %s'),
                    self.path, error)
            self.close()


class _OutPacket(object):

    def __init__(self, path, next_volume_cb=None, **kwargs):
        self.path = path
        self._next_volume_cb = next_volume_cb
        self._header = {'subject': 'Sugar Network Packet'}
        self._header.update(kwargs)
        self._zip = None
        self._couter = 0

    def close(self):
        if self._zip is not None:
            self._zip.close()
            self._zip = None
            self._couter = 0

    def write_row(self, **kwargs):
        if self._zip is None or self._couter >= _RESERVED_SIZE:
            self._next_volume()
        data = json.dumps(kwargs)
        self._zip.write(data)
        self._zip.write('\n')
        self._couter += len(data) + 1

    def _next_volume(self):
        self.close()

        fs_path = abspath(dirname(self.path))
        while True:
            stat = os.statvfs(fs_path)
            if stat.f_bfree * stat.f_frsize >= _RESERVED_SIZE * 2:
                break
            if self._next_volume_cb is None or \
                    not self._next_volume_cb(fs_path):
                raise IOError(_('No free disk space in "%s"') % fs_path)
            _logger.info(_('Switched volumes for "%s"'), fs_path)

        _logger.info(_('Open output packet file "%s"'), self.path)
        self._zip = gzip.GzipFile(self.path, 'w')
        self.write_row(**self._header)
