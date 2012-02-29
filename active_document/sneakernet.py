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
import json
import gzip
import uuid
import logging
from glob import glob
from os.path import abspath, isfile, join
from gettext import gettext as _


#: Callback to execute while synchronizing server data when target directory
#: is full; accepts (`path`) arguments
next_volume_cb = None

_HEADER_SIZE = 4096
_RESERVED_SIZE = 1024 * 1024

_logger = logging.getLogger('ad.sneakernet')


def sync_node(node, volume_path, merge_cb, diff):
    _logger.info(_('Synchronize with %s directory'), volume_path)

    for packet in _import(volume_path, merge_cb):
        sender = packet.header.get('sender')
        if sender == node:
            _logger.debug('Remove existing %s packet', packet.path)
            os.unlink(packet.path)
        elif sender == 'master' and packet.header.get('to') == node:
            for row in packet.read_rows(type='ack'):
                merge_cb(packet.header, row)
            _logger.debug('Remove loaded %s packet', packet.path)
            os.unlink(packet.path)
        else:
            for row in packet.read_rows(type=['diff', 'request']):
                if row['type'] == 'diff':
                    merge_cb(packet.header, row)
            if sender != 'master':
                del packet.syns[:]

    return _export(volume_path, node, diff)


def sync_master(volume_path, merge_cb, diff):
    _logger.info(_('Synchronize with %s directory'), volume_path)

    for packet in _import(volume_path, merge_cb):
        if packet.header.get('sender') == 'master':
            del packet.syns[:]
        else:
            for row in packet.read_rows(type=['diff', 'request']):
                merge_cb(packet.header, row)
        _logger.debug('Remove loaded %s packet', packet.path)
        os.unlink(packet.path)

    return _export(volume_path, 'master', diff)


class _InPacket(object):

    def __init__(self, path):
        self.path = abspath(path)
        self._zip = None
        self._row = None
        self.header = {}
        self.syns = []

        if not isfile(path):
            return

        self._zip = gzip.GzipFile(path)
        header = self._read(size=_HEADER_SIZE, subject='Sugar Network Packet')
        if header is None:
            _logger.info(_('Skip not recognized input packet file, %s'), path)
            self.close()
        else:
            _logger.info(_('Open input packet file, %s: %r'), path, header)
            self.header = header

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def opened(self):
        return self._zip is not None

    def read_rows(self, **kwargs):
        while True:
            row = self._read(**kwargs)
            if row is None:
                break
            elif row is False:
                continue
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

            if row.get('type') == 'syn':
                self.syns.append(row)
                self._row = None
                return False

            for key, value in kwargs.items():
                if type(value) == list:
                    if row.get(key) in value:
                        continue
                elif row.get(key) == value:
                    continue
                self._row = row
                return None

            self._row = None
            return row

        except (IOError, ValueError, TypeError), error:
            _logger.warning(_('Malformed input packet file "%s": %s'),
                    self.path, error)
            self.close()


class _OutPacket(object):

    def __init__(self, root, **kwargs):
        self._root = abspath(root)
        self.header = {'subject': 'Sugar Network Packet'}
        for key, value in kwargs.items():
            if value is not None:
                self.header[key] = value
        self._zip = None
        self._couter = 0
        self.path = None
        self._packets = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self._zip is not None:
            self._zip.close()
            self._zip = None
            self._couter = 0

    def write_row(self, **kwargs):
        if self._zip is None or self._couter >= _RESERVED_SIZE:
            self._next_volume()
        if kwargs.get('type') == 'syn':
            kwargs['packets'] = self._packets
        self._write_row(kwargs)

    def _next_volume(self):
        next_guid = str(uuid.uuid1())
        if self._zip is not None:
            self._write_row({'type': 'part', 'next': next_guid})
            self.header['prev'] = self.header['guid']
        self.close()

        while True:
            stat = os.statvfs(self._root)
            if stat.f_bfree * stat.f_frsize >= _RESERVED_SIZE * 2:
                break
            # pylint: disable-msg=E1102
            if next_volume_cb is None or not next_volume_cb(self._root):
                raise IOError(_('No free disk space in "%s"') % self._root)
            _logger.info(_('Switched volumes for "%s"'), self._root)

        self._packets.append(next_guid)
        self.header['guid'] = next_guid
        self.path = join(self._root, '%s.packet.gz' % next_guid)

        _logger.info(_('Open output packet file "%s"'), self.path)
        self._zip = gzip.GzipFile(self.path, 'w')
        self.write_row(**self.header)

    def _write_row(self, kwargs):
        data = json.dumps(kwargs)
        self._zip.write(data)
        self._zip.write('\n')
        self._couter += len(data) + 1


def _import(volume_path, merge_cb):
    processed = set()
    processed_guids = set()
    syns = []

    while True:
        parts = set()

        for packet_path in glob(join(volume_path, '*.packet.gz')):
            if packet_path in processed:
                continue
            processed.add(packet_path)
            with _InPacket(packet_path) as packet:
                processed_guids.add(packet.header.get('guid'))
                if 'prev' in packet.header:
                    parts.add(packet.header['prev'])
                yield packet
                for i in packet.syns:
                    syns.append((packet.header, i))
                for row in packet.read_rows(type='part'):
                    parts.add(row['next'])

        if parts and next_volume_cb is not None:
            part_names = ', '.join(['%s.packet.gz' for i in parts])
            # pylint: disable-msg=E1102
            if next_volume_cb(volume_path,
                    _('Change %s volume to load %s packet(s)') % \
                            (volume_path, part_names)):
                continue
        break

    for header, row in syns:
        if not (set(row.get('packets', [])) - processed_guids):
            merge_cb(header, row)


def _export(volume_path, sender, diff):
    packet = None
    last_to = False
    result = None

    try:
        for to, row in diff:
            if to != last_to:
                last_to = to
                if packet is not None:
                    packet.close()
                packet = _OutPacket(volume_path, sender=sender, to=to)
            packet.write_row(**row)
    finally:
        if packet is not None:
            packet.close()
            result = packet.path

    return result
