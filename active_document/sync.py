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

import copy
import logging
from os.path import join, exists
from gettext import gettext as _

from active_toolkit import util, coroutine
from active_document import env, sneakernet


_logger = logging.getLogger('active_document.sync')


class _Node(object):

    def __init__(self, document_classes, sync_class, node_id):
        self._synchronizers = {}
        self._node_id = node_id

        for cls in document_classes:
            self._synchronizers[cls.metadata.name] = sync_class(cls)

        _logger.info(_('Open %r documents volume'), self.node_id)

    @property
    def node_id(self):
        """Server unique identity."""
        return self._node_id

    def sync(self, volume_path):
        """Synchronize server data."""

        for sync in self._synchronizers.values():
            sync.cls.commit()
            sync.reset()

    def close(self):
        """Close operations with the server."""
        _logger.info(_('Closing %r documents volume'), self.node_id)
        while self._synchronizers:
            __, sync = self._synchronizers.popitem()
            sync.cls.close()

    def _merge(self, packet, row, *args):
        coroutine.dispatch()

        sync = self._synchronizers.get(row['document'])
        if sync is None:
            _logger.warning(_('Unknown document %r'), row['document'])
            return

        method = 'process_%s' % row['type']
        if not hasattr(sync, method):
            _logger.warning(_('Unknown type %(row)r'), row)
            return

        return getattr(sync, method)(packet, row, *args)

    def _diff(self):
        for document, sync in self._synchronizers.items():
            sync.cls.commit()

            for i in sync.diff():
                yield i

            if not sync.to_diff:
                continue
            to_diff = copy.deepcopy(sync.to_diff)

            diff_range, patch = sync.cls.diff(to_diff)
            for guid, diff in patch:
                coroutine.dispatch()
                yield None, {
                        'type': 'diff',
                        'document': document,
                        'guid': guid,
                        'diff': diff,
                        }

            if diff_range[1]:
                to_diff.floor(diff_range[1])
                coroutine.dispatch()
                yield None, {
                        'type': 'syn',
                        'document': document,
                        'syn': to_diff,
                        }

            sync.commit()


class Node(_Node):
    """Node server."""

    def __init__(self, document_classes):
        """
        :param document_classes:
            list of active_document.Document classes for documents that
            server should provide

        """
        # XXX
        id_path = join('env.data_root.value', 'id')
        if exists(id_path):
            with file(id_path) as f:
                node_id = f.read().strip()
        else:
            node_id = env.uuid()
            with util.new_file(id_path) as f:
                f.write(node_id)

        _Node.__init__(self, document_classes, _NodeSynchronizer, node_id)

    def sync(self, volume_path):
        _Node.sync(self, volume_path)
        sneakernet.sync_node(self.node_id, volume_path,
                self._merge, self._diff())


class Master(_Node):
    """Master server."""

    def __init__(self, document_classes):
        """
        :param document_classes:
            list of active_document.Document classes for documents that
            server should provide

        """
        _Node.__init__(self, document_classes, _MasterSynchronizer, 'master')

    def sync(self, volume_path):
        _Node.sync(self, volume_path)
        sneakernet.sync_master(volume_path, self._merge, self._diff())


class _NodeSynchronizer(object):

    def __init__(self, cls):
        self.cls = cls
        self.to_diff = env.Range(cls.metadata.path('send'), [1, None])
        self._to_receive = env.Range(cls.metadata.path('receive'), [1, None])

    def reset(self):
        pass

    def commit(self):
        self.to_diff.commit()
        self._to_receive.commit()

    def process_ack(self, packet, row):
        self.to_diff.exclude(row['ack'])
        self._to_receive.exclude(row['merged'])

    def process_diff(self, packet, row):
        self.cls(row['guid']).merge(row['guid'], row['diff'], False)

    def process_syn(self, packet, row):
        self._to_receive.exclude(row['syn'])

    def diff(self):
        yield None, {
                'type': 'request',
                'document': self.cls.metadata.name,
                'request': self._to_receive,
                }


class _MasterSynchronizer(object):

    def __init__(self, cls):
        self.cls = cls
        self.to_diff = None
        self._merged = None
        self._to_ack = None
        self.reset()

    def reset(self):
        self.to_diff = env.Range()
        self._merged = {}
        self._to_ack = {}

    def commit(self):
        pass

    def process_request(self, packet, row):
        self.to_diff.include(row['request'])

    def process_diff(self, packet, row):
        seqno = self.cls(row['guid']).merge(row['guid'], row['diff'], True)
        if seqno:
            self._merged.setdefault(packet['sender'], env.Range())
            self._merged[packet['sender']].include(seqno, seqno)

    def process_syn(self, packet, row):
        self._to_ack.setdefault(packet['sender'], [])
        self._to_ack[packet['sender']].append(row['syn'])

    def diff(self):
        for requester, syns in self._to_ack.items():
            merged = self._merged.get(requester)
            for syn in syns:
                yield requester, {
                        'type': 'ack',
                        'document': self.cls.metadata.name,
                        'ack': syn,
                        'merged': merged or [],
                        }
        if len(self._merged) == 1:
            # Exclude only singular SYN.
            # Otherwise, all nodes that sent REQUEST need to know about
            # each other's SYNs.
            self.to_diff.exclude(self._merged.values()[0])
