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
from os.path import join

import active_document as ad
from active_toolkit import coroutine
from sugar_network.toolkit.sneakernet import DiskFull
from sugar_network.toolkit.collection import Sequence


_DIFF_CHUNK = 1024

_logger = logging.getLogger('resources.volume')


class Resource(ad.Document):

    @ad.active_property(prefix='RU', typecast=[], default=[],
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def user(self, value):
        return value

    @ad.active_property(prefix='RL', typecast=[], default=['public'])
    def layer(self, value):
        return value

    @ad.active_property(prefix='RA', full_text=True, default=[], typecast=[],
            permissions=ad.ACCESS_READ)
    def author(self, value):
        return value

    @ad.active_property(prefix='RT', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value


class Volume(ad.SingleVolume):

    RESOURCES = (
            'sugar_network.resources.artifact',
            'sugar_network.resources.comment',
            'sugar_network.resources.context',
            'sugar_network.resources.implementation',
            'sugar_network.resources.notification',
            'sugar_network.resources.feedback',
            'sugar_network.resources.report',
            'sugar_network.resources.solution',
            'sugar_network.resources.user',
            )

    def __init__(self, root, document_classes=None, lazy_open=False):
        if document_classes is None:
            document_classes = Volume.RESOURCES
        ad.SingleVolume.__init__(self, root, document_classes, lazy_open)

    def notify(self, event):
        if event['event'] == 'update' and 'props' in event and \
                'deleted' in event['props'].get('layer', []):
            event['event'] = 'delete'
            del event['props']

        ad.SingleVolume.notify(self, event)

    def merge(self, record, increment_seqno=True):
        coroutine.dispatch()
        if record.get('content_type') == 'blob':
            record['diff'] = record['blob']
        return self[record['document']].merge(increment_seqno=increment_seqno,
                **record)

    def diff(self, in_seq, out_packet):
        # Since `in_seq` will be changed in `patch()`, original sequence
        # should be passed as-is to every document's `diff()` because
        # seqno handling is common for all documents
        orig_seq = Sequence(in_seq)
        push_seq = Sequence()

        for document, directory in self.items():
            coroutine.dispatch()
            directory.commit()

            def patch():
                for meta, data in directory.diff(orig_seq, limit=_DIFF_CHUNK):
                    coroutine.dispatch()

                    seqno = None
                    if 'seqno' in meta:
                        seqno = meta.pop('seqno')

                    if hasattr(data, 'fileno'):
                        arcname = join(document, 'blobs', meta['guid'],
                                meta['prop'])
                        out_packet.push(data, arcname=arcname,
                                cmd='sn_push', document=document, **meta)
                    else:
                        meta['diff'] = data
                        yield meta

                    # Process `seqno` only after processing yield'ed data
                    if seqno:
                        # Update `in_seq`, it might be reused by caller
                        in_seq.exclude(seqno, seqno)
                        push_seq.include(seqno, seqno)

            try:
                out_packet.push(patch(), arcname=join(document, 'diff'),
                        cmd='sn_push', document=document)
            except DiskFull:
                if push_seq:
                    out_packet.push(force=True, cmd='sn_commit',
                            sequence=push_seq)
                raise

        if push_seq:
            # Only here we can collapse `push_seq` since seqno handling
            # is common for all documents; if there was an exception before
            # this place, `push_seq` should contain not-collapsed sequence
            orig_seq.floor(push_seq.last)
            out_packet.push(force=True, cmd='sn_commit', sequence=orig_seq)


class Commands(object):

    def __init__(self):
        self._notifier = coroutine.AsyncResult()
        self.connect(lambda event: self._notify(event))

    def connect(self, callback, condition=None, **kwargs):
        raise NotImplementedError()

    @ad.volume_command(method='GET', cmd='subscribe')
    def subscribe(self, response, only_commits=False):
        """Subscribe to Server-Sent Events.

        :param only_commits:
            subscribers can be notified only with "commit" events;
            that is useful to minimize interactions between server and clients

        """
        response.content_type = 'text/event-stream'
        response['Cache-Control'] = 'no-cache'
        return self._pull_events(only_commits)

    def _pull_events(self, only_commits):
        while True:
            event = self._notifier.get()

            if only_commits:
                if event['event'] != 'commit':
                    continue
            else:
                if event['event'] == 'commit':
                    # Subscribers already got update notifications enough
                    continue

            yield 'data: %s\n\n' % json.dumps(event)

    def _notify(self, event):
        self._notifier.set(event)
        self._notifier = coroutine.AsyncResult()
        coroutine.dispatch()
