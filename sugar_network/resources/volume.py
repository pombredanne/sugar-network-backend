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

import logging
from os.path import join

import active_document as ad
from active_toolkit import coroutine


_DIFF_CHUNK = 1024

_logger = logging.getLogger('resources.volume')


class Resource(ad.Document):

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

    def diff(self, in_seq, out_packet):
        for document, directory in self.items():
            coroutine.dispatch()
            directory.commit()

            def patch():
                for meta, data in directory.diff(in_seq, limit=_DIFF_CHUNK):
                    coroutine.dispatch()
                    if hasattr(data, 'fileno'):
                        arcname = join(document, 'blobs', meta['guid'],
                                meta['prop'])
                        out_packet.push(data, arcname=arcname,
                                cmd='sn_push', document=document, **meta)
                    else:
                        meta['diff'] = data
                        yield meta
                    if 'range' in meta:
                        in_seq.exclude(*meta['range'])

            out_packet.push(patch(), arcname=join(document, 'diff'),
                    cmd='sn_push', document=document)
