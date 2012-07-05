#!/usr/bin/env python
# sugar-lint: disable

import os
from cStringIO import StringIO
from os.path import exists, join

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.collection import Sequences
from sugar_network.toolkit.sneakernet import InPacket, OutPacket
from sugar_network.resources.volume import Volume


class VolumeTest(tests.Test):

    def test_diff_merge(self):

        class Document(ad.Document):

            @ad.active_property(slot=1)
            def prop(self, value):
                return value

            @ad.active_property(ad.BlobProperty)
            def blob(self, value):
                return value

        volume = Volume('node1', [Document])
        volume['document'].create(guid='1', prop='prop')
        volume['document'].set_blob('1', 'blob', StringIO('blob'))

        packet = OutPacket('push', root='sync')
        out_seq = Sequences()
        volume.diff(Sequences(empty_value=[1, None]), out_seq, packet)
        self.assertEqual({'document': [[1, 2]]}, out_seq)
        packet.close()

        volume = Volume('node2', [Document])
        volume.merge(InPacket(packet.path))
        doc = volume['document'].get('1')
        self.assertEqual('prop', doc['prop'])
        self.assertEqual('blob', file(doc.meta('blob')['path']).read())


if __name__ == '__main__':
    tests.main()
