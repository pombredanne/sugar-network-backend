#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
from os.path import dirname, join, abspath


src_root = abspath(dirname(__file__))


from __init__ import tests

from active_document import volume, document, SingleVolume


class VolumeTest(tests.Test):

    def test_walk_classes(self):
        classes = volume._walk_classes(join(src_root, 'document_classes'))
        self.assertEqual(
                ['resource_1', 'resource_3'],
                sorted(dict(classes).keys()))

    def test_SingleVolume_Populate(self):
        self.touch(
                ('document/1/1/.seqno', ''),
                ('document/1/1/guid', '1'),
                ('document/1/1/ctime', '1'),
                ('document/1/1/mtime', '1'),
                ('document/1/1/layers', '["public"]'),
                ('document/1/1/author', '["me"]'),

                ('document/2/2/.seqno', ''),
                ('document/2/2/guid', '2'),
                ('document/2/2/ctime', '2'),
                ('document/2/2/mtime', '2'),
                ('document/2/2/layers', '["public"]'),
                ('document/2/2/author', '["me"]'),
                )

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

        shutil.rmtree('document/index')

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))


if __name__ == '__main__':
    tests.main()
