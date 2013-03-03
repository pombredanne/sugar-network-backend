#!/usr/bin/env python
# sugar-lint: disable

import os
import hashlib
from cStringIO import StringIO

from __init__ import tests

from sugar_network import db
from sugar_network.node.volume import diff, merge
from sugar_network.resources.volume import Volume, Resource
from sugar_network.toolkit import util


class VolumeTest(tests.Test):

    def test_diff(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1, prop='a')
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (1, 1))
        volume['document'].create(guid='2', seqno=2, prop='b')
        for i in os.listdir('db/document/2/2'):
            os.utime('db/document/2/2/%s' % i, (2, 2))

        in_seq = util.Sequence([[1, None]])
        self.assertEqual([
            {'document': 'document'},
            {'guid': '1',
                'diff': {
                    'guid': {'value': '1', 'mtime': 1},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'value': 'a', 'mtime': 1},
                    },
                },
            {'guid': '2',
                'diff': {
                    'guid': {'value': '2', 'mtime': 2},
                    'mtime': {'value': 0, 'mtime': 2},
                    'ctime': {'value': 0, 'mtime': 2},
                    'prop': {'value': 'b', 'mtime': 2},
                    },
                },
            {'commit': [[1, 2]]},
            ],
            [i for i in diff(volume, in_seq)])
        self.assertEqual([[1, None]], in_seq)

    def test_diff_Partial(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1, prop='a')
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (1, 1))
        volume['document'].create(guid='2', seqno=2, prop='b')
        for i in os.listdir('db/document/2/2'):
            os.utime('db/document/2/2/%s' % i, (2, 2))

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertEqual({'commit': []}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertEqual('2', next(patch)['guid'])
        self.assertEqual({'commit': [[1, 1]]}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

    def test_diff_Stretch(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1, prop='a')
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (1, 1))
        volume['document'].create(guid='3', seqno=3, prop='c')
        for i in os.listdir('db/document/3/3'):
            os.utime('db/document/3/3/%s' % i, (3, 3))
        volume['document'].create(guid='5', seqno=5, prop='f')
        for i in os.listdir('db/document/5/5'):
            os.utime('db/document/5/5/%s' % i, (5, 5))

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, patch.send(None))
        self.assertEqual('1', patch.send(None)['guid'])
        self.assertEqual('3', patch.send(None)['guid'])
        self.assertEqual('5', patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3]]}, patch.throw(StopIteration()))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, patch.send(None))
        self.assertEqual('1', patch.send(None)['guid'])
        self.assertEqual('3', patch.send(None)['guid'])
        self.assertEqual('5', patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 5]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_DoNotStretchContinuesPacket(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='3', seqno=3, prop='c')
        for i in os.listdir('db/document/3/3'):
            os.utime('db/document/3/3/%s' % i, (3, 3))
        volume['document'].create(guid='5', seqno=5, prop='f')
        for i in os.listdir('db/document/5/5'):
            os.utime('db/document/5/5/%s' % i, (5, 5))

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq, util.Sequence([[1, 1]]))
        self.assertEqual({'document': 'document'}, patch.send(None))
        self.assertEqual('3', patch.send(None)['guid'])
        self.assertEqual('5', patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3], [5, 5]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_TheSameInSeqForAllDocuments(self):

        class Document1(db.Document):
            pass

        class Document2(db.Document):
            pass

        class Document3(db.Document):
            pass

        volume = Volume('db', [Document1, Document2, Document3])
        volume['document1'].create(guid='3', seqno=3)
        for i in os.listdir('db/document1/3/3'):
            os.utime('db/document1/3/3/%s' % i, (3, 3))
        volume['document2'].create(guid='2', seqno=2)
        for i in os.listdir('db/document2/2/2'):
            os.utime('db/document2/2/2/%s' % i, (2, 2))
        volume['document3'].create(guid='1', seqno=1)
        for i in os.listdir('db/document3/1/1'):
            os.utime('db/document3/1/1/%s' % i, (1, 1))

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document1'}, patch.send(None))
        self.assertEqual('3', patch.send(None)['guid'])
        self.assertEqual({'document': 'document2'}, patch.send(None))
        self.assertEqual('2', patch.send(None)['guid'])
        self.assertEqual({'document': 'document3'}, patch.send(None))
        self.assertEqual('1', patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 3]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_merge_Create(self):

        class Document1(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        class Document2(db.Document):
            pass

        self.touch(('db/seqno', '100'))
        volume = Volume('db', [Document1, Document2])

        records = [
                {'document': 'document1'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 1.0},
                    'ctime': {'value': 2, 'mtime': 2.0},
                    'mtime': {'value': 3, 'mtime': 3.0},
                    'prop': {'value': '4', 'mtime': 4.0},
                    }},
                {'document': 'document2'},
                {'guid': '5', 'diff': {
                    'guid': {'value': '5', 'mtime': 5.0},
                    'ctime': {'value': 6, 'mtime': 6.0},
                    'mtime': {'value': 7, 'mtime': 7.0},
                    }},
                {'commit': [[1, 2]]},
                ]
        self.assertEqual(([[1, 2]], [[101, 102]]), merge(volume, records))

        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 2, 'mtime': 3},
                volume['document1'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(1, os.stat('db/document1/1/1/guid').st_mtime)
        self.assertEqual(2, os.stat('db/document1/1/1/ctime').st_mtime)
        self.assertEqual(3, os.stat('db/document1/1/1/mtime').st_mtime)
        self.assertEqual(4, os.stat('db/document1/1/1/prop').st_mtime)

        self.assertEqual(
                {'guid': '5', 'ctime': 6, 'mtime': 7},
                volume['document2'].get('5').properties(['guid', 'ctime', 'mtime']))
        self.assertEqual(5, os.stat('db/document2/5/5/guid').st_mtime)
        self.assertEqual(6, os.stat('db/document2/5/5/ctime').st_mtime)
        self.assertEqual(7, os.stat('db/document2/5/5/mtime').st_mtime)

    def test_merge_Update(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(('db/seqno', '100'))
        volume = Volume('db', [Document])
        volume['document'].create(guid='1', prop='1', ctime=1, mtime=1)
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (2, 2))

        records = [
                {'document': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '2', 'mtime': 1.0}}},
                {'commit': [[1, 1]]},
                ]
        self.assertEqual(([[1, 1]], []), merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('db/document/1/1/prop').st_mtime)

        records = [
                {'document': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '3', 'mtime': 2.0}}},
                {'commit': [[2, 2]]},
                ]
        self.assertEqual(([[2, 2]], []), merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('db/document/1/1/prop').st_mtime)

        records = [
            {'document': 'document'},
            {'guid': '1', 'diff': {'prop': {'value': '4', 'mtime': 3.0}}},
            {'commit': [[3, 3]]},
            ]
        self.assertEqual(([[3, 3]], [[102, 102]]), merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(3, os.stat('db/document/1/1/prop').st_mtime)

    def test_merge_MultipleCommits(self):

        class Document(db.Document):
            pass

        self.touch(('db/seqno', '100'))
        volume = Volume('db', [Document])

        def generator():
            for i in [
                    {'document': 'document'},
                    {'commit': [[1, 1]]},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1.0},
                        'ctime': {'value': 2, 'mtime': 2.0},
                        'mtime': {'value': 3, 'mtime': 3.0},
                        'prop': {'value': '4', 'mtime': 4.0},
                        }},
                    {'commit': [[2, 3]]},
                    ]:
                yield i

        records = generator()
        self.assertEqual(([[1, 3]], [[101, 101]]), merge(volume, records))
        assert volume['document'].exists('1')

    def test_diff_Blobs(self):

        class Document(db.Document):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1)
        volume['document'].set_blob('1', 'prop', 'payload')
        self.utime('db', 0)

        in_seq = util.Sequence([[1, None]])
        self.assertEqual([
            {'document': 'document'},
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 0},
                'mtime': {'value': 0, 'mtime': 0},
                'ctime': {'value': 0, 'mtime': 0},
                'prop': {
                    'blob': tests.tmpdir + '/db/document/1/1/prop.blob',
                    'digest': hashlib.sha1('payload').hexdigest(),
                    'mime_type': 'application/octet-stream',
                    'mtime': 0,
                    },
                }},
            {'commit': [[1, 1]]},
            ],
            [i for i in diff(volume, in_seq)])

    def test_merge_Blobs(self):

        class Document(db.Document):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = Volume('db', [Document])

        merge(volume, [
            {'document': 'document'},
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 1.0},
                'ctime': {'value': 2, 'mtime': 2.0},
                'mtime': {'value': 3, 'mtime': 3.0},
                'prop': {
                    'blob': StringIO('payload'),
                    'blob_size': len('payload'),
                    'digest': hashlib.sha1('payload').hexdigest(),
                    'mime_type': 'foo/bar',
                    'mtime': 1,
                    },
                }},
            {'commit': [[1, 1]]},
            ])

        assert volume['document'].exists('1')
        blob = volume['document'].get('1')['prop']
        self.assertEqual(1, blob['mtime'])
        self.assertEqual('foo/bar', blob['mime_type'])
        self.assertEqual(hashlib.sha1('payload').hexdigest(), blob['digest'])
        self.assertEqual(tests.tmpdir + '/db/document/1/1/prop.blob', blob['blob'])
        self.assertEqual('payload', file(blob['blob']).read())

    def test_diff_ByLayers(self):

        class Context(Resource):
            pass

        class Implementation(Resource):
            pass

        class Review(Resource):
            pass

        volume = Volume('db', [Context, Implementation, Review])
        volume['context'].create(guid='1', ctime=1, mtime=1, layer='layer1')
        volume['implementation'].create(guid='2', ctime=2, mtime=2, layer='layer2')
        volume['review'].create(guid='3', ctime=3, mtime=3, layer='layer3')
        volume['context'].update(guid='1', tags='1')
        volume['implementation'].update(guid='2', tags='2')
        volume['review'].update(guid='3', tags='3')
        self.utime('db', 0)

        self.assertEqual(sorted([
            {'document': 'context'},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'document': 'implementation'},
            {'guid': '2', 'diff': {'tags': {'value': '2', 'mtime': 0}}},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[4, 6]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[4, None]]))]))

        self.assertEqual(sorted([
            {'document': 'context'},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'document': 'implementation'},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[4, 6]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[4, None]]), layer='layer1')]))

        self.assertEqual(sorted([
            {'document': 'context'},
            {'document': 'implementation'},
            {'guid': '2', 'diff': {'tags': {'value': '2', 'mtime': 0}}},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 6]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[4, None]]), layer='layer2')]))

        self.assertEqual(sorted([
            {'document': 'context'},
            {'document': 'implementation'},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[6, 6]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[4, None]]), layer='foo')]))


if __name__ == '__main__':
    tests.main()
