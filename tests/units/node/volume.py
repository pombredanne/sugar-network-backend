#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import urllib2
import hashlib
from cStringIO import StringIO

from __init__ import tests

from sugar_network import db
from sugar_network.node.volume import diff, merge
from sugar_network.node.stats_node import stats_node_step, Sniffer
from sugar_network.node.commands import NodeCommands
from sugar_network.resources.user import User
from sugar_network.resources.volume import Volume, Resource
from sugar_network.resources.review import Review
from sugar_network.resources.context import Context
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.feedback import Feedback
from sugar_network.resources.solution import Solution
from sugar_network.toolkit import util


class VolumeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.override(time, 'time', lambda: 0)

    def test_diff(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        cp = NodeCommands('guid', volume)

        guid1 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'a'})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'b'})
        self.utime('db/document/%s/%s' % (guid2[:2], guid2), 2)

        in_seq = util.Sequence([[1, None]])
        self.assertEqual([
            {'document': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'value': 'a', 'mtime': 1},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 2},
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
        cp = NodeCommands('guid', volume)

        guid1 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'a'})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'b'})
        self.utime('db/document/%s/%s' % (guid2[:2], guid2), 2)

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, next(patch))
        self.assertEqual(guid1, next(patch)['guid'])
        self.assertEqual({'commit': []}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, next(patch))
        self.assertEqual(guid1, next(patch)['guid'])
        self.assertEqual(guid2, next(patch)['guid'])
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
        cp = NodeCommands('guid', volume)

        guid1 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'a'})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'b'})
        volume['document'].delete(guid2)
        guid3 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'c'})
        self.utime('db/document/%s/%s' % (guid3[:2], guid3), 2)
        guid4 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'd'})
        volume['document'].delete(guid4)
        guid5 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'f'})
        self.utime('db/document/%s/%s' % (guid5[:2], guid5), 2)

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3]]}, patch.throw(StopIteration()))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
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
        cp = NodeCommands('guid', volume)

        guid1 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'a'})
        volume['document'].delete(guid1)
        guid2 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'b'})
        volume['document'].delete(guid2)
        guid3 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'c'})
        self.utime('db/document/%s/%s' % (guid3[:2], guid3), 2)
        guid4 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'd'})
        volume['document'].delete(guid4)
        guid5 = call(cp, method='POST', document='document', principal='principal', content={'prop': 'f'})
        self.utime('db/document/%s/%s' % (guid5[:2], guid5), 2)

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq, util.Sequence([[1, 1]]))
        self.assertEqual({'document': 'document'}, patch.send(None))
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
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
        cp = NodeCommands('guid', volume)

        guid3 = call(cp, method='POST', document='document1', principal='principal', content={})
        self.utime('db/document/%s/%s' % (guid3[:2], guid3), 3)
        guid2 = call(cp, method='POST', document='document2', principal='principal', content={})
        self.utime('db/document/%s/%s' % (guid2[:2], guid2), 2)
        guid1 = call(cp, method='POST', document='document3', principal='principal', content={})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)

        in_seq = util.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'document': 'document1'}, patch.send(None))
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual({'document': 'document2'}, patch.send(None))
        self.assertEqual(guid2, patch.send(None)['guid'])
        self.assertEqual({'document': 'document3'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
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
        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
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

    def test_merge_UpdateReviewStats(self):
        stats_node_step.value = 1
        volume = Volume('db', [User, Context, Review, Feedback, Solution, Artifact])
        cp = NodeCommands('guid', volume)
        stats = Sniffer(volume)

        context = call(cp, method='POST', document='context', principal='principal', content={
                'guid': 'context',
                'type': 'package',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                })
        artifact = call(cp, method='POST', document='artifact', principal='principal', content={
                'guid': 'artifact',
                'type': 'instance',
                'context': context,
                'title': '',
                'description': '',
                })

        records = [
                {'document': 'review'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'context': {'value': context, 'mtime': 1.0},
                    'artifact': {'value': artifact, 'mtime': 4.0},
                    'rating': {'value': 1, 'mtime': 1.0},
                    }},
                {'guid': '2', 'diff': {
                    'guid': {'value': '2', 'mtime': 2.0},
                    'ctime': {'value': 2, 'mtime': 2.0},
                    'mtime': {'value': 2, 'mtime': 2.0},
                    'context': {'value': context, 'mtime': 2.0},
                    'rating': {'value': 2, 'mtime': 2.0},
                    }},
                {'commit': [[1, 2]]},
                ]
        merge(volume, records, node_stats=stats)

        stats.commit()
        self.assertEqual(1, volume['artifact'].get(artifact)['rating'])
        self.assertEqual([1, 1], volume['artifact'].get(artifact)['reviews'])
        self.assertEqual(2, volume['context'].get(context)['rating'])
        self.assertEqual([1, 2], volume['context'].get(context)['reviews'])

    def test_diff_Blobs(self):

        class Document(Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = Volume('db', [User, Document])
        cp = NodeCommands('guid', volume)

        guid = call(cp, method='POST', document='document', principal='principal', content={})
        call(cp, method='PUT', document='document', guid=guid, principal='principal', content={'prop': 'payload'})
        self.utime('db', 0)

        patch = diff(volume, util.Sequence([[1, None]]))
        self.assertEqual(
                {'document': 'document'},
                next(patch))
        record = next(patch)
        self.assertEqual('payload', ''.join([i for i in record.pop('blob')]))
        self.assertEqual(
                {'guid': guid, 'blob_size': len('payload'), 'diff': {
                    'prop': {
                        'digest': hashlib.sha1('payload').hexdigest(),
                        'blob_size': len('payload'),
                        'mime_type': 'application/octet-stream',
                        'mtime': 0,
                        },
                    }},
                record)
        self.assertEqual(
                {'guid': guid, 'diff': {
                    'guid': {'value': guid, 'mtime': 0},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 0},
                    'layer': {'mtime': 0, 'value': ['public']},
                    'tags': {'mtime': 0, 'value': []},
                    'mtime': {'value': 0, 'mtime': 0},
                    'ctime': {'value': 0, 'mtime': 0},
                    }},
                next(patch))
        self.assertEqual(
                {'document': 'user'},
                next(patch))
        self.assertEqual(
                {'commit': [[1, 2]]},
                next(patch))
        self.assertRaises(StopIteration, next, patch)

    def test_diff_BlobUrls(self):
        url = 'http://src.sugarlabs.org/robots.txt'
        blob = urllib2.urlopen(url).read()

        class Document(Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = Volume('db', [User, Document])
        cp = NodeCommands('guid', volume)

        guid = call(cp, method='POST', document='document', principal='principal', content={})
        call(cp, method='PUT', document='document', guid=guid, principal='principal', content={'prop': {'url': url}})
        self.utime('db', 1)

        self.assertEqual([
            {'document': 'document'},
            {'guid': guid,
                'diff': {
                    'guid': {'value': guid, 'mtime': 1},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 1},
                    'layer': {'mtime': 1, 'value': ['public']},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'url': url, 'mtime': 1},
                    },
                },
            {'document': 'user'},
            {'commit': [[1, 2]]},
            ],
            [i for i in diff(volume, util.Sequence([[1, None]]))])

        patch = diff(volume, util.Sequence([[1, None]]), fetch_blobs=True)
        self.assertEqual(
                {'document': 'document'},
                next(patch))
        record = next(patch)
        self.assertEqual(blob, ''.join([i for i in record.pop('blob')]))
        self.assertEqual(
                {'guid': guid, 'blob_size': len(blob), 'diff': {'prop': {'mtime': 1}}},
                record)
        self.assertEqual(
                {'guid': guid, 'diff': {
                    'guid': {'value': guid, 'mtime': 1},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 1},
                    'layer': {'mtime': 1, 'value': ['public']},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    }},
                next(patch))
        self.assertEqual(
                {'document': 'user'},
                next(patch))
        self.assertEqual(
                {'commit': [[1, 2]]},
                next(patch))
        self.assertRaises(StopIteration, next, patch)

    def test_diff_SkipBrokenBlobUrls(self):

        class Document(Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = Volume('db', [User, Document])
        cp = NodeCommands('guid', volume)

        guid1 = call(cp, method='POST', document='document', principal='principal', content={})
        call(cp, method='PUT', document='document', guid=guid1, principal='principal', content={'prop': {'url': 'http://foo/bar'}})
        guid2 = call(cp, method='POST', document='document', principal='principal', content={})
        self.utime('db', 1)

        self.assertEqual([
            {'document': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 1},
                    'layer': {'mtime': 1, 'value': ['public']},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'url': 'http://foo/bar', 'mtime': 1},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 1},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 1},
                    'layer': {'mtime': 1, 'value': ['public']},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    },
                },
            {'document': 'user'},
            {'commit': [[1, 3]]},
            ],
            [i for i in diff(volume, util.Sequence([[1, None]]), fetch_blobs=False)])

        self.assertEqual([
            {'document': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 1},
                    'layer': {'mtime': 1, 'value': ['public']},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 1},
                    'author': {'value': {'principal': {'order': 0, 'role': 2}}, 'mtime': 1},
                    'layer': {'mtime': 1, 'value': ['public']},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    },
                },
            {'document': 'user'},
            {'commit': [[1, 3]]},
            ],
            [i for i in diff(volume, util.Sequence([[1, None]]), fetch_blobs=True)])

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
        volume['context'].create({'guid': '0', 'ctime': 1, 'mtime': 1, 'layer': ['layer0', 'common']})
        volume['context'].create({'guid': '1', 'ctime': 1, 'mtime': 1, 'layer': 'layer1'})
        volume['implementation'].create({'guid': '2', 'ctime': 2, 'mtime': 2, 'layer': 'layer2'})
        volume['review'].create({'guid': '3', 'ctime': 3, 'mtime': 3, 'layer': 'layer3'})

        volume['context'].update('0', {'tags': '0'})
        volume['context'].update('1', {'tags': '1'})
        volume['implementation'].update('2', {'tags': '2'})
        volume['review'].update('3', {'tags': '3'})
        self.utime('db', 0)

        self.assertEqual(sorted([
            {'document': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'document': 'implementation'},
            {'guid': '2', 'diff': {'tags': {'value': '2', 'mtime': 0}}},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[5, None]]))]))

        self.assertEqual(sorted([
            {'document': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'document': 'implementation'},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[5, None]]), layer='layer1')]))

        self.assertEqual(sorted([
            {'document': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'document': 'implementation'},
            {'guid': '2', 'diff': {'tags': {'value': '2', 'mtime': 0}}},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[5, None]]), layer='layer2')]))

        self.assertEqual(sorted([
            {'document': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'document': 'implementation'},
            {'document': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, util.Sequence([[5, None]]), layer='foo')]))


def call(cp, principal=None, content=None, **kwargs):
    request = db.Request(**kwargs)
    request.principal = principal
    request.content = content
    request.environ = {'HTTP_HOST': '127.0.0.1'}
    return cp.call(request, db.Response())


if __name__ == '__main__':
    tests.main()
