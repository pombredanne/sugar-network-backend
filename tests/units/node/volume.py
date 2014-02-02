#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import urllib2
import hashlib
from cStringIO import StringIO

from __init__ import tests

from sugar_network import db, toolkit, model
from sugar_network.node.volume import diff, merge
from sugar_network.node.stats_node import Sniffer
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, Blob, ACL, route


current_time = time.time


class VolumeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

    def test_diff(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid1 = call(cp, method='POST', document='document', content={'prop': 'a'})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = call(cp, method='POST', document='document', content={'prop': 'b'})
        self.utime('db/document/%s/%s' % (guid2[:2], guid2), 2)

        in_seq = toolkit.Sequence([[1, None]])
        self.assertEqual([
            {'resource': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'value': 'a', 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 2},
                    'mtime': {'value': 0, 'mtime': 2},
                    'ctime': {'value': 0, 'mtime': 2},
                    'prop': {'value': 'b', 'mtime': 2},
                    'author': {'mtime': 2, 'value': {}},
                    'layer': {'mtime': 2, 'value': []},
                    'tags': {'mtime': 2, 'value': []},
                    },
                },
            {'commit': [[1, 2]]},
            ],
            [i for i in diff(volume, in_seq)])
        self.assertEqual([[1, None]], in_seq)

    def test_diff_Partial(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid1 = call(cp, method='POST', document='document', content={'prop': 'a'})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = call(cp, method='POST', document='document', content={'prop': 'b'})
        self.utime('db/document/%s/%s' % (guid2[:2], guid2), 2)

        in_seq = toolkit.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual(guid1, next(patch)['guid'])
        self.assertEqual({'commit': []}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

        patch = diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual(guid1, next(patch)['guid'])
        self.assertEqual(guid2, next(patch)['guid'])
        self.assertEqual({'commit': [[1, 1]]}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

    def test_diff_Stretch(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid1 = call(cp, method='POST', document='document', content={'prop': 'a'})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = call(cp, method='POST', document='document', content={'prop': 'b'})
        volume['document'].delete(guid2)
        guid3 = call(cp, method='POST', document='document', content={'prop': 'c'})
        self.utime('db/document/%s/%s' % (guid3[:2], guid3), 2)
        guid4 = call(cp, method='POST', document='document', content={'prop': 'd'})
        volume['document'].delete(guid4)
        guid5 = call(cp, method='POST', document='document', content={'prop': 'f'})
        self.utime('db/document/%s/%s' % (guid5[:2], guid5), 2)

        in_seq = toolkit.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, patch.send(None))
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
        self.assertEqual({'resource': 'document'}, patch.send(None))
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

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid1 = call(cp, method='POST', document='document', content={'prop': 'a'})
        volume['document'].delete(guid1)
        guid2 = call(cp, method='POST', document='document', content={'prop': 'b'})
        volume['document'].delete(guid2)
        guid3 = call(cp, method='POST', document='document', content={'prop': 'c'})
        self.utime('db/document/%s/%s' % (guid3[:2], guid3), 2)
        guid4 = call(cp, method='POST', document='document', content={'prop': 'd'})
        volume['document'].delete(guid4)
        guid5 = call(cp, method='POST', document='document', content={'prop': 'f'})
        self.utime('db/document/%s/%s' % (guid5[:2], guid5), 2)

        in_seq = toolkit.Sequence([[1, None]])
        patch = diff(volume, in_seq, toolkit.Sequence([[1, 1]]))
        self.assertEqual({'resource': 'document'}, patch.send(None))
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3], [5, 5]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_TheSameInSeqForAllDocuments(self):

        class Document1(db.Resource):
            pass

        class Document2(db.Resource):
            pass

        class Document3(db.Resource):
            pass

        volume = db.Volume('db', [Document1, Document2, Document3])
        cp = NodeRoutes('guid', volume)

        guid3 = call(cp, method='POST', document='document1', content={})
        self.utime('db/document/%s/%s' % (guid3[:2], guid3), 3)
        guid2 = call(cp, method='POST', document='document2', content={})
        self.utime('db/document/%s/%s' % (guid2[:2], guid2), 2)
        guid1 = call(cp, method='POST', document='document3', content={})
        self.utime('db/document/%s/%s' % (guid1[:2], guid1), 1)

        in_seq = toolkit.Sequence([[1, None]])
        patch = diff(volume, in_seq)
        self.assertEqual({'resource': 'document1'}, patch.send(None))
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual({'resource': 'document2'}, patch.send(None))
        self.assertEqual(guid2, patch.send(None)['guid'])
        self.assertEqual({'resource': 'document3'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 3]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_merge_Create(self):

        class Document1(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        class Document2(db.Resource):
            pass

        self.touch(('db/seqno', '100'))
        volume = db.Volume('db', [Document1, Document2])

        records = [
                {'resource': 'document1'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 1.0},
                    'ctime': {'value': 2, 'mtime': 2.0},
                    'mtime': {'value': 3, 'mtime': 3.0},
                    'prop': {'value': '4', 'mtime': 4.0},
                    }},
                {'resource': 'document2'},
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

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(('db/seqno', '100'))
        volume = db.Volume('db', [Document])
        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (2, 2))

        records = [
                {'resource': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '2', 'mtime': 1.0}}},
                {'commit': [[1, 1]]},
                ]
        self.assertEqual(([[1, 1]], []), merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('db/document/1/1/prop').st_mtime)

        records = [
                {'resource': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '3', 'mtime': 2.0}}},
                {'commit': [[2, 2]]},
                ]
        self.assertEqual(([[2, 2]], []), merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('db/document/1/1/prop').st_mtime)

        records = [
            {'resource': 'document'},
            {'guid': '1', 'diff': {'prop': {'value': '4', 'mtime': 3.0}}},
            {'commit': [[3, 3]]},
            ]
        self.assertEqual(([[3, 3]], [[102, 102]]), merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(3, os.stat('db/document/1/1/prop').st_mtime)

    def test_merge_MultipleCommits(self):

        class Document(db.Resource):
            pass

        self.touch(('db/seqno', '100'))
        volume = db.Volume('db', [Document])

        def generator():
            for i in [
                    {'resource': 'document'},
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

    def test_merge_UpdateStats(self):
        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume)
        stats = Sniffer(volume, 'stats/node')

        records = [
                {'resource': 'context'},
                {'guid': 'context', 'diff': {
                    'guid': {'value': 'context', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'type': {'value': ['package'], 'mtime': 1.0},
                    'title': {'value': {}, 'mtime': 1.0},
                    'summary': {'value': {}, 'mtime': 1.0},
                    'description': {'value': {}, 'mtime': 1.0},
                    }},
                {'resource': 'post'},
                {'guid': 'topic_1', 'diff': {
                    'guid': {'value': 'topic_1', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'type': {'value': 'object', 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'title': {'value': {}, 'mtime': 1.0},
                    'message': {'value': {}, 'mtime': 1.0},
                    'solution': {'value': 'solution_1', 'mtime': 1.0},
                    }},
                {'guid': 'topic_2', 'diff': {
                    'guid': {'value': 'topic_2', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'type': {'value': 'object', 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'title': {'value': {}, 'mtime': 1.0},
                    'message': {'value': {}, 'mtime': 1.0},
                    'solution': {'value': 'solution_2', 'mtime': 1.0},
                    }},
                {'guid': 'context_review', 'diff': {
                    'guid': {'value': 'context_review', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'vote': {'value': 1, 'mtime': 1.0},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'type': {'value': 'review', 'mtime': 1.0},
                    }},
                {'guid': 'topic_review', 'diff': {
                    'guid': {'value': 'topic_review', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'topic': {'value': 'topic_1', 'mtime': 1.0},
                    'vote': {'value': 1, 'mtime': 1.0},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'type': {'value': 'feedback', 'mtime': 1.0},
                    }},
                {'guid': 'solution_1', 'diff': {
                    'guid': {'value': 'solution_1', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'topic': {'value': 'topic_1', 'mtime': 1.0},
                    'type': {'value': 'answer', 'mtime': 1.0},
                    'title': {'value': {}, 'mtime': 1.0},
                    'message': {'value': {}, 'mtime': 1.0},
                    }},
                {'guid': 'solution_2', 'diff': {
                    'guid': {'value': 'solution_2', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'topic': {'value': 'topic_2', 'mtime': 1.0},
                    'type': {'value': 'answer', 'mtime': 1.0},
                    'title': {'value': {}, 'mtime': 1.0},
                    'message': {'value': {}, 'mtime': 1.0},
                    }},
                {'resource': 'release'},
                {'guid': 'release', 'diff': {
                    'guid': {'value': 'release', 'mtime': 1.0},
                    'ctime': {'value': 1, 'mtime': 1.0},
                    'mtime': {'value': 1, 'mtime': 1.0},
                    'context': {'value': 'context', 'mtime': 1.0},
                    'license': {'value': ['GPL-3.0'], 'mtime': 1.0},
                    'version': {'value': '1', 'mtime': 1.0},
                    'stability': {'value': 'stable', 'mtime': 1.0},
                    'notes': {'value': {}, 'mtime': 1.0},
                    }},
                {'commit': [[1, 1]]},
                ]
        merge(volume, records, stats=stats)
        ts = int(current_time())
        stats.commit(ts)
        stats.commit_objects()

        self.assertEqual([
            [('post', ts, {
                'downloaded': 0.0,
                'total': 6.0,
                })],
            [('user', ts, {
                'total': 0.0,
                })],
            [('context', ts, {
                'failed': 0.0,
                'downloaded': 0.0,
                'total': 1.0,
                'released': 1.0,
                })],
            ],
            [[(j.name,) + i for i in j.get(j.last, j.last)] for j in Rrd('stats/node', 1)])
        self.assertEqual([1, 1], volume['context'].get('context')['rating'])
        self.assertEqual([1, 1], volume['post'].get('topic_1')['rating'])

        records = [
                {'resource': 'post'},
                {'guid': 'topic_2', 'diff': {'solution': {'value': '', 'mtime': 2.0}}},
                {'commit': [[2, 2]]},
                ]
        merge(volume, records, stats=stats)
        ts += 1
        stats.commit(ts)
        stats.commit_objects()

        self.assertEqual([
            [('post', ts, {
                'downloaded': 0.0,
                'total': 6.0,
                })],
            [('user', ts, {
                'total': 0.0,
                })],
            [('context', ts, {
                'failed': 0.0,
                'downloaded': 0.0,
                'total': 1.0,
                'released': 1.0,
                })],
            ],
            [[(j.name,) + i for i in j.get(j.last, j.last)] for j in Rrd('stats/node', 1)])

        records = [
                {'resource': 'context'},
                {'guid': 'context', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'resource': 'post'},
                {'guid': 'topic_1', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'guid': 'topic_2', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'guid': 'context_review', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'guid': 'topic_review', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'guid': 'solution_1', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'guid': 'solution_2', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'resource': 'release'},
                {'guid': 'release', 'diff': {'layer': {'value': ['deleted'], 'mtime': 3.0}}},
                {'commit': [[3, 3]]},
                ]
        merge(volume, records, stats=stats)
        ts += 1
        stats.commit(ts)
        stats.commit_objects()

        self.assertEqual([
            [('post', ts, {
                'downloaded': 0.0,
                'total': 0.0,
                })],
            [('user', ts, {
                'total': 0.0,
                })],
            [('context', ts, {
                'failed': 0.0,
                'downloaded': 0.0,
                'total': 0.0,
                'released': 1.0,
                })],
            ],
            [[(j.name,) + i for i in j.get(j.last, j.last)] for j in Rrd('stats/node', 1)])

    def test_diff_Blobs(self):

        class Document(db.Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid = call(cp, method='POST', document='document', content={})
        call(cp, method='PUT', document='document', guid=guid, content={'prop': 'payload'})
        self.utime('db', 0)

        patch = diff(volume, toolkit.Sequence([[1, None]]))
        self.assertEqual(
                {'resource': 'document'},
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
                    'author': {'mtime': 0, 'value': {}},
                    'layer': {'mtime': 0, 'value': []},
                    'tags': {'mtime': 0, 'value': []},
                    'mtime': {'value': 0, 'mtime': 0},
                    'ctime': {'value': 0, 'mtime': 0},
                    }},
                next(patch))
        self.assertEqual(
                {'commit': [[1, 2]]},
                next(patch))
        self.assertRaises(StopIteration, next, patch)

    def test_diff_BlobUrls(self):
        url = 'http://src.sugarlabs.org/robots.txt'
        blob = urllib2.urlopen(url).read()

        class Document(db.Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid = call(cp, method='POST', document='document', content={})
        call(cp, method='PUT', document='document', guid=guid, content={'prop': {'url': url}})
        self.utime('db', 1)

        self.assertEqual([
            {'resource': 'document'},
            {'guid': guid,
                'diff': {
                    'guid': {'value': guid, 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'url': url, 'mtime': 1},
                    },
                },
            {'commit': [[1, 2]]},
            ],
            [i for i in diff(volume, toolkit.Sequence([[1, None]]))])

        patch = diff(volume, toolkit.Sequence([[1, None]]), fetch_blobs=True)
        self.assertEqual(
                {'resource': 'document'},
                next(patch))
        record = next(patch)
        self.assertEqual(blob, ''.join([i for i in record.pop('blob')]))
        self.assertEqual(
                {'guid': guid, 'blob_size': len(blob), 'diff': {'prop': {'mtime': 1}}},
                record)
        self.assertEqual(
                {'guid': guid, 'diff': {
                    'guid': {'value': guid, 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    }},
                next(patch))
        self.assertEqual(
                {'commit': [[1, 2]]},
                next(patch))
        self.assertRaises(StopIteration, next, patch)

    def test_diff_SkipBrokenBlobUrls(self):

        class Document(db.Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])
        cp = NodeRoutes('guid', volume)

        guid1 = call(cp, method='POST', document='document', content={})
        call(cp, method='PUT', document='document', guid=guid1, content={'prop': {'url': 'http://foo/bar'}})
        guid2 = call(cp, method='POST', document='document', content={})
        self.utime('db', 1)

        self.assertEqual([
            {'resource': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'url': 'http://foo/bar', 'mtime': 1},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    },
                },
            {'commit': [[1, 3]]},
            ],
            [i for i in diff(volume, toolkit.Sequence([[1, None]]), fetch_blobs=False)])

        self.assertEqual([
            {'resource': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    },
                },
            {'commit': [[1, 3]]},
            ],
            [i for i in diff(volume, toolkit.Sequence([[1, None]]), fetch_blobs=True)])

    def test_merge_Blobs(self):

        class Document(db.Resource):

            @db.blob_property()
            def prop(self, value):
                return value

        volume = db.Volume('db', [Document])

        merge(volume, [
            {'resource': 'document'},
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

        class Context(db.Resource):
            pass

        class release(db.Resource):
            pass

        class Review(db.Resource):
            pass

        volume = db.Volume('db', [Context, release, Review])
        volume['context'].create({'guid': '0', 'ctime': 1, 'mtime': 1, 'layer': ['layer0', 'common']})
        volume['context'].create({'guid': '1', 'ctime': 1, 'mtime': 1, 'layer': 'layer1'})
        volume['release'].create({'guid': '2', 'ctime': 2, 'mtime': 2, 'layer': 'layer2'})
        volume['review'].create({'guid': '3', 'ctime': 3, 'mtime': 3, 'layer': 'layer3'})

        volume['context'].update('0', {'tags': '0'})
        volume['context'].update('1', {'tags': '1'})
        volume['release'].update('2', {'tags': '2'})
        volume['review'].update('3', {'tags': '3'})
        self.utime('db', 0)

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'resource': 'release'},
            {'guid': '2', 'diff': {'tags': {'value': '2', 'mtime': 0}}},
            {'resource': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, toolkit.Sequence([[5, None]]))]))

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'resource': 'release'},
            {'resource': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, toolkit.Sequence([[5, None]]), layer='layer1')]))

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'resource': 'release'},
            {'guid': '2', 'diff': {'tags': {'value': '2', 'mtime': 0}}},
            {'resource': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, toolkit.Sequence([[5, None]]), layer='layer2')]))

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'resource': 'release'},
            {'resource': 'review'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[5, 8]]},
            ]),
            sorted([i for i in diff(volume, toolkit.Sequence([[5, None]]), layer='foo')]))


def call(routes, method, document=None, guid=None, prop=None, cmd=None, content=None, **kwargs):
    path = []
    if document:
        path.append(document)
    if guid:
        path.append(guid)
    if prop:
        path.append(prop)
    request = Request(method=method, path=path, cmd=cmd, content=content)
    request.update(kwargs)
    request.environ['HTTP_HOST'] = '127.0.0.1'
    router = Router(routes)
    return router.call(request, Response())


if __name__ == '__main__':
    tests.main()
