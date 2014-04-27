#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db
from sugar_network.client import model
from sugar_network.toolkit.router import ACL, File
from sugar_network.toolkit.coroutine import this


class ClientModelTest(tests.Test):

    def test_dump_volume_Post(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': '1', 'prop': '1'})
        volume['document'].create({'guid': '2', 'prop': '2'})
        volume['document'].create({'guid': '3', 'prop': '3'})

        self.assertEqual([
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '3', 'prop': '3'}, 'keys': ['guid']},
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '2', 'prop': '2'}, 'keys': ['guid']},
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '1', 'prop': '1'}, 'keys': ['guid']},
            ],
            [i for i in model.dump_volume(volume)])

    def test_dump_volume_SkipSeqnolessObjects(self):

        class Document(db.Resource):
            pass

        volume = db.Volume('.', [Document])

        volume['document']['1'].post('guid', '1')
        volume['document'].create({'guid': '2'})
        volume['document']['3'].post('guid', '3')

        self.assertEqual([
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '2'}, 'keys': ['guid']},
            ],
            [i for i in model.dump_volume(volume)])

    def test_dump_volume_SkipSeqnolessProps(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property(acl=ACL.LOCAL | ACL.PUBLIC)
            def prop2(self, value):
                return value

            @db.stored_property()
            def prop3(self, value):
                return value

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': 'guid', 'prop1': 'a', 'prop2': 'b', 'prop3': 'c'})

        self.assertEqual([
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': 'guid', 'prop1': 'a', 'prop3': 'c'}, 'keys': ['guid']},
            ],
            [i for i in model.dump_volume(volume)])

    def test_dump_volume_Put(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property(acl=ACL.LOCAL | ACL.PUBLIC)
            def prop2(self, value):
                return value

        volume = db.Volume('.', [Document])

        doc = volume['document']['1']
        doc.post('guid', '1')
        volume['document'].update('1', {'prop1': 'a', 'prop2': '1'})

        doc = volume['document']['2']
        doc.post('guid', '2')
        volume['document'].update('2', {'prop1': 'b', 'prop2': '2'})

        doc = volume['document']['3']
        doc.post('guid', '3')
        volume['document'].update('3', {'prop1': 'c', 'prop2': '3'})

        self.assertEqual([
            {'op': {'method': 'PUT', 'path': ['document', '3']}, 'content': {'prop1': 'c'}},
            {'op': {'method': 'PUT', 'path': ['document', '2']}, 'content': {'prop1': 'b'}},
            {'op': {'method': 'PUT', 'path': ['document', '1']}, 'content': {'prop1': 'a'}},
            ],
            [i for i in model.dump_volume(volume)])

    def test_dump_volume_Blobs(self):

        class Document(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume('.', [Document])

        blob1 = volume.blobs.post('blob1')
        blob2 = volume.blobs.post('blob2')
        blob3 = volume.blobs.post('blob3')

        volume['document'].create({'guid': '1', 'blob': blob1.digest})
        volume['document'].create({'guid': '2', 'blob': blob2.digest})
        volume['document'].create({'guid': '3', 'blob': blob3.digest})

        dump = [i for i in model.dump_volume(volume)]
        self.assertEqual([
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '3'}, 'keys': ['guid']},
            {'content-length': '5', 'content-type': 'application/octet-stream', 'x-seqno': '3',
                'op': {'method': 'PUT', 'path': ['document', '3', 'blob']},
                },
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '2'}, 'keys': ['guid']},
            {'content-length': '5', 'content-type': 'application/octet-stream', 'x-seqno': '2',
                'op': {'method': 'PUT', 'path': ['document', '2', 'blob']},
                },
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '1'}, 'keys': ['guid']},
            {'content-length': '5', 'content-type': 'application/octet-stream', 'x-seqno': '1',
                'op': {'method': 'PUT', 'path': ['document', '1', 'blob']},
                },
            ],
            [i.meta if type(i) is File else i for i in dump])
        self.assertEqual('blob3', file(dump[1].path).read())
        self.assertEqual('blob2', file(dump[3].path).read())
        self.assertEqual('blob1', file(dump[5].path).read())

    def test_dump_volume_Aggregates(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, subtype=db.Blob())
            def prop2(self, value):
                return value

        volume = this.volume = db.Volume('.', [Document])

        blob1 = volume.blobs.post('blob1')
        blob2 = volume.blobs.post('blob2')
        blob3 = volume.blobs.post('blob3')

        volume['document'].create({
            'guid': '1',
            'prop1': {
                '1a': {'value': 'a'},
                '1b': {'value': 'b'},
                '1c': {'value': 'c'},
                },
            'prop2': {
                '2a': {'value': blob1.digest},
                '2b': {'value': blob2.digest},
                '2c': {'value': blob3.digest},
                },
            })

        dump = [i for i in model.dump_volume(volume)]
        self.assertEqual([
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '1'}, 'keys': ['guid']},
            {'op': {'method': 'POST', 'path': ['document', '1', 'prop1', '1a']}, 'content': 'a'},
            {'op': {'method': 'POST', 'path': ['document', '1', 'prop1', '1c']}, 'content': 'c'},
            {'op': {'method': 'POST', 'path': ['document', '1', 'prop1', '1b']}, 'content': 'b'},
            {'content-length': '5', 'content-type': 'application/octet-stream', 'x-seqno': '1',
                'op': {'method': 'POST', 'path': ['document', '1', 'prop2', '2a']}},
            {'content-length': '5', 'content-type': 'application/octet-stream', 'x-seqno': '2',
                'op': {'method': 'POST', 'path': ['document', '1', 'prop2', '2b']}},
            {'content-length': '5', 'content-type': 'application/octet-stream', 'x-seqno': '3',
                'op': {'method': 'POST', 'path': ['document', '1', 'prop2', '2c']}},
            ],
            [i.meta if type(i) is File else i for i in dump])
        self.assertEqual('blob1', file(dump[4].path).read())
        self.assertEqual('blob2', file(dump[5].path).read())
        self.assertEqual('blob3', file(dump[6].path).read())

    def test_dump_volume_References(self):

        class Document(db.Resource):

            @db.stored_property(db.Reference)
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': '1', 'prop': '1'})

        self.assertEqual([
            {'op': {'method': 'POST', 'path': ['document']}, 'content': {'guid': '1', 'prop': '1'}, 'keys': ['prop', 'guid']},
            ],
            [i for i in model.dump_volume(volume)])


if __name__ == '__main__':
    tests.main()
