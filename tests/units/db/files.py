
    def test_diff_WithBlobsSetByUrl(self):
        URL = 'http://src.sugarlabs.org/robots.txt'
        URL_content = urllib2.urlopen(URL).read()

        class Document(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': '1', 'ctime': 1, 'mtime': 1})
        directory.update('1', {'blob': {'url': URL}})
        self.utime('1/1', 1)

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'blob': {
                    'url': URL,
                    'mtime': 1,
                    },
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq)])
        self.assertEqual([[1, 2]], out_seq)

    def test_merge_AvoidCalculatedBlobs(self):

        class Document(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return {'url': 'http://foo/bar', 'mime_type': 'image/png'}

        directory1 = Directory('document1', Document, IndexWriter)
        directory1.create({'guid': 'guid', 'ctime': 1, 'mtime': 1})
        for i in os.listdir('document1/gu/guid'):
            os.utime('document1/gu/guid/%s' % i, (1, 1))

        directory2 = Directory('document2', Document, IndexWriter)
        for patch in diff(directory1, [[0, None]], Sequence()):
            directory2.merge(**patch)

        doc = directory2.get('guid')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        assert not exists('document2/gu/guid/blob')

    def test_merge_Blobs(self):

        class Document(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

        directory = Directory('document', Document, IndexWriter)
        self.touch(('blob', 'blob-1'))
        directory.merge('1', {
            'guid': {'mtime': 1, 'value': '1'},
            'ctime': {'mtime': 2, 'value': 2},
            'mtime': {'mtime': 3, 'value': 3},
            'blob': {'mtime': 4, 'blob': 'blob'},
            })

        self.assertEqual(
                [(2, 3, '1')],
                [(i['ctime'], i['mtime'], i['guid']) for i in directory.find()[0]])

        doc = directory.get('1')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(4, doc.meta('blob')['mtime'])
        self.assertEqual('blob-1', file('document/1/1/blob.blob').read())

        self.touch(('blob', 'blob-2'))
        directory.merge('1', {
            'blob': {'mtime': 5, 'blob': 'blob'},
            })

        self.assertEqual(5, doc.meta('blob')['mtime'])
        self.assertEqual('blob-2', file('document/1/1/blob.blob').read())


    def test_DeleteOldBlobOnUpdate(self):

        class Document(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': 'guid', 'blob': 'foo'})
        assert exists('gu/guid/blob.blob')
        directory.update('guid', {'blob': {'url': 'foo'}})
        assert not exists('gu/guid/blob.blob')

        directory.update('guid', {'blob': 'foo'})
        assert exists('gu/guid/blob.blob')
        directory.update('guid', {'blob': {}})
        assert not exists('gu/guid/blob.blob')

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

