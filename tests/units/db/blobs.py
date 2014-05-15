#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import hashlib
import mimetypes
from cStringIO import StringIO
from os.path import exists, abspath

from __init__ import tests

from sugar_network import toolkit
from sugar_network.db.blobs import Blobs
from sugar_network.toolkit.router import Request, File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, coroutine


class BlobsTest(tests.Test):

    def test_post(self):
        blobs = Blobs('.', Seqno())

        content = 'probe'
        blob = blobs.post(content)

        self.assertEqual(
                hashlib.sha1(content).hexdigest(),
                blob.digest)
        self.assertEqual(
                abspath('blobs/%s/%s' % (blob.digest[:2], blob.digest)),
                blob.path)
        self.assertEqual({
            'content-type': 'application/octet-stream',
            'content-length': str(len(content)),
            'x-seqno': '1',
            },
            blob.meta)

        self.assertEqual(
                content,
                file(blob.path).read())
        self.assertEqual(
                sorted([
                    'content-type: application/octet-stream',
                    'content-length: %s' % len(content),
                    'x-seqno: 1',
                    ]),
                sorted(file(blob.path + '.meta').read().strip().split('\n')))

        the_same_blob = blobs.get(blob.digest)
        assert the_same_blob is not blob
        assert the_same_blob == blob
        assert the_same_blob.digest == blob.digest
        assert the_same_blob.path == blob.path

    def test_post_Stream(self):
        blobs = Blobs('.', Seqno())

        content = 'probe'
        blob = blobs.post(StringIO(content))

        self.assertEqual(
                hashlib.sha1(content).hexdigest(),
                blob.digest)
        self.assertEqual(
                abspath('blobs/%s/%s' % (blob.digest[:2], blob.digest)),
                blob.path)
        self.assertEqual({
            'content-type': 'application/octet-stream',
            'content-length': str(len(content)),
            'x-seqno': '1',
            },
            blob.meta)

        self.assertEqual(
                content,
                file(blob.path).read())
        self.assertEqual(
                sorted([
                    'content-type: application/octet-stream',
                    'content-length: %s' % len(content),
                    'x-seqno: 1',
                    ]),
                sorted(file(blob.path + '.meta').read().strip().split('\n')))

        the_same_blob = blobs.get(blob.digest)
        assert the_same_blob is not blob
        assert the_same_blob == blob
        assert the_same_blob.digest == blob.digest
        assert the_same_blob.path == blob.path

    def test_post_Url(self):
        blobs = Blobs('.', Seqno())

        self.assertRaises(http.BadRequest, blobs.post, {})
        self.assertRaises(http.BadRequest, blobs.post, {'digest': 'digest'})
        blob = blobs.post({
            'location': 'location',
            'digest': '0000000000000000000000000000000000000000',
            'content-length': '101',
            'foo': 'bar',
            })

        self.assertEqual(
                '0000000000000000000000000000000000000000',
                blob.digest)
        assert blob.path is None
        self.assertEqual({
            'status': '301 Moved Permanently',
            'location': 'location',
            'content-type': 'application/octet-stream',
            'content-length': '101',
            'x-seqno': '1',
            },
            blob.meta)
        self.assertEqual(
                sorted([
                    'status: 301 Moved Permanently',
                    'location: location',
                    'content-type: application/octet-stream',
                    'content-length: 101',
                    'x-seqno: 1',
                    ]),
                sorted(file('blobs/%s/%s.meta' % (blob.digest[:2], blob.digest)).read().strip().split('\n')))

        the_same_blob = blobs.get(blob.digest)
        assert the_same_blob is not blob
        assert the_same_blob == blob
        assert the_same_blob.digest == blob.digest
        assert the_same_blob.path == blob.path

    def test_update(self):
        blobs = Blobs('.', Seqno())

        blob = blobs.post('probe')
        self.assertEqual({
            'content-type': 'application/octet-stream',
            'content-length': str(len('probe')),
            'x-seqno': '1',
            },
            blob.meta)

        blobs.update(blob.digest, {'foo': 'bar'})
        self.assertEqual({
            'content-type': 'application/octet-stream',
            'content-length': str(len('probe')),
            'x-seqno': '1',
            'foo': 'bar',
            },
            blobs.get(blob.digest).meta)

    def test_delete(self):
        blobs = Blobs('.', Seqno())

        blob = blobs.post('probe')
        assert exists(blob.path)
        assert exists(blob.path + '.meta')
        self.assertEqual({
            'content-length': '5',
            'content-type': 'application/octet-stream',
            'x-seqno': '1',
            },
            blobs.get(blob.digest).meta)

        blobs.delete(blob.digest)
        assert not exists(blob.path)
        assert exists(blob.path + '.meta')
        self.assertEqual({
            'content-length': '5',
            'content-type': 'application/octet-stream',
            'status': '410 Gone',
            'x-seqno': '2',
            },
            blobs.get(blob.digest).meta)

    def test_diff_Blobs(self):
        blobs = Blobs('.', Seqno())
        this.request = Request()

        self.touch('blobs/10/1000000000000000000000000000000000000001',
                  ('blobs/10/1000000000000000000000000000000000000001.meta', 'n: 1\nx-seqno: 1'))
        self.utime('blobs/10/1000000000000000000000000000000000000001', 1)
        self.utime('blobs/10/1000000000000000000000000000000000000001.meta', 1)
        self.touch('blobs/10/1000000000000000000000000000000000000002',
                  ('blobs/10/1000000000000000000000000000000000000002.meta', 'n: 2\nx-seqno: 2'))
        self.utime('blobs/10/1000000000000000000000000000000000000002', 2)
        self.utime('blobs/10/1000000000000000000000000000000000000002.meta', 2)
        self.touch('blobs/20/2000000000000000000000000000000000000003',
                  ('blobs/20/2000000000000000000000000000000000000003.meta', 'n: 3\nx-seqno: 3'))
        self.utime('blobs/20/2000000000000000000000000000000000000003', 3)
        self.utime('blobs/20/2000000000000000000000000000000000000003.meta', 3)

        self.assertEqual([
            ('2000000000000000000000000000000000000003', {'n': '3', 'x-seqno': '3'}),
            ('1000000000000000000000000000000000000002', {'n': '2', 'x-seqno': '2'}),
            ('1000000000000000000000000000000000000001', {'n': '1', 'x-seqno': '1'}),
            ],
            [(i.digest, i.meta) for i in  blobs.diff([[1, None]])])
        self.assertEqual([
            ],
            [(i.digest, i.meta) for i in  blobs.diff([[4, None]])])

        self.touch('blobs/20/2000000000000000000000000000000000000004',
                  ('blobs/20/2000000000000000000000000000000000000004.meta', 'n: 4\nx-seqno: 4'))
        self.utime('blobs/20/2000000000000000000000000000000000000004', 4)
        self.utime('blobs/20/2000000000000000000000000000000000000004.meta', 4)
        self.touch('blobs/30/3000000000000000000000000000000000000005',
                  ('blobs/30/3000000000000000000000000000000000000005.meta', 'n: 5\nx-seqno: 5'))
        self.utime('blobs/30/3000000000000000000000000000000000000005', 5)
        self.utime('blobs/30/3000000000000000000000000000000000000005.meta', 5)

        self.assertEqual([
            ('3000000000000000000000000000000000000005', {'n': '5', 'x-seqno': '5'}),
            ('2000000000000000000000000000000000000004', {'n': '4', 'x-seqno': '4'}),
            ],
            [(i.digest, i.meta) for i in  blobs.diff([[4, None]])])
        self.assertEqual([
            ],
            [i for i in  blobs.diff([[6, None]])])

        self.assertEqual([
            ('3000000000000000000000000000000000000005', {'n': '5', 'x-seqno': '5'}),
            ('2000000000000000000000000000000000000004', {'n': '4', 'x-seqno': '4'}),
            ('2000000000000000000000000000000000000003', {'n': '3', 'x-seqno': '3'}),
            ('1000000000000000000000000000000000000002', {'n': '2', 'x-seqno': '2'}),
            ('1000000000000000000000000000000000000001', {'n': '1', 'x-seqno': '1'}),
            ],
            [(i.digest, i.meta) for i in  blobs.diff([[1, None]])])

    def test_diff_Files(self):
        blobs = Blobs('.', Seqno())
        this.request = Request()

        self.touch('foo/bar', ('foo/bar.meta', 'n: -1\nx-seqno: -1'))
        self.utime('foo/bar', 1)
        self.utime('foo/bar.meta', 1)

        self.assertEqual(
                sorted([]),
                sorted([i.digest for i in  blobs.diff([[1, None]])]))

        self.touch('files/1', ('files/1.meta', 'n: 1\nx-seqno: 1'))
        self.utime('files/1', 1)
        self.utime('files/1.meta', 1)
        self.touch('files/2/3', ('files/2/3.meta', 'n: 2\nx-seqno: 2'))
        self.utime('files/2/3', 2)
        self.utime('files/2/3.meta', 2)
        self.touch('files/2/4/5', ('files/2/4/5.meta', 'n: 3\nx-seqno: 3'))
        self.utime('files/2/4/5', 3)
        self.utime('files/2/4/5.meta', 3)
        self.touch('files/6', ('files/6.meta', 'n: 4\nx-seqno: 4'))
        self.utime('files/6', 4)
        self.utime('files/6.meta', 4)

        self.assertEqual(sorted([
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]])]))
        self.assertEqual(sorted([
            ('1', {'n': '1', 'path': '1', 'x-seqno': '1'}),
            ('2/3', {'n': '2', 'path': '2/3', 'x-seqno': '2'}),
            ('2/4/5', {'n': '3', 'path': '2/4/5', 'x-seqno': '3'}),
            ('6', {'n': '4', 'path': '6', 'x-seqno': '4'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '')]))
        self.assertEqual(sorted([
            ('2/3', {'n': '2', 'path': '2/3', 'x-seqno': '2'}),
            ('2/4/5', {'n': '3', 'path': '2/4/5', 'x-seqno': '3'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '2')]))
        self.assertEqual(sorted([
            ('2/4/5', {'n': '3', 'path': '2/4/5', 'x-seqno': '3'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '2/4')]))
        self.assertEqual(sorted([
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], 'foo')]))
        self.assertEqual(sorted([
            ('1', {'n': '1', 'path': '1', 'x-seqno': '1'}),
            ('6', {'n': '4', 'path': '6', 'x-seqno': '4'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '', False)]))

    def test_diff_FailOnRelativePaths(self):
        blobs = Blobs('.', Seqno())

        self.assertRaises(http.BadRequest, lambda: [i for i in blobs.diff([[1, None]], '..')])
        self.assertRaises(http.BadRequest, lambda: [i for i in blobs.diff([[1, None]], '/..')])
        self.assertRaises(http.BadRequest, lambda: [i for i in blobs.diff([[1, None]], '/../foo')])
        self.assertRaises(http.BadRequest, lambda: [i for i in blobs.diff([[1, None]], 'foo/..')])

    def test_diff_CheckinFiles(self):
        blobs = Blobs('.', Seqno())
        this.request = Request()

        self.touch(
                ('files/1.pdf', '1'),
                ('files/2/3.txt', '22'),
                ('files/2/4/5.svg', '333'),
                ('files/6.png', '4444'),
                )

        self.assertEqual(0, blobs._seqno.value)
        self.assertEqual(sorted([
            ('1.pdf', {'content-type': 'application/pdf', 'content-length': '1', 'x-seqno': '1', 'path': '1.pdf'}),
            ('2/3.txt', {'content-type': 'text/plain', 'content-length': '2', 'x-seqno': '1', 'path': '2/3.txt'}),
            ('2/4/5.svg', {'content-type': 'image/svg+xml', 'content-length': '3', 'x-seqno': '1', 'path': '2/4/5.svg'}),
            ('6.png', {'content-type': 'image/png', 'content-length': '4', 'x-seqno': '1', 'path': '6.png'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '')]))
        self.assertEqual(1, blobs._seqno.value)

        self.assertEqual(sorted([
            ('1.pdf', {'content-type': 'application/pdf', 'content-length': '1', 'x-seqno': '1', 'path': '1.pdf'}),
            ('2/3.txt', {'content-type': 'text/plain', 'content-length': '2', 'x-seqno': '1', 'path': '2/3.txt'}),
            ('2/4/5.svg', {'content-type': 'image/svg+xml', 'content-length': '3', 'x-seqno': '1', 'path': '2/4/5.svg'}),
            ('6.png', {'content-type': 'image/png', 'content-length': '4', 'x-seqno': '1', 'path': '6.png'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '')]))
        self.assertEqual(1, blobs._seqno.value)

    def test_diff_HandleUpdates(self):
        blobs = Blobs('.', Seqno())
        this.request = Request()

        self.touch('blobs/00/0000000000000000000000000000000000000001',
                  ('blobs/00/0000000000000000000000000000000000000001.meta', 'n: 1\nx-seqno: 1'))
        self.utime('blobs/00/0000000000000000000000000000000000000001', 100)
        self.utime('blobs/00/0000000000000000000000000000000000000001.meta', 1)

        self.touch('files/2', ('files/2.meta', 'n: 2\nx-seqno: 2'))
        self.utime('files/2', 200)
        self.utime('files/2.meta', 2)

        blobs._seqno.value = 10
        self.assertEqual([
            ('0000000000000000000000000000000000000001', {'n': '1', 'content-length': '49', 'x-seqno': '11'}),
            ],
            [(i.digest, i.meta) for i in  blobs.diff([[1, None]])])
        self.assertEqual(11, blobs._seqno.value)
        self.assertEqual([
            ('0000000000000000000000000000000000000001', {'n': '1', 'content-length': '49', 'x-seqno': '11'}),
            ],
            [(i.digest, i.meta) for i in  blobs.diff([[1, None]])])
        self.assertEqual(11, blobs._seqno.value)

        self.assertEqual(sorted([
            ('2', {'n': '2', 'path': '2', 'content-length': '7', 'x-seqno': '12'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '')]))
        self.assertEqual(12, blobs._seqno.value)
        self.assertEqual(sorted([
            ('2', {'n': '2', 'path': '2', 'content-length': '7', 'x-seqno': '12'}),
            ]),
            sorted([(i.digest, i.meta) for i in  blobs.diff([[1, None]], '')]))
        self.assertEqual(12, blobs._seqno.value)

    def test_patch_Blob(self):
        blobs = Blobs('.', Seqno())

        self.touch(('blob', '1'))
        blobs.patch(File('./blob', '0000000000000000000000000000000000000001', {'n': 1}), -1)
        blob = blobs.get('0000000000000000000000000000000000000001')
        self.assertEqual(tests.tmpdir + '/blobs/00/0000000000000000000000000000000000000001', blob.path)
        self.assertEqual('0000000000000000000000000000000000000001', blob.digest)
        self.assertEqual('1', file(blob.path).read())
        self.assertEqual({'x-seqno': '-1', 'n': '1'}, blob.meta)
        assert not exists('blob')

        blobs.patch(File('./fake', '0000000000000000000000000000000000000002', {'n': 2, 'content-length': '0'}), -2)
        assert blobs.get('0000000000000000000000000000000000000002') is None

        blobs.patch(File('./fake', '0000000000000000000000000000000000000001', {'n': 3, 'content-length': '0'}), -3)
        blob = blobs.get('0000000000000000000000000000000000000001')
        assert blob.path is None
        self.assertEqual({'x-seqno': '-3', 'n': '1', 'status': '410 Gone'}, blob.meta)

    def test_patch_File(self):
        blobs = Blobs('.', Seqno())

        self.touch(('file', '1'))
        blobs.patch(File('./file', '1', {'n': 1, 'path': 'foo/bar'}), -1)
        blob = blobs.get('foo/bar')
        self.assertEqual('1', file(blob.path).read())
        self.assertEqual({'x-seqno': '-1', 'n': '1'}, blob.meta)
        assert not exists('file')

        blobs.patch(File('./fake', 'bar/foo', {'n': 2, 'content-length': '0'}), -2)
        assert blobs.get('bar/foo') is None

        blobs.patch(File('./fake', 'foo/bar', {'n': 3, 'content-length': '0', 'path': 'foo/bar'}), -3)
        blob = blobs.get('foo/bar')
        assert blob.path is None
        self.assertEqual({'x-seqno': '-3', 'n': '1', 'status': '410 Gone'}, blob.meta)

    def test_walk_Blobs(self):
        blobs = Blobs('.', Seqno())

        blob1 = blobs.post('1')
        blob2 = blobs.post('2')
        blob3 = blobs.post('3')

        self.assertEqual(
                sorted([blob1.digest, blob2.digest, blob3.digest]),
                sorted([i.digest for i in blobs.walk()]))

    def test_post_Thumbs(self):
        blobs = Blobs('1', Seqno())
        coroutine.spawn(blobs.poll_thumbs)
        coroutine.dispatch()

        blob = blobs.post(SVG, 'image/svg+xml', thumbs=100)
        coroutine.sleep(.5)
        self.assertEqual({
            'content-type': 'image/svg+xml',
            'content-length': str(len(SVG)),
            'x-seqno': '1',
            'x-thumbs': '100',
            },
            blob.meta)
        self.assertEqual(
                'PNG image data, 100 x 100, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', '1/thumbs/100/%s/%s' % (blob.digest[:2], blob.digest)]))

        blobs = Blobs('2', Seqno())
        coroutine.spawn(blobs.poll_thumbs)
        coroutine.dispatch()

        blob = blobs.post(SVG, 'image/svg+xml', thumbs=[100, 200, 300])
        coroutine.sleep(.5)
        self.assertEqual({
            'content-type': 'image/svg+xml',
            'content-length': str(len(SVG)),
            'x-seqno': '1',
            'x-thumbs': '100 200 300',
            },
            blob.meta)
        self.assertEqual(
                'PNG image data, 100 x 100, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', '2/thumbs/100/%s/%s' % (blob.digest[:2], blob.digest)]))
        self.assertEqual(
                'PNG image data, 200 x 200, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', '2/thumbs/200/%s/%s' % (blob.digest[:2], blob.digest)]))
        self.assertEqual(
                'PNG image data, 300 x 300, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', '2/thumbs/300/%s/%s' % (blob.digest[:2], blob.digest)]))

    def test_get_Thumbs(self):
        blobs = Blobs('.', Seqno())
        coroutine.spawn(blobs.poll_thumbs)
        coroutine.dispatch()
        digest = blobs.post(SVG, 'image/svg+xml', thumbs=[100, 200, 300]).digest
        coroutine.sleep(.5)

        blob = blobs.get(digest)
        self.assertEqual(
                'SVG Scalable Vector Graphics image',
                toolkit.call(['file', '-b', blob.path]))
        self.assertEqual('image/svg+xml', blob.meta['content-type'])
        assert SVG == file(blob.path).read()

        thumb100 = blobs.get(digest, 100)
        self.assertEqual(
                'PNG image data, 100 x 100, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb100.path]))
        self.assertEqual('image/png', thumb100.meta['content-type'])
        assert SVG != file(thumb100.path).read()

        thumb200 = blobs.get(digest, 200)
        self.assertEqual(
                'PNG image data, 200 x 200, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb200.path]))
        self.assertEqual('image/png', thumb200.meta['content-type'])
        assert SVG != file(thumb200.path).read()
        assert file(thumb100.path).read() != file(thumb200.path).read()

        thumb300 = blobs.get(digest, 300)
        self.assertEqual(
                'PNG image data, 300 x 300, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb300.path]))
        self.assertEqual('image/png', thumb300.meta['content-type'])
        assert SVG != file(thumb300.path).read()
        assert file(thumb100.path).read() != file(thumb300.path).read()
        assert file(thumb200.path).read() != file(thumb300.path).read()

        blob = blobs.get(digest, 400)
        self.assertEqual(
                'SVG Scalable Vector Graphics image',
                toolkit.call(['file', '-b', blob.path]))
        self.assertEqual('image/svg+xml', blob.meta['content-type'])
        assert SVG == file(blob.path).read()

    def test_patch_Thumbs(self):
        blobs = Blobs('.', Seqno())
        coroutine.spawn(blobs.poll_thumbs)
        coroutine.dispatch()

        self.touch(('src', SVG))
        blobs.patch(File('./src', '0000000000000000000000000000000000000001', {
            'x-thumbs': '100 200',
            'content-type': 'image/svg+xml',
            }), -1)
        coroutine.sleep(.5)

        blob = blobs.get('0000000000000000000000000000000000000001')
        self.assertEqual(
                'SVG Scalable Vector Graphics image',
                toolkit.call(['file', '-b', blob.path]))
        self.assertEqual('image/svg+xml', blob.meta['content-type'])
        assert SVG == file(blob.path).read()

        thumb100 = blobs.get('0000000000000000000000000000000000000001', 100)
        self.assertEqual(
                'PNG image data, 100 x 100, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb100.path]))
        self.assertEqual('image/png', thumb100.meta['content-type'])
        assert SVG != file(thumb100.path).read()

        thumb200 = blobs.get('0000000000000000000000000000000000000001', 200)
        self.assertEqual(
                'PNG image data, 200 x 200, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb200.path]))
        self.assertEqual('image/png', thumb200.meta['content-type'])
        assert SVG != file(thumb200.path).read()
        assert file(thumb100.path).read() != file(thumb200.path).read()

    def test_populate_thumbs(self):
        self.touch(
            ('blobs/00/0000000000000000000000000000000000000000', SVG),
            ('blobs/00/0000000000000000000000000000000000000000.meta', ['x-thumbs: 100 200']),
            )

        blobs = Blobs('.', None)
        blobs.populate_thumbs()

        thumb100 = blobs.get('0000000000000000000000000000000000000000', 100)
        self.assertEqual(
                'PNG image data, 100 x 100, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb100.path]))
        thumb200 = blobs.get('0000000000000000000000000000000000000000', 200)
        self.assertEqual(
                'PNG image data, 200 x 200, 1-bit grayscale, non-interlaced',
                toolkit.call(['file', '-b', thumb200.path]))


class Seqno(object):

    def __init__(self):
        self.value = 0

    def next(self):
        self.value += 1
        return self.value

    def commit(self):
        pass


SVG = """\
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<svg xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns="http://www.w3.org/2000/svg" xmlns:cc="http://creativecommons.org/ns#" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:dc="http://purl.org/dc/elements/1.1/" id="svg53383" viewBox="0 0 48 48">
</svg>"""


if __name__ == '__main__':
    tests.main()
