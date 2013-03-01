#!/usr/bin/env python
# sugar-lint: disable

import json
from os.path import exists

from __init__ import tests

from sugar_network.node import downloads
from sugar_network.toolkit import coroutine


class DownloadsTest(tests.Test):

    def test_populate(self):
        self.touch(('1', '1'))
        self.touch(('2', '2'))
        self.touch(('2.tag', json.dumps((2, {'file': 2}))))
        self.touch(('3.tag', json.dumps((3, {'file': 3}))))

        pool = downloads.Pool('.')
        self.assertEqual(None, pool.get(1))
        self.assertEqual(2, pool.get(2).tag['file'])
        self.assertEqual('2', pool.get(2).open().read())
        self.assertEqual(None, pool.get(3))

    def test_ComplexKeys(self):
        key = {-1: None}

        self.touch(('file', 'file'))
        self.touch(('file.tag', json.dumps((key, {'file': 2}))))

        pool = downloads.Pool('.')
        self.assertEqual('file', pool.get(key).open().read())
        key['foo'] = 'bar'
        pool.set(key, None, lambda path: file(path, 'w').close())
        self.assertNotEqual(None, pool.get(key))

    def test_set_Tags(self):
        tag = []

        def fetch(path):
            with file(path, 'w') as f:
                f.write('payload')
            tag.append(True)

        pool = downloads.Pool('.')
        dl = pool.set('key', tag, fetch)
        self.assertEqual(False, dl.ready)
        self.assertEqual(None, dl.open())
        self.assertEqual([], tag)

        coroutine.dispatch()
        self.assertEqual(True, dl.ready)
        self.assertEqual('payload', dl.open().read())
        self.assertEqual([True], tag)

        pool2 = downloads.Pool('.')
        dl2 = pool2.get('key')
        self.assertEqual(True, dl2.ready)
        self.assertEqual('payload', dl2.open().read())
        self.assertEqual([True], tag)

    def test_Eject(self):
        downloads._POOL_SIZE = 3
        pool = downloads.Pool('.')

        pool.set(1, None, lambda path: file(path, 'w').close())
        coroutine.dispatch()
        file1 = pool.get(1).open().name
        pool.set(2, None, lambda path: file(path, 'w').close())
        coroutine.dispatch()
        file2 = pool.get(2).open().name
        pool.set(3, None, lambda path: file(path, 'w').close())
        coroutine.dispatch()
        file3 = pool.get(3).open().name

        assert pool.get(1) is not None
        assert exists(file1)
        assert exists(file1 + '.tag')
        assert pool.get(2) is not None
        assert exists(file2)
        assert exists(file2 + '.tag')
        assert pool.get(3) is not None
        assert exists(file3)
        assert exists(file3 + '.tag')

        pool.set(4, None, lambda path: file(path, 'w').close())
        pool.set(5, None, lambda path: file(path, 'w').close())
        pool.set(6, None, lambda path: file(path, 'w').close())

        assert pool.get(1) is None
        assert not exists(file1)
        assert not exists(file1 + '.tag')
        assert pool.get(2) is None
        assert not exists(file2)
        assert not exists(file2 + '.tag')
        assert pool.get(3) is None
        assert not exists(file3)
        assert not exists(file3 + '.tag')

    def test_remove(self):
        pool = downloads.Pool('.')

        pool.set(1, None, lambda path: file(path, 'w').close())
        coroutine.dispatch()
        file1 = pool.get(1).open().name
        assert pool.get(1) is not None
        assert exists(file1)
        assert exists(file1 + '.tag')

        pool.remove(1)
        assert pool.get(1) is None
        assert not exists(file1)
        assert not exists(file1 + '.tag')


if __name__ == '__main__':
    tests.main()
