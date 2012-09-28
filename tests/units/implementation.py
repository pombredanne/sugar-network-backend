#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.resources.implementation import _encode_version


class ImplementationTest(tests.Test):

    def test_encode_version(self):
        self.assertEqual(
                '00000''00000''00001' '10',
                _encode_version('1'))
        self.assertEqual(
                '00000''00001''00002' '10',
                _encode_version('1.2'))
        self.assertEqual(
                '00001''00020''00300' '10',
                _encode_version('1.20.300'))
        self.assertEqual(
                '00020''00300''04000' '10',
                _encode_version('1.20.300.4000'))

        self.assertEqual(
                '00000''00000''00001' '10' '00002''00003''00004' '10',
                _encode_version('1-2.3.4'))
        self.assertEqual(
                '00000''00000''00001' '10' '00002''00003''00004' '10' '00006''00007''00008' '10',
                _encode_version('1-2.3.4-5.6.7.8'))

        self.assertEqual(
                '00000''00000''00001' '08',
                _encode_version('1-pre'))
        self.assertEqual(
                '00000''00000''00001' '09',
                _encode_version('1-rc'))
        self.assertEqual(
                '00000''00000''00001' '10',
                _encode_version('1-'))
        self.assertEqual(
                '00000''00000''00001' '11',
                _encode_version('1-post'))

        self.assertEqual(
                '00000''00000''00001' '08' '00003''00004''00005' '10',
                _encode_version('1-pre2.3.4.5'))


if __name__ == '__main__':
    tests.main()
