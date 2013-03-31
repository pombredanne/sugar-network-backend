#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit import lsb_release


class LSBReleaseTest(tests.Test):

    def test_Trisquel(self):
        func = lsb_release._DERIVATES['Trisquel'][1][0]
        self.assertEqual('10.04', func('4.1'))
        self.assertEqual('10.10', func('4.5'))
        self.assertEqual('11.04', func('5.0'))

    def test_LinuxMint(self):
        func = lsb_release._DERIVATES['LinuxMint'][1][0]
        self.assertEqual('10.04', func('9'))
        self.assertEqual('10.10', func('10'))
        self.assertEqual('11.04', func('11'))

    def test_Tuquito(self):
        func = lsb_release._DERIVATES['Tuquito'][1][0]
        self.assertEqual('10.10', func('4.1'))
        self.assertEqual('11.04', func('5'))
        self.assertEqual('12.04', func('6'))



if __name__ == '__main__':
    tests.main()
