#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from restful_document.http import Request, _parse_accept_language


class HttpTest(tests.Test):

    def test_Request_MultipleQueryArguments(self):
        request = Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'a1=v1&a2=v2&a1=v3&a3=v4&a1=v5&a3=v6',
            })
        self.assertEqual(
                {'a1': ['v1', 'v3', 'v5'], 'a2': 'v2', 'a3': ['v4', 'v6'], 'method': 'GET'},
                request)

    def test_parse_accept_language(self):
        self.assertEqual(
                ['ru', 'en', 'es'],
                _parse_accept_language('  ru , en   ,  es'))
        self.assertEqual(
                ['ru', 'en', 'es'],
                _parse_accept_language('  en;q=.4 , ru, es;q=0.1'))
        self.assertEqual(
                ['ru', 'en', 'es'],
                _parse_accept_language('ru;q=1,en;q=1,es;q=0.5'))


if __name__ == '__main__':
    tests.main()
