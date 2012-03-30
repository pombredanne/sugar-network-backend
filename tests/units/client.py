#!/usr/bin/env python
# sugar-lint: disable

from os.path import join

from __init__ import tests

from sugar_network import client, sugar, cache, http


class ClientTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.requests = []
        self.responses = []

        def request(method, path, data=None, headers=None, **kwargs):
            params = kwargs.get('params')
            self.requests.append((method, '/' + '/'.join(path), params))

            if params is None:
                if self.responses:
                    return self.responses.pop(0)
                else:
                    return {'guid': path[-1],
                            'prop': 'value',
                            }

            result = [{'document': None, 'guid': i} for i in \
                    range(params['offset'], params['offset'] + params['limit'])]
            return {'total': 10, 'result': result}

        self.override(http, 'request', request)
        self.override(http, 'raw_request', request)

    def test_Query_Browse(self):
        client._PAGE_SIZE = 1
        client._PAGE_NUMBER = 3

        query = client.Query()

        self.assertEqual(10, query.total)
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 0, 'limit': 1}),
                    ],
                self.requests)
        self.assertEqual(10, query.total)
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 0, 'limit': 1}),
                    ],
                self.requests)

        for i in range(10):
            self.assertEqual(i, query[i]['guid'])
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 1, 'limit': 1}),
                    ('GET', '/', {'offset': 2, 'limit': 1}),
                    ('GET', '/', {'offset': 3, 'limit': 1}),
                    ('GET', '/', {'offset': 4, 'limit': 1}),
                    ('GET', '/', {'offset': 5, 'limit': 1}),
                    ('GET', '/', {'offset': 6, 'limit': 1}),
                    ('GET', '/', {'offset': 7, 'limit': 1}),
                    ('GET', '/', {'offset': 8, 'limit': 1}),
                    ('GET', '/', {'offset': 9, 'limit': 1}),
                    ],
                self.requests[1:])

        for i in reversed(range(10)):
            self.assertEqual(i, query[i]['guid'])
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 6, 'limit': 1}),
                    ('GET', '/', {'offset': 5, 'limit': 1}),
                    ('GET', '/', {'offset': 4, 'limit': 1}),
                    ('GET', '/', {'offset': 3, 'limit': 1}),
                    ('GET', '/', {'offset': 2, 'limit': 1}),
                    ('GET', '/', {'offset': 1, 'limit': 1}),
                    ('GET', '/', {'offset': 0, 'limit': 1}),
                    ],
                self.requests[10:])

    def test_Object_Gets(self):
        guid = '00000000-0000-0000-0000-000000000000'
        obj = client.Object('resource', {'guid': guid})
        self.assertEqual([], self.requests)

        self.assertEqual(guid, obj['guid'])
        self.assertEqual([], self.requests)

        self.assertEqual('value', obj['prop'])
        self.assertEqual([('GET', '/resource/' + guid, None)], self.requests)
        self.assertEqual('value', obj['prop'])
        self.assertEqual([('GET', '/resource/' + guid, None)], self.requests)

        self.assertRaises(KeyError, lambda: obj['foo'])
        self.assertEqual([('GET', '/resource/' + guid, None)], self.requests)

        guid = '00000000-0000-0000-0000-000000000001'
        obj = client.Object('resource', {'guid': guid})
        self.assertRaises(KeyError, lambda: obj['foo'])
        self.assertEqual([('GET', '/resource/' + guid, None)], self.requests[1:])
        self.assertRaises(KeyError, lambda: obj['foo'])
        self.assertEqual([('GET', '/resource/' + guid, None)], self.requests[1:])

    def test_Object_Sets(self):
        obj = client.Object('resource')
        self.assertEqual([], self.requests)

        self.assertRaises(KeyError, lambda: obj['foo'])
        self.assertEqual([], self.requests)
        self.assertRaises(KeyError, lambda: obj['guid'])
        self.assertEqual([], self.requests)

        self.assertRaises(RuntimeError, obj.__setitem__, 'guid', 'foo')

        obj['foo'] = 'bar'
        self.assertEqual('bar', obj['foo'])
        self.assertEqual([], self.requests)

        self.responses.append({'guid': 'guid'})
        obj.post()
        self.assertEqual([('POST', '/resource', None)], self.requests)
        obj.post()
        self.assertEqual([('POST', '/resource', None)], self.requests)
        self.assertEqual('bar', obj['foo'])
        self.assertEqual('guid', obj['guid'])
        self.assertEqual([('POST', '/resource', None)], self.requests)

        self.assertRaises(KeyError, lambda: obj['fail'])
        self.assertEqual([('GET', '/resource/guid', None)], self.requests[1:])
        self.assertRaises(KeyError, lambda: obj['fail'])
        self.assertEqual([('GET', '/resource/guid', None)], self.requests[1:])

        obj['foo'] = 'new'
        obj.post()
        self.assertEqual([('PUT', '/resource/guid', None)], self.requests[2:])
        self.assertEqual('new', obj['foo'])
        obj.post()
        self.assertEqual([], self.requests[3:])

    def test_Object_CheckAuthorOnPost(self):
        obj = client.Object('resource')
        obj['foo'] = 'bar'
        obj.post()
        self.assertEqual(1, len(self.requests))
        self.assertEqual([sugar.guid()], obj['author'])

        obj = client.Object('resource')
        obj['author'] = ['fake']
        self.assertRaises(RuntimeError, obj.post)
        self.assertEqual(1, len(self.requests))

    def test_Object_DoNotOverrideSetsAfterPost(self):
        obj = client.Object('resource')
        obj['foo'] = 'bar'
        self.responses.append({'guid': '1'})
        obj.post()

        self.assertEqual(
                [
                    ('POST', '/resource', None),
                    ],
                self.requests)

        self.responses.append({'foo': 'fail', 'probe': 'probe'})
        self.assertEqual('probe', obj['probe'])
        self.assertEqual('bar', obj['foo'])

        self.assertEqual(
                [
                    ('POST', '/resource', None),
                    ('GET', '/resource/1', None),
                    ],
                self.requests)

    def test_delete(self):
        client.delete('resource', 'guid')
        self.assertEqual([('DELETE', '/resource/guid', None)], self.requests)


if __name__ == '__main__':
    tests.main()
