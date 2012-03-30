#!/usr/bin/env python
# sugar-lint: disable

from os.path import join

from __init__ import tests

from sugar_network import client, sugar, cache, http


class ClientTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        self.total = 10
        self.requests = []
        self.responses = []

        def request(method, path, **kwargs):
            params = kwargs.get('params')
            self.requests.append((method, '/' + '/'.join(path), params))
            if self.responses:
                return self.responses.pop(0)
            elif params and 'offset' in params:
                result = [{'document': None, 'guid': i} for i in \
                        range(params['offset'], params['offset'] + \
                        params['limit'])]
                return {'total': self.total, 'result': result}
            else:
                return {}
        self.override(http, 'request', request)
        self.override(http, 'raw_request', request)

        client._PAGE_SIZE = 16
        client._PAGE_NUMBER = 5

    def test_SpecifyReplyProps(self):
        self.responses.append({'p1': 'value'})
        obj = client.Object('resource', {'guid': 'guid'}, reply=['p1', 'p2', 'p3'])
        self.assertEqual('value', obj['p1'])
        self.assertEqual(
                [
                    ('GET', '/resource/guid', {'reply': 'p1,p2,p3'}),
                    ],
                self.requests)

        query = client.Query(reply=['p1', 'p2', 'p3'])
        self.assertEqual(10, query.total)
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 0, 'limit': 16, 'reply': 'p1,p2,p3'}),
                    ],
                self.requests[1:])

    def test_Query_Browse(self):
        client._PAGE_SIZE = 1
        client._PAGE_NUMBER = 3

        query = client.Query()

        self.assertEqual(10, query.total)
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 0, 'limit': 1, 'reply': 'guid'}),
                    ],
                self.requests)
        self.assertEqual(10, query.total)
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 0, 'limit': 1, 'reply': 'guid'}),
                    ],
                self.requests)

        for i in range(10):
            self.assertEqual(i, query[i]['guid'])
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 1, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 2, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 3, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 4, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 5, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 6, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 7, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 8, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 9, 'limit': 1, 'reply': 'guid'}),
                    ],
                self.requests[1:])

        for i in reversed(range(10)):
            self.assertEqual(i, query[i]['guid'])
        self.assertEqual(
                [
                    ('GET', '/', {'offset': 6, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 5, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 4, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 3, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 2, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 1, 'limit': 1, 'reply': 'guid'}),
                    ('GET', '/', {'offset': 0, 'limit': 1, 'reply': 'guid'}),
                    ],
                self.requests[10:])

    def test_Object_Gets(self):
        guid = '00000000-0000-0000-0000-000000000000'
        self.responses.append({'prop': 'value'})

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

        self.responses.append({'guid': 'guid', 'foo': 'bar'})
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
        obj = client.Object('resource', {
            'guid': 'guid',
            'author': [sugar.guid()],
            })
        obj['foo'] = 'bar'
        obj.post()

        self.assertEqual(
                [('PUT', '/resource/guid', None)],
                self.requests)
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

    def test_Object_MemoryCache(self):
        client.Object.memory_cache['resource'] = {'vote': (None, None)}

        self.responses.append({'vote': False})
        self.assertEqual(False, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual(
                [
                    ('GET', '/resource/guid', None),
                    ],
                self.requests)

        obj = client.Object('resource', {'guid': 'guid'})
        obj['vote'] = True
        obj.post()
        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual(
                [
                    ('PUT', '/resource/guid', None),
                    ],
                self.requests[1:])

        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual([], self.requests[2:])

        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual([], self.requests[2:])

    def test_Object_MemoryCache_Typecast(self):
        client.Object.memory_cache['resource'] = {'vote': (None, bool)}
        self.responses.append({'guid': 'guid'})

        obj = client.Object('resource')
        obj['vote'] = 1
        obj.post()
        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual(
                [
                    ('POST', '/resource', None),
                    ],
                self.requests)

        obj = client.Object('resource', {'guid': 'guid'})
        obj['vote'] = 0
        obj.post()
        self.assertEqual(False, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual(
                [
                    ('PUT', '/resource/guid', None),
                    ],
                self.requests[1:])

        obj = client.Object('resource', {'guid': 'guid'})
        obj['vote'] = -1
        obj.post()
        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual(
                [
                    ('PUT', '/resource/guid', None),
                    ],
                self.requests[2:])

    def test_Object_MemoryCache_Defaults(self):
        client.Object.memory_cache['resource'] = {'vote': (True, None)}
        self.responses.append({'guid': 'guid'})

        obj = client.Object('resource')
        obj['foo'] = 'bar'
        obj.post()
        self.assertEqual(
                [
                    ('POST', '/resource', None),
                    ],
                self.requests)

        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual([], self.requests[1:])

        self.assertEqual(True, client.Object('resource', {'guid': 'guid'})['vote'])
        self.assertEqual([], self.requests[1:])


if __name__ == '__main__':
    tests.main()
