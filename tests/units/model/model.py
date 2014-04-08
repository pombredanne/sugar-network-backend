#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import base64
import mimetypes

from __init__ import tests

from sugar_network import db
from sugar_network.model import load_bundle
from sugar_network.model.post import Post
from sugar_network.model.context import Context
from sugar_network.node.model import User
from sugar_network.node.auth import Principal as _Principal
from sugar_network.client import IPCConnection, Connection, keyfile
from sugar_network.client.auth import SugarCreds
from sugar_network.toolkit.router import Request
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import i18n, http, coroutine, enforce


class ModelTest(tests.Test):

    def test_RatingSort(self):
        this.localcast = lambda event: None
        directory = db.Volume('db', [Post])['post']

        directory.create({'guid': '1', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [0, 0]})
        directory.create({'guid': '2', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [1, 2]})
        directory.create({'guid': '3', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [1, 4]})
        directory.create({'guid': '4', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [10, 10]})
        directory.create({'guid': '5', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [30, 90]})

        self.assertEqual(
                ['1', '2', '3', '4', '5'],
                [i.guid for i in directory.find()[0]])
        self.assertEqual(
                ['1', '4', '2', '5', '3'],
                [i.guid for i in directory.find(order_by='rating')[0]])
        self.assertEqual(
                ['3', '5', '2', '4', '1'],
                [i.guid for i in directory.find(order_by='-rating')[0]])

    def test_load_bundle_Activity(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])
        changelog = "LOG"
        bundle = self.zips(
                ('topdir/activity/activity.info', activity_info),
                ('topdir/CHANGELOG', changelog),
                )
        blob = blobs.post(bundle)

        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob, bundle_id)

        self.assertEqual({
            'content-type': 'application/vnd.olpc-sugar',
            'content-disposition': 'attachment; filename="Activity-1%s"' % (mimetypes.guess_extension('application/vnd.olpc-sugar') or ''),
            'content-length': str(len(bundle)),
            'x-seqno': '6',
            }, blobs.get(blob.digest).meta)
        self.assertEqual(bundle_id, context)
        self.assertEqual([[1], 0], release['version'])
        self.assertEqual('developer', release['stability'])
        self.assertEqual(['Public Domain'], release['license'])
        self.assertEqual('developer', release['stability'])
        self.assertEqual({
            'dep': [],
            'sugar': [([1, 0], [[0, 88], 0])],
            },
            release['requires'])
        self.assertEqual({
            '*-*': {
                'blob': blob.digest,
                'unpack_size': len(activity_info) + len(changelog),
                },
            },
            release['bundles'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Activity 1 release',
            'es': 'Activity 1 release',
            'fr': 'Activity 1 release',
            }, post['title'])
        self.assertEqual({
            'en-us': 'LOG',
            }, post['message'])

    def test_load_bundle_NonActivity(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'book',
            'title': 'NonActivity',
            'summary': 'summary',
            'description': 'description',
            })
        bundle = 'non-activity'
        blob = blobs.post(bundle)
        blob.meta['content-type'] = 'application/pdf'

        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='2', license='GPL')
        context, release = load_bundle(blob, bundle_id)

        self.assertEqual({
            'content-type': 'application/pdf',
            'content-disposition': 'attachment; filename="NonActivity-2.pdf"',
            'content-length': str(len(bundle)),
            'x-seqno': '6',
            }, blobs.get(blob.digest).meta)
        self.assertEqual(bundle_id, context)
        self.assertEqual([[2], 0], release['version'])
        self.assertEqual(['GPL'], release['license'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'NonActivity 2 release',
            'es': 'NonActivity 2 release',
            'fr': 'NonActivity 2 release',
            }, post['title'])
        self.assertEqual({
            'en-us': '',
            }, post['message'])

    def test_load_bundle_ReuseActivityLicense(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        activity_info_wo_license = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            ])
        bundle = self.zips(('topdir/activity/activity.info', activity_info_wo_license))
        blob_wo_license = blobs.post(bundle)
        self.assertRaises(http.BadRequest, load_bundle, blob_wo_license, bundle_id)

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob_wo_license, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob_wo_license, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            'newest': {'value': {'release': 3, 'license': ['Newest']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob_wo_license, bundle_id)
        self.assertEqual(['Newest'], release['license'])

    def test_load_bundle_ReuseNonActivityLicense(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'book',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        blob = blobs.post('non-activity')
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        self.assertRaises(http.BadRequest, load_bundle, blob, bundle_id)

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        context, release = load_bundle(blob, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        context, release = load_bundle(blob, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            'newest': {'value': {'release': 3, 'license': ['Newest']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        context, release = load_bundle(blob, bundle_id)
        self.assertEqual(['Newest'], release['license'])

    def test_load_bundle_WrontContextType(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'group',
            'title': 'NonActivity',
            'summary': 'summary',
            'description': 'description',
            })

        blob = blobs.post('non-activity')
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='2', license='GPL')
        self.assertRaises(http.BadRequest, load_bundle, blob, bundle_id)

        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])
        changelog = "LOG"
        bundle = self.zips(
                ('topdir/activity/activity.info', activity_info),
                ('topdir/CHANGELOG', changelog),
                )
        blob = blobs.post(bundle)
        self.assertRaises(http.BadRequest, load_bundle, blob, bundle_id)

    def test_load_bundle_MissedContext(self):
        volume = self.start_master()
        blobs = volume.blobs
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])))
        blob = blobs.post(bundle)

        this.principal = Principal(tests.UID)
        this.request = Request()
        self.assertRaises(http.NotFound, load_bundle, blob, initial=False)

    def test_load_bundle_CreateContext(self):
        volume = self.start_master()
        blobs = volume.blobs
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle = self.zips(
                ('ImageViewer.activity/activity/activity.info', '\n'.join([
                    '[Activity]',
                    'bundle_id = org.laptop.ImageViewerActivity',
                    'name      = Image Viewer',
                    'summary   = The Image Viewer activity is a simple and fast image viewer tool',
                    'description = It has features one would expect of a standard image viewer, like zoom, rotate, etc.',
                    'homepage  = http://wiki.sugarlabs.org/go/Activities/Image_Viewer',
                    'activity_version = 1',
                    'license   = GPLv2+',
                    'icon      = activity-imageviewer',
                    'exec      = true',
                    'mime_types = image/bmp;image/gif',
                    ])),
                ('ImageViewer.activity/activity/activity-imageviewer.svg', ''),
                )
        blob = blobs.post(bundle)

        this.principal = Principal(tests.UID)
        this.request = Request()
        context, release = load_bundle(blob, initial=True)
        self.assertEqual('org.laptop.ImageViewerActivity', context)

        context = volume['context'].get('org.laptop.ImageViewerActivity')
        self.assertEqual({'en': 'Image Viewer'}, context['title'])
        self.assertEqual({'en': 'The Image Viewer activity is a simple and fast image viewer tool'}, context['summary'])
        self.assertEqual({'en': 'It has features one would expect of a standard image viewer, like zoom, rotate, etc.'}, context['description'])
        self.assertEqual('http://wiki.sugarlabs.org/go/Activities/Image_Viewer', context['homepage'])
        self.assertEqual(['image/bmp', 'image/gif'], context['mime_types'])
        assert context['ctime'] > 0
        assert context['mtime'] > 0
        self.assertEqual({tests.UID: {'role': 3, 'name': 'user', 'order': 0}}, context['author'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Image Viewer 1 release',
            'es': 'Image Viewer 1 release',
            'fr': 'Image Viewer 1 release',
            }, post['title'])

    def test_load_bundle_UpdateContext(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))
        self.touch(('master/etc/authorization.conf', [
            '[permissions]',
            '%s = admin' % tests.UID,
            ]))

        conn.post(['context'], {
            'guid': 'org.laptop.ImageViewerActivity',
            'type': 'activity',
            'title': {'en': ''},
            'summary': {'en': ''},
            'description': {'en': ''},
            })
        svg = '\n'.join([
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd" [',
            '  <!ENTITY fill_color "#123456">',
            '  <!ENTITY stroke_color "#123456">',
            ']>',
            '<svg xmlns="http://www.w3.org/2000/svg" width="50" height="50">',
            '    <rect x="3" y="7" width="44" height="36" style="fill:&fill_color;;stroke:&stroke_color;;stroke-width:3"/>',
            '    <polyline points="15,7 25,1 35,7" style="fill:none;;stroke:&stroke_color;;stroke-width:1.25"/>',
            '    <circle cx="14" cy="19" r="4.5" style="fill:&stroke_color;;stroke:&stroke_color;;stroke-width:1.5"/>',
            '    <polyline points="3,36 16,32 26,35" style="fill:none;;stroke:&stroke_color;;stroke-width:2.5"/>',
            '    <polyline points="15,43 37,28 47,34 47,43" style="fill:&stroke_color;;stroke:&stroke_color;;stroke-width:3"/>',
            '    <polyline points="22,41.5 35,30 27,41.5" style="fill:&fill_color;;stroke:none;;stroke-width:0"/>',
            '    <polyline points="26,23 28,25 30,23" style="fill:none;;stroke:&stroke_color;;stroke-width:.9"/>',
            '    <polyline points="31.2,20 33.5,17.7 35.8,20" style="fill:none;;stroke:&stroke_color;;stroke-width:1"/>',
            '    <polyline points="36,13 38.5,15.5 41,13" style="fill:none;;stroke:&stroke_color;;stroke-width:1"/>',
            '</svg>',
            ])
        bundle = self.zips(
                ('ImageViewer.activity/activity/activity.info', '\n'.join([
                    '[Activity]',
                    'bundle_id = org.laptop.ImageViewerActivity',
                    'name      = Image Viewer',
                    'summary   = The Image Viewer activity is a simple and fast image viewer tool',
                    'description = It has features one would expect of a standard image viewer, like zoom, rotate, etc.',
                    'homepage  = http://wiki.sugarlabs.org/go/Activities/Image_Viewer',
                    'activity_version = 22',
                    'license   = GPLv2+',
                    'icon      = activity-imageviewer',
                    'exec      = true',
                    'mime_types = image/bmp;image/gif',
                    ])),
                ('ImageViewer.activity/locale/ru/LC_MESSAGES/org.laptop.ImageViewerActivity.mo',
                    base64.b64decode('3hIElQAAAAAMAAAAHAAAAHwAAAARAAAA3AAAAAAAAAAgAQAADwAAACEBAAAOAAAAMQEAAA0AAABAAQAACgAAAE4BAAAMAAAAWQEAAA0AAABmAQAAJwAAAHQBAAAUAAAAnAEAABAAAACxAQAABwAAAMIBAAAIAAAAygEAANEBAADTAQAAIQAAAKUDAAATAAAAxwMAABwAAADbAwAAFwAAAPgDAAAhAAAAEAQAAB0AAAAyBAAAQAAAAFAEAAA9AAAAkQQAADUAAADPBAAAFAAAAAUFAAAQAAAAGgUAAAEAAAACAAAABwAAAAAAAAADAAAAAAAAAAwAAAAJAAAAAAAAAAoAAAAEAAAAAAAAAAAAAAALAAAABgAAAAgAAAAFAAAAAENob29zZSBkb2N1bWVudABEb3dubG9hZGluZy4uLgBGaXQgdG8gd2luZG93AEZ1bGxzY3JlZW4ASW1hZ2UgVmlld2VyAE9yaWdpbmFsIHNpemUAUmV0cmlldmluZyBzaGFyZWQgaW1hZ2UsIHBsZWFzZSB3YWl0Li4uAFJvdGF0ZSBhbnRpY2xvY2t3aXNlAFJvdGF0ZSBjbG9ja3dpc2UAWm9vbSBpbgBab29tIG91dABQcm9qZWN0LUlkLVZlcnNpb246IFBBQ0tBR0UgVkVSU0lPTgpSZXBvcnQtTXNnaWQtQnVncy1UbzogClBPVC1DcmVhdGlvbi1EYXRlOiAyMDEyLTA5LTI3IDE0OjU3LTA0MDAKUE8tUmV2aXNpb24tRGF0ZTogMjAxMC0wOS0yMiAxMzo1MCswMjAwCkxhc3QtVHJhbnNsYXRvcjoga3JvbTlyYSA8a3JvbTlyYUBnbWFpbC5jb20+Ckxhbmd1YWdlLVRlYW06IExBTkdVQUdFIDxMTEBsaS5vcmc+Ckxhbmd1YWdlOiAKTUlNRS1WZXJzaW9uOiAxLjAKQ29udGVudC1UeXBlOiB0ZXh0L3BsYWluOyBjaGFyc2V0PVVURi04CkNvbnRlbnQtVHJhbnNmZXItRW5jb2Rpbmc6IDhiaXQKUGx1cmFsLUZvcm1zOiBucGx1cmFscz0zOyBwbHVyYWw9KG4lMTA9PTEgJiYgbiUxMDAhPTExID8gMCA6IG4lMTA+PTIgJiYgbiUxMDw9NCAmJiAobiUxMDA8MTAgfHwgbiUxMDA+PTIwKSA/IDEgOiAyKTsKWC1HZW5lcmF0b3I6IFBvb3RsZSAyLjAuMwoA0JLRi9Cx0LXRgNC40YLQtSDQtNC+0LrRg9C80LXQvdGCANCX0LDQs9GA0YPQt9C60LAuLi4A0KPQvNC10YHRgtC40YLRjCDQsiDQvtC60L3QtQDQn9C+0LvQvdGL0Lkg0Y3QutGA0LDQvQDQn9GA0L7RgdC80L7RgtGAINC60LDRgNGC0LjQvdC+0LoA0JjRgdGC0LjQvdC90YvQuSDRgNCw0LfQvNC10YAA0J/QvtC70YPRh9C10L3QuNC1INC40LfQvtCx0YDQsNC20LXQvdC40LksINC/0L7QtNC+0LbQtNC40YLQtS4uLgDQn9C+0LLQtdGA0L3Rg9GC0Ywg0L/RgNC+0YLQuNCyINGH0LDRgdC+0LLQvtC5INGB0YLRgNC10LvQutC4ANCf0L7QstC10YDQvdGD0YLRjCDQv9C+INGH0LDRgdC+0LLQvtC5INGB0YLRgNC10LvQutC1ANCf0YDQuNCx0LvQuNC30LjRgtGMANCe0YLQtNCw0LvQuNGC0YwA')),
                ('ImageViewer.activity/activity/activity-imageviewer.svg', svg),
                )

        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', 'org.laptop.ImageViewerActivity'])
        context, release = load_bundle(blob, initial=True)

        context = volume['context'].get('org.laptop.ImageViewerActivity')
        self.assertEqual({
            'en': 'Image Viewer',
            'ru': u'Просмотр картинок',
            },
            context['title'])
        self.assertEqual({
            'en': 'The Image Viewer activity is a simple and fast image viewer tool',
            },
            context['summary'])
        self.assertEqual({
            'en': 'It has features one would expect of a standard image viewer, like zoom, rotate, etc.',
            },
            context['description'])
        self.assertEqual(svg, file(blobs.get(context['artefact_icon']).path).read())
        assert context['icon'] != 'missing.png'
        assert context['logo'] != 'missing-logo.png'
        self.assertEqual('http://wiki.sugarlabs.org/go/Activities/Image_Viewer', context['homepage'])
        self.assertEqual(['image/bmp', 'image/gif'], context['mime_types'])

    def test_load_bundle_3rdPartyRelease(self):
        i18n._default_langs = ['en']
        volume = self.start_master()
        blobs = volume.blobs
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity2',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            ])))
        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID2)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob, bundle_id)

        assert tests.UID in volume['context'][bundle_id]['author']
        assert tests.UID2 not in volume['context'][bundle_id]['author']
        self.assertEqual({'en': 'Activity'}, volume['context'][bundle_id]['title'])

        post = volume['post'][release['announce']]
        assert tests.UID not in post['author']
        assert tests.UID2 in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Activity 1 third-party release',
            'es': 'Activity 1 third-party release',
            'fr': 'Activity 1 third-party release',
            }, post['title'])

        blobs.delete(blob.digest)
        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob, bundle_id)

        assert tests.UID in volume['context'][bundle_id]['author']
        assert tests.UID2 not in volume['context'][bundle_id]['author']
        self.assertEqual({'en': 'Activity2'}, volume['context'][bundle_id]['title'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        assert tests.UID2 not in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Activity2 1 release',
            'es': 'Activity2 1 release',
            'fr': 'Activity2 1 release',
            }, post['title'])

    def test_load_bundle_PopulateRequires(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection(creds=SugarCreds(keyfile.value))

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        bundle = self.zips(
                ('ImageViewer.activity/activity/activity.info', '\n'.join([
                    '[Activity]',
                    'bundle_id = %s' % bundle_id,
                    'name      = Image Viewer',
                    'activity_version = 22',
                    'license   = GPLv2+',
                    'icon      = activity-imageviewer',
                    'exec      = true',
                    'requires  = dep1, dep2<10, dep3<=20, dep4>30, dep5>=40, dep6>5<7, dep7>=1<=3',
                    ])),
                ('ImageViewer.activity/activity/activity-imageviewer.svg', ''),
                )
        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = load_bundle(blob, bundle_id)

        self.assertEqual({
            'dep5': [([1, 0], [[40], 0])],
            'dep4': [([1], [[30], 0])],
            'dep7': [([1, 0], [[1], 0]), ([-1, 0], [[3], 0])],
            'dep6': [([1], [[5], 0]), ([-1], [[7], 0])],
            'dep1': [],
            'dep3': [([-1, 0], [[20], 0])],
            'dep2': [([-1], [[10], 0])],
            },
            release['requires'])

    def test_load_bundle_IgnoreNotSupportedContextTypes(self):
        volume = self.start_master([User, Context])
        conn = Connection(creds=SugarCreds(keyfile.value))

        context = conn.post(['context'], {
            'type': 'package',
            'title': '',
            'summary': '',
            'description': '',
            })
        this.request = Request(method='POST', path=['context', context])
        aggid = conn.post(['context', context, 'releases'], {})
        self.assertEqual({
            aggid: {'seqno': 3, 'value': {}, 'author': {tests.UID: {'role': 3, 'name': 'test', 'order': 0}}},
            }, volume['context'][context]['releases'])


class Principal(_Principal):

    admin = True


if __name__ == '__main__':
    tests.main()
