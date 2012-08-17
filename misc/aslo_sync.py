#!/usr/bin/env python

# Copyright (C) 2012 Aleksey Lim
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# sugar-lint: disable

import os
import sys
import json
import time
import getpass
import logging
from cStringIO import StringIO
from os.path import join, exists

import MySQLdb as mdb

import active_document as ad
from sweets_recipe import GOOD_LICENSES, Bundle
from sugar_network.resources.volume import Volume


DOWNLOAD_URL = 'http://download.sugarlabs.org/activities'
ACTIVITIES_PATH = '/upload/activities'
EXCLUDE_BUNDLE_IDS = ['net.gcompris']
SUGAR_GUID = 'sugar'
SN_GUID = 'sugar'

CATEGIORIES_TO_TAGS = {
        'Search & Discovery': 'discovery',
        'Documents': 'documents',
        'Chat, mail and talk': 'conversation',
        'Programming': 'programming',
        'Maps & Geography': 'geography',
        'Media players': 'media',
        'Teacher tools': 'teacher',
        'Games': 'games',
        'Media creation': 'media',
        'Maths & Science': 'science',
        'News': 'news',
        }

MISNAMED_LICENSES = {
        ('artistic', '2.0'): 'Artistic 2.0',
        ('cc-by-sa',): 'CC-BY-SA',
        ('creative', 'share', 'alike'): 'CC-BY-SA',
        }

connection = None
volume = None


def main():
    if not volume['context'].exists(SN_GUID):
        volume['context'].create(guid=SN_GUID, type='project',
                title='Sugar Network', summary='Sugar Network',
                description='Sugar Network', user=['aslo'],
                layer=['public'], ctime=time.time(), mtime=time.time())

    if not volume['context'].exists(SUGAR_GUID):
        volume['context'].create(guid=SUGAR_GUID, type='project',
                title='Sugar', summary='Sugar', description='Sugar',
                user=['aslo'],
                layer=['public'], ctime=time.time(), mtime=time.time())

    for version in ['0.82', '0.84', '0.86', '0.88', '0.90', '0.92',
            '0.94', '0.96']:
        guid = '%s-%s' % (SUGAR_GUID, version)
        if volume['context'].exists(guid):
            continue
        volume['implementation'].create(guid=guid, context=SUGAR_GUID,
                license=['GPLv3+'], version=version, date=0,
                stability='stable', notes='', user=['aslo'],
                layer=['public'], ctime=time.time(), mtime=time.time())

    import_versions()


def import_versions(addon_id=None):
    rows = sqlexec("""
        SELECT
            addons.id,
            addons.status,
            addons.guid,
            licenses.name,
            (select max(localized_string) from translations where
                id=licenses.text),
            versions.created,
            (select max(localized_string) from translations where
                id=versions.releasenotes),
            files.filename,
            (select version from appversions where
                id=applications_versions.min),
            (select version from appversions where
                id=applications_versions.max),
            CONVERT(versions.version, DECIMAL)
        FROM addons
            INNER JOIN versions ON versions.addon_id=addons.id
            LEFT JOIN licenses ON licenses.id=versions.license_id
            INNER JOIN files ON files.version_id=versions.id
            INNER JOIN applications_versions ON
                applications_versions.version_id=versions.id
        WHERE
            (select version from appversions where
                id=applications_versions.min) < 0.96 AND
            addons.status > 0 AND addons.status < 5
            %s
        """ % ('AND addons.id = %s' % addon_id if addon_id else ''))

    grouped_rows = {}
    for row in rows:
        grouped = grouped_rows.get(row[0])
        if grouped is None or grouped[-1] < row[-1]:
            grouped_rows[row[0]] = row

    for row in grouped_rows.values():
        addon_id, status, bundle_id, license_id, alicense, release_date, \
                releasenotes, filename, sugar_min, sugar_max, __ = row
        if [i for i in EXCLUDE_BUNDLE_IDS if i in bundle_id]:
            continue

        if license_id is None:
            pass
        elif license_id == 0:
            alicense = 'MPLv1.1'
        elif license_id == 1:
            alicense = 'GPLv2'
        elif license_id == 2:
            alicense = 'GPLv3'
        elif license_id == 3:
            alicense = 'LGPLv2'
        elif license_id == 4:
            alicense = 'LGPLv3'
        elif license_id == 5:
            alicense = 'MIT'
        elif license_id == 6:
            alicense = 'BSD'
        else:
            parsed_license = parse_license(alicense)
            if not parsed_license:
                print '-- Skip bad license %r from %s' % (alicense, filename)
                continue
            alicense = parsed_license

        if not volume['context'].exists(bundle_id):
            context_new(addon_id, bundle_id)

        release_from_aslo(bundle_id,
                'http://download.sugarlabs.org/activities/%s/%s' % \
                        (addon_id, filename),
                sugar_min, sugar_max,
                stability='stable' if status == 4 else 'developer',
                date=int(time.mktime(release_date.timetuple())),
                notes=releasenotes or 'Mirror activity from ASLO',
                license=[alicense] if alicense else [],
                )


def context_new(addon_id, bundle_id):
    title, summary, description, homepage, nick, name, icondata = sqlexec("""
        SELECT
            (select max(localized_string) from translations where
                id=addons.name),
            (select max(localized_string) from translations where
                id=addons.summary),
            (select max(localized_string) from translations where
                id=addons.description),
            (select max(localized_string) from translations where
                id=addons.homepage),
            users.nickname,
            CONCAT_WS(' ', users.firstname, users.lastname),
            icondata
        FROM
            addons
            INNER JOIN addons_users on addons_users.addon_id=addons.id
            INNER JOIN users on users.id=addons_users.user_id
        WHERE addons.id=%s
        """ % addon_id)[0]

    tags = set()
    for row in sqlexec("""
            SELECT
                (select localized_string from translations where
                    id=categories.name AND locale='en-US')
            FROM addons_categories
                INNER JOIN categories ON
                    categories.id=addons_categories.category_id
            WHERE
                addons_categories.addon_id=%s
            """ % addon_id):
        tags.add(CATEGIORIES_TO_TAGS[row[0]])
    for row in sqlexec("""
            SELECT
                tags.tag_text
            FROM users_tags_addons
                INNER JOIN tags ON tags.id=users_tags_addons.tag_id
                INNER JOIN addons_users ON
                    addons_users.addon_id=users_tags_addons.addon_id
            WHERE
                users_tags_addons.addon_id=%s
            """ % addon_id):
        tags.add(row[0])

    authors = []
    if nick:
        authors.append(nick)
    if name:
        authors.append(name)

    volume['context'].create(guid=bundle_id, type='activity',
            implement=bundle_id, title=title, summary=summary or title,
            description=description or title, homepage=homepage or '',
            tags=list(tags), user= ['aslo'], author=authors,
            layer=['public'], ctime=time.time(), mtime=time.time())

    if icondata:
        volume['context'].set_blob(bundle_id, 'icon', StringIO(icondata))

    for row in sqlexec("""
            SELECT
                thumbdata
            FROM previews
            WHERE
                addon_id=%s
            """ % addon_id):
        thumb, = row
        if thumb:
            volume['context'].set_blob(bundle_id, 'preview', StringIO(thumb))
            break


def release_from_aslo(context_guid, url, sugar_min, sugar_max, stability,
        **kwargs):
    path = url[len(DOWNLOAD_URL):].strip('/').split('/')
    path = join(ACTIVITIES_PATH, *path)
    if not exists(path):
        print '-- Cannot find ASLO bundle at %s' % path
        return

    try:
        bundle = Bundle(path)
    except Exception, error:
        print '-- Cannor read %s bundle: %s' % (path, error)
        return

    spec = bundle.get_spec()
    if spec is None:
        print '-- Bundle %s does not contain spec' % path
        return

    if not kwargs['license']:
        kwargs['license'] = parse_license(spec['license'])
        if not kwargs['license']:
            print '-- Skip bad license %r from %s' % (spec['license'], path)
            return

    # TODO
    #spec.lint()

    feed_info = volume['context'].get(context_guid).meta('feed')
    if not feed_info:
        feed = {}
    else:
        with file(feed_info['path']) as f:
            feed = json.load(f)

    kwargs['context'] = context_guid
    kwargs['version'] = spec['version']
    kwargs['stability'] = stability
    kwargs['user'] = ['aslo']
    impl_guid = volume['implementation'].create(kwargs)
    volume['implementation'].set_blob(impl_guid, 'data', url)

    if not feed or spec['version'] >= max(feed.keys()):
        volume['context'].update(context_guid, {
            'type': spec.types,
            'title': spec['name'],
            'summary': spec['summary'],
            'description': spec['description'],
            'homepage': spec['homepage'] or '',
            'tags': spec['tags'] or [],
            'mime_types': spec['mime_types'] or [],
            })

        icon_path = join(bundle.extract, spec['icon'])
        try:
            volume['context'].set_blob(context_guid, 'artifact_icon',
                    bundle.extractfile(icon_path))
        except Exception, error:
            print '-- No icon for %s: %s' % (path, icon_path)

    sugar_dep = {
            'restrictions': [
                (sugar_min, None),
                (None, '0.%s' % (int(sugar_max.split('.')[-1]) + 1)),
                ],
            }
    #spec.requires[SUGAR_GUID] = sugar_dep

    feed.setdefault(spec['version'], {})
    feed[spec['version']]['*-*'] = {
            'guid': impl_guid,
            'stability': stability,
            'commands': spec.commands,
            'requires': spec.requires,
            'extract': bundle.extract,
            'size': os.stat(path).st_size,
            }

    volume['context'].set_blob(context_guid, 'feed',
            StringIO(json.dumps(feed, indent=4)))


def parse_license(alicense):
    for good in GOOD_LICENSES:
        if not alicense or good in ['ec']:
            continue
        if good in alicense:
            alicense = good
            break
    else:
        for words, good in MISNAMED_LICENSES.items():
            for i in words:
                if i not in alicense.lower():
                    break
            else:
                alicense = good
                break
        else:
            return None

    return alicense


def sqlexec(text):
    cur = connection.cursor()
    cur.execute(text)
    return cur.fetchall()


logging.basicConfig(level=logging.INFO)

ad.index_write_queue.value = 1024 * 10
ad.index_flush_threshold.value = 0
ad.index_flush_timeout.value = 0

connection = mdb.connect('localhost',
        'root', getpass.getpass(), 'activities')
volume = Volume('db')

try:
    if len(sys.argv) > 1:
        for addon_id in sys.argv[1:]:
            import_versions(addon_id)
    else:
        main()
finally:
    volume.close()
