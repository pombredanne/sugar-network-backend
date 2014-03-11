# Copyright (C) 2012-2014 Aleksey Lim
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

import os
import gettext
import logging
import mimetypes
from os.path import join

import xapian

from sugar_network import toolkit, db
from sugar_network.model.routes import FrontRoutes
from sugar_network.toolkit.spec import parse_version, parse_requires
from sugar_network.toolkit.spec import EMPTY_LICENSE
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit import i18n, http, svg_to_png, exception, enforce


CONTEXT_TYPES = [
        'activity', 'group', 'package', 'book',
        ]

POST_TYPES = [
        'review',        # Review the Context
        'object',        # Object generated by Context application
        'question',      # Q&A request
        'answer',        # Q&A response
        'issue',         # Propblem with the Context
        'announce',      # General announcement
        'notification',  # Auto-generated Post for updates within the Context
        'feedback',      # Review parent Post
        'post',          # General purpose dependent Post
        ]

STABILITIES = [
        'insecure', 'buggy', 'developer', 'testing', 'stable',
        ]

RESOURCES = (
        'sugar_network.model.context',
        'sugar_network.model.post',
        'sugar_network.model.report',
        'sugar_network.model.user',
        )

_logger = logging.getLogger('model')


class Rating(db.List):

    def __init__(self, **kwargs):
        db.List.__init__(self, db.Numeric(), default=[0, 0], **kwargs)

    def slotting(self, value):
        rating = float(value[1]) / value[0] if value[0] else 0
        return xapian.sortable_serialise(rating)


class Release(object):

    def typecast(self, release):
        if this.resource.exists and \
                'activity' not in this.resource['type'] and \
                'book' not in this.resource['type']:
            return release
        if not isinstance(release, dict):
            __, release = load_bundle(
                    this.volume.blobs.post(release, this.request.content_type),
                    context=this.request.guid)
        return release['bundles']['*-*']['blob'], release

    def teardown(self, release):
        if this.resource.exists and \
                'activity' not in this.resource['type'] and \
                'book' not in this.resource['type']:
            return
        for bundle in release['bundles'].values():
            this.volume.blobs.delete(bundle['blob'])

    def encode(self, value):
        return []


def generate_node_stats(volume):

    def calc_rating(**kwargs):
        rating = [0, 0]
        alldocs, __ = volume['post'].find(**kwargs)
        for post in alldocs:
            if post['vote']:
                rating[0] += 1
                rating[1] += post['vote']
        return rating

    alldocs, __ = volume['context'].find()
    for context in alldocs:
        rating = calc_rating(type='review', context=context.guid)
        volume['context'].update(context.guid, {'rating': rating})

    alldocs, __ = volume['post'].find(topic='')
    for topic in alldocs:
        rating = calc_rating(type='feedback', topic=topic.guid)
        volume['post'].update(topic.guid, {'rating': rating})


def populate_context_images(props, svg):
    if 'guid' in props:
        from sugar_network.toolkit.sugar import color_svg
        svg = color_svg(svg, props['guid'])
    blobs = this.volume.blobs
    props['artifact_icon'] = blobs.post(svg, 'image/svg+xml').digest
    props['icon'] = blobs.post(svg_to_png(svg, 55, 55), 'image/png').digest
    props['logo'] = blobs.post(svg_to_png(svg, 140, 140), 'image/png').digest


def load_bundle(blob, context=None, initial=False, extra_deps=None):
    contexts = this.volume['context']
    context_type = None
    context_meta = None
    release_notes = None
    release = {}
    version = None

    try:
        bundle = Bundle(blob.path, mime_type='application/zip')
    except Exception:
        context_type = 'book'
        if not context:
            context = this.request['context']
        version = this.request['version']
        if 'license' in this.request:
            release['license'] = this.request['license']
            if isinstance(release['license'], basestring):
                release['license'] = [release['license']]
        release['bundles'] = {
                '*-*': {
                    'bundle': blob.digest,
                    },
                }
    else:
        context_type = 'activity'
        unpack_size = 0

        with bundle:
            changelog = join(bundle.rootdir, 'CHANGELOG')
            for arcname in bundle.get_names():
                if changelog and arcname == changelog:
                    with bundle.extractfile(changelog) as f:
                        release_notes = f.read()
                    changelog = None
                unpack_size += bundle.getmember(arcname).size
            spec = bundle.get_spec()
            context_meta = _load_context_metadata(bundle, spec)

        if not context:
            context = spec['context']
        else:
            enforce(context == spec['context'],
                    http.BadRequest, 'Wrong context')
        if extra_deps:
            spec.requires.update(parse_requires(extra_deps))

        version = spec['version']
        release['stability'] = spec['stability']
        if spec['license'] is not EMPTY_LICENSE:
            release['license'] = spec['license']
        release['commands'] = spec.commands
        release['requires'] = spec.requires
        release['bundles'] = {
                '*-*': {
                    'blob': blob.digest,
                    'unpack_size': unpack_size,
                    },
                }
        blob['content-type'] = 'application/vnd.olpc-sugar'

    enforce(context, http.BadRequest, 'Context is not specified')
    enforce(version, http.BadRequest, 'Version is not specified')
    release['version'] = parse_version(version)
    if initial and not contexts.exists(context):
        enforce(context_meta, http.BadRequest, 'No way to initate context')
        context_meta['guid'] = context
        context_meta['type'] = [context_type]
        this.call(method='POST', path=['context'], content=context_meta)
    else:
        enforce(context_type in contexts[context]['type'],
                http.BadRequest, 'Inappropriate bundle type')
    context_doc = contexts[context]

    if 'license' not in release:
        releases = context_doc['releases'].values()
        enforce(releases, http.BadRequest, 'License is not specified')
        recent = max(releases, key=lambda x: x.get('value', {}).get('release'))
        enforce(recent, http.BadRequest, 'License is not specified')
        release['license'] = recent['value']['license']

    _logger.debug('Load %r release: %r', context, release)

    if this.request.principal in context_doc['author']:
        patch = context_doc.format_patch(context_meta)
        if patch:
            this.call(method='PUT', path=['context', context], content=patch)
            context_doc.props.update(patch)
        # TRANS: Release notes title
        title = i18n._('%(name)s %(version)s release')
    else:
        # TRANS: 3rd party release notes title
        title = i18n._('%(name)s %(version)s third-party release')
    release['announce'] = this.call(method='POST', path=['post'],
            content={
                'context': context,
                'type': 'notification',
                'title': i18n.encode(title,
                    name=context_doc['title'],
                    version=version,
                    ),
                'message': release_notes or '',
                },
            content_type='application/json')

    blob['content-disposition'] = 'attachment; filename="%s-%s%s"' % (
            ''.join(i18n.decode(context_doc['title']).split()),
            version, mimetypes.guess_extension(blob.get('content-type')) or '',
            )
    this.volume.blobs.update(blob.digest, blob)

    return context, release


def _load_context_metadata(bundle, spec):
    result = {}
    for prop in ('homepage', 'mime_types'):
        if spec[prop]:
            result[prop] = spec[prop]
    result['guid'] = spec['context']

    try:
        icon_file = bundle.extractfile(join(bundle.rootdir, spec['icon']))
        populate_context_images(result, icon_file.read())
        icon_file.close()
    except Exception:
        exception(_logger, 'Failed to load icon')

    msgids = {}
    for prop, confname in [
            ('title', 'name'),
            ('summary', 'summary'),
            ('description', 'description'),
            ]:
        if spec[confname]:
            msgids[prop] = spec[confname]
            result[prop] = {'en': spec[confname]}
    with toolkit.mkdtemp() as tmpdir:
        for path in bundle.get_names():
            if not path.endswith('.mo'):
                continue
            mo_path = path.strip(os.sep).split(os.sep)
            if len(mo_path) != 5 or mo_path[1] != 'locale':
                continue
            lang = mo_path[2]
            bundle.extract(path, tmpdir)
            try:
                translation = gettext.translation(spec['context'],
                        join(tmpdir, *mo_path[:2]), [lang])
                for prop, value in msgids.items():
                    msgstr = translation.gettext(value).decode('utf8')
                    if lang == 'en' or msgstr != value:
                        result[prop][lang] = msgstr
            except Exception:
                exception(_logger, 'Gettext failed to read %r', mo_path[-1])

    return result
