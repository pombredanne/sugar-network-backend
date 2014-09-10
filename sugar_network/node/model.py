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
import json
import hashlib
import logging
import gettext
import mimetypes
from copy import deepcopy
from os.path import join

from sugar_network import db, toolkit
from sugar_network.model import context as _context, user as _user
from sugar_network.model import ICON_SIZE, LOGO_SIZE
from sugar_network.node.auth import Principal
from sugar_network.toolkit.router import ACL, File, Request, Response
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import http, i18n, ranges, packets, spec
from sugar_network.toolkit import svg_to_png, enforce


BATCH_SUFFIX = '.meta'

_logger = logging.getLogger('node.model')


class User(_user.User):

    def routed_creating(self):
        self.posts['guid'] = str(hashlib.sha1(self['pubkey']).hexdigest())
        _user.User.routed_creating(self)


class _ReleaseValue(dict):

    guid = None


class _Release(object):

    _subcast = db.Dict()

    def typecast(self, value):
        if isinstance(value, _ReleaseValue):
            return value.guid, value
        bundle = this.volume.blobs.post(value, this.request.content_type)
        __, value = load_bundle(bundle, context=this.request.guid)
        return value.guid, value

    def reprcast(self, value):
        return self._subcast.reprcast(value)

    def encode(self, value):
        return []

    def teardown(self, value):
        if 'bundles' in value:
            for bundle in value['bundles'].values():
                this.volume.blobs.delete(bundle['blob'])
        # TODO Delete presolved files


class Context(_context.Context):

    @db.stored_property(db.Aggregated, subtype=_Release(),
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
    def releases(self, value):
        return value

    def routed_creating(self):
        _context.Context.routed_creating(self)
        self._invalidate_solutions()

    def routed_updating(self):
        _context.Context.routed_updating(self)
        self._invalidate_solutions()

    def _invalidate_solutions(self):
        if self['releases'] and \
                [i for i in ('state', 'releases', 'dependencies')
                    if i in self.posts and self.posts[i] != self.orig(i)]:
            this.broadcast({
                'event': 'release',
                'seqno': this.volume.release_seqno.next(),
                })


class Volume(db.Volume):

    def __init__(self, root, resources, **kwargs):
        db.Volume.__init__(self, root, resources, **kwargs)
        self.release_seqno = toolkit.Seqno(join(root, 'var', 'seqno-release'))

    def close(self):
        db.Volume.close(self)
        self.release_seqno.commit()


def diff_volume(r, exclude=None, files=None, blobs=True, one_way=False):
    volume = this.volume
    if exclude:
        include = deepcopy(r)
        ranges.exclude(include, exclude)
    else:
        include = r
    last_seqno = None
    found = False

    try:
        for resource, directory in volume.items():
            if one_way and directory.resource.one_way:
                continue
            yield {'resource': resource}
            for doc in directory.diff(r):
                patch = doc.diff(include)
                if patch:
                    yield {'guid': doc.guid, 'patch': patch}
                    found = True
                last_seqno = max(last_seqno, doc['seqno'])
        if blobs:
            for blob in volume.blobs.diff(include):
                seqno = int(blob.meta.pop('x-seqno'))
                yield blob
                found = True
                last_seqno = max(last_seqno, seqno)
        for dirpath in files or []:
            for blob in volume.blobs.diff(include, dirpath):
                seqno = int(blob.meta.pop('x-seqno'))
                yield blob
                found = True
                last_seqno = max(last_seqno, seqno)
    except StopIteration:
        pass

    if found:
        commit_r = include if exclude else deepcopy(r)
        ranges.exclude(commit_r, last_seqno + 1, None)
        ranges.exclude(r, None, last_seqno)
        yield {'commit': commit_r}


def patch_volume(records, shift_seqno=True):
    volume = this.volume
    directory = None
    committed = []
    seqno = None if shift_seqno else False

    for record in records:
        if isinstance(record, File):
            if seqno is None:
                seqno = volume.seqno.next()
            volume.blobs.patch(record, seqno or 0)
            continue
        resource = record.get('resource')
        if resource:
            directory = volume[resource]
            continue
        guid = record.get('guid')
        if guid is not None:
            seqno = directory.patch(guid, record['patch'], seqno)
            continue
        commit = record.get('commit')
        if commit is not None:
            ranges.include(committed, commit)
            continue
        raise http.BadRequest('Malformed patch')

    return seqno, committed


def diff_resource(in_r):
    request = this.request
    enforce(request.resource != 'user', http.BadRequest,
            'Not allowed for User resource')
    doc = this.volume[request.resource][request.guid]
    enforce(doc.exists, http.NotFound, 'Resource not found')

    out_r = []
    if in_r is None:
        in_r = [[1, None]]
    patch = doc.diff(in_r, out_r)
    if not patch:
        return packets.encode([], compresslevel=0)
    blobs = []

    def add_blob(blob):
        if not isinstance(blob, File) or 'x-seqno' not in blob.meta:
            return
        seqno = int(blob.meta['x-seqno'])
        ranges.include(out_r, seqno, seqno)
        blobs.append(blob)

    for prop, meta in patch.items():
        prop = doc.metadata[prop]
        value = prop.reprcast(meta['value'])
        if isinstance(prop, db.Aggregated):
            for aggvalue in value:
                add_blob(aggvalue['value'])
        else:
            add_blob(value)

    return packets.encode(blobs, patch=patch, ranges=out_r, compresslevel=0)


def apply_batch(path):
    with file(path + BATCH_SUFFIX) as f:
        meta = json.load(f)
    principal = Principal(meta['principal'])
    principal.cap_create_with_guid = True
    only_nums = meta.get('failed')
    guid_map = meta.setdefault('guid_map', {})
    failed = meta['failed'] = []
    volume = this.volume

    def map_guid(remote_guid):
        local_guid = guid_map.get(remote_guid)
        if not local_guid:
            if volume[request.resource][remote_guid].exists:
                return remote_guid
            local_guid = guid_map[remote_guid] = toolkit.uuid()
        return local_guid

    with file(path, 'rb') as batch:
        num = 0
        for record in packets.decode(batch):
            num += 1
            if only_nums and not ranges.contains(only_nums, num):
                continue
            if isinstance(record, File):
                request = Request(**record.meta.pop('op'))
                request.content = record
            else:
                request = Request(**record['op'])
                props = record['content']
                keys = record.get('keys') or []
                enforce('guid' not in props or 'guid' in keys,
                        http.BadRequest, 'Guid values is not mapped')
                for key in keys:
                    enforce(key in props, http.BadRequest,
                            'No mapped property value')
                    props[key] = map_guid(props[key])
                request.content = props
            if request.guid and \
                    not volume[request.resource][request.guid].exists:
                request.guid = map_guid(request.guid)
            request.principal = principal
            try:
                this.call(request, Response())
            except Exception:
                _logger.exception('Failed to apply %r', request)
                ranges.include(failed, num, num)

    if failed:
        with toolkit.new_file(path + BATCH_SUFFIX) as f:
            json.dump(meta, f)
    else:
        os.unlink(path + BATCH_SUFFIX)
        os.unlink(path)


def load_bundle(blob, context=None, initial=False, extra_deps=None,
        license=None, release_notes=None, update_context=True):
    context_type = None
    context_meta = None
    context_icon = None
    context_updated = False
    version = None
    release = _ReleaseValue()
    release.guid = blob.digest

    try:
        bundle = Bundle(blob.path, mime_type='application/zip')
    except Exception:
        context_type = 'book'
        if not context:
            context = this.request['context']
        version = this.request['version']
        release['bundles'] = {
                '*-*': {
                    'blob': blob.digest,
                    },
                }
        release['stability'] = 'stable'
    else:
        context_type = 'activity'
        unpack_size = 0

        with bundle:
            changelog = join(bundle.rootdir, 'CHANGELOG')
            for arcname in bundle.get_names():
                if not release_notes and changelog and arcname == changelog:
                    with bundle.extractfile(changelog) as f:
                        release_notes = f.read()
                    changelog = None
                unpack_size += bundle.getmember(arcname).size
            spc = bundle.get_spec()
            context_meta, context_icon = _load_context_metadata(bundle, spc)

        if not context:
            context = spc['context']
        else:
            enforce(context == spc['context'],
                    http.BadRequest, 'Wrong context')
        if extra_deps:
            spc.requires.update(spec.parse_requires(extra_deps))

        version = spc['version']
        release['stability'] = spc['stability']
        release['commands'] = spc.commands
        release['requires'] = spc.requires
        release['bundles'] = {
                '*-*': {
                    'blob': blob.digest,
                    'unpack_size': unpack_size,
                    },
                }
        if not license and spc['license'] is not spec.EMPTY_LICENSE:
            license = spc['license']
        blob.meta['content-type'] = 'application/vnd.olpc-sugar'

    enforce(context, http.BadRequest, 'Context is not specified')
    enforce(version, http.BadRequest, 'Version is not specified')
    release['version'] = spec.parse_version(version)

    doc = this.volume['context'][context]
    if initial and not doc.exists:
        enforce(context_meta, http.BadRequest, 'No way to initate context')
        context_meta['guid'] = context
        context_meta['type'] = [context_type]
        if context_icon:
            _generate_icons(context_icon, context_meta)
        with this.principal as principal:
            principal.cap_create_with_guid = True
            this.call(method='POST', path=['context'], content=context_meta,
                    principal=principal)
        context_updated = True
    else:
        enforce(doc.available, http.NotFound, 'No context')
        enforce(context_type in doc['type'],
                http.BadRequest, 'Inappropriate bundle type')

    if not license:
        license = this.request.get('license')
    if license:
        if isinstance(license, basestring):
            license = [license]
    else:
        releases = doc['releases'].values()
        enforce(releases, http.BadRequest, 'License is not specified')
        recent = max(releases, key=lambda x: x.get('value', {}).get('release'))
        enforce(recent, http.BadRequest, 'License is not specified')
        license = recent['value']['license']
    release['license'] = license

    _logger.debug('Load %r release: %r', context, release)

    if this.principal in doc['author'] or this.principal.cap_author_override:
        if not context_updated and update_context:
            patch = doc.format_patch(context_meta) or {}
            if context_icon and doc['artefact_icon'] == 'assets/missing.svg':
                _generate_icons(context_icon, patch)
            if patch:
                this.call(method='PUT', path=['context', context],
                        content=patch, principal=this.principal)
                doc.posts.update(patch)
        # TRANS: Release notes title
        title = i18n._('%(name)s %(version)s release')
    else:
        # TRANS: 3rd party release notes title
        title = i18n._('%(name)s %(version)s third-party release')
    announce = {
        'context': context,
        'type': 'topic',
        'title': i18n.encode(title, name=doc['title'], version=version),
        'message': release_notes or '',
        'tags': ['release'],
        }
    release['announce'] = this.call(method='POST', path=['post'],
            content=announce, content_type='application/json',
            principal=this.principal)

    blob.meta['content-disposition'] = 'attachment; filename="%s-%s%s"' % (
            ''.join(i18n.decode(doc['title']).split()), version,
            mimetypes.guess_extension(blob.meta.get('content-type')) or '',
            )
    this.volume.blobs.update(blob.digest, blob.meta)

    return context, release


def _load_context_metadata(bundle, spc):
    result = {}
    for prop in ('homepage', 'mime_types'):
        if spc[prop]:
            result[prop] = spc[prop]
    result['guid'] = spc['context']
    icon_svg = None

    try:
        from sugar_network.toolkit.sugar import color_svg
        icon_file = bundle.extractfile(join(bundle.rootdir, spc['icon']))
        icon_svg = color_svg(icon_file.read(), result['guid'])
        icon_file.close()
    except Exception:
        _logger.exception('Failed to load icon')

    msgids = {}
    for prop, confname in [
            ('title', 'name'),
            ('summary', 'summary'),
            ('description', 'description'),
            ]:
        if spc[confname]:
            msgids[prop] = spc[confname]
            result[prop] = {'en': spc[confname]}
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
                translation = gettext.translation(spc['context'],
                        join(tmpdir, *mo_path[:2]), [lang])
                for prop, value in msgids.items():
                    msgstr = translation.gettext(value).decode('utf8')
                    if lang == 'en' or msgstr != value:
                        result[prop][lang] = msgstr
            except Exception:
                _logger.exception('Gettext failed to read %r', mo_path[-1])

    return result, icon_svg


def _generate_icons(svg, props):
    blobs = this.volume.blobs
    props['artefact_icon'] = \
            blobs.post(svg, 'image/svg+xml').digest
    props['icon'] = \
            blobs.post(svg_to_png(svg, ICON_SIZE), 'image/png').digest
    props['logo'] = \
            blobs.post(svg_to_png(svg, LOGO_SIZE), 'image/png').digest
