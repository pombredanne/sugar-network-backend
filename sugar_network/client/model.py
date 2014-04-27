# Copyright (C) 2014 Aleksey Lim
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

import logging

from sugar_network import db
from sugar_network.model.user import User
from sugar_network.model.post import Post
from sugar_network.model.report import Report
from sugar_network.model.context import Context as _Context
from sugar_network.toolkit.router import ACL, File
from sugar_network.toolkit.coroutine import this


_logger = logging.getLogger('client.model')


class Context(_Context):

    @db.indexed_property(db.List, prefix='P', default=[],
            acl=ACL.READ | ACL.LOCAL)
    def pins(self, value):
        return value + this.injector.pins(self.guid)


class Volume(db.Volume):

    def __init__(self, root, resources=None):
        if resources is None:
            resources = [User, Context, Post, Report]
        db.Volume.__init__(self, root, resources)
        for directory in self.values():
            directory.metadata['author'].acl |= ACL.LOCAL


def dump_volume(volume):
    for resource, directory in volume.items():
        if not directory.has_seqno:
            continue

        for doc in directory:
            if not doc['seqno'] or doc['state'] != 'active':
                continue

            dump = {}
            op = dump['op'] = {}
            props = dump['content'] = {}
            keys = []
            postfix = []

            for name, prop in doc.metadata.items():
                meta = doc.meta(name)
                if meta is None or 'seqno' not in meta:
                    continue
                if isinstance(prop, db.Aggregated):
                    for aggid, value in doc.repr(name):
                        aggop = {
                            'method': 'POST',
                            'path': [resource, doc.guid, name, aggid],
                            }
                        if isinstance(value, File):
                            value.meta['op'] = aggop
                            postfix.append(value)
                        else:
                            postfix.append({'op': aggop, 'content': value})
                elif prop.acl & (ACL.WRITE | ACL.CREATE):
                    if isinstance(prop, db.Blob):
                        blob = volume.blobs.get(doc[name])
                        blob.meta['op'] = {
                            'method': 'PUT',
                            'path': [resource, doc.guid, name],
                            }
                        postfix.append(blob)
                    else:
                        if isinstance(prop, db.Reference):
                            keys.append(name)
                        props[name] = doc[name]

            if 'seqno' in doc.meta('guid'):
                keys.append('guid')
                props['guid'] = doc.guid
                op['method'] = 'POST'
                op['path'] = [resource]
            else:
                op['method'] = 'PUT'
                op['path'] = [resource, doc.guid]

            if keys:
                dump['keys'] = keys

            yield dump
            for dump in postfix:
                yield dump
