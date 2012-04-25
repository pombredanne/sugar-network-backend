# Copyright (C) 2012, Aleksey Lim
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
from gettext import gettext as _

import gevent

import active_document as ad
from local_document import storage, sugar
from active_document.util import enforce


_logger = logging.getLogger('local_document.cp_offline')


class OfflineCommandsProcessor(object):

    def __init__(self, resources):
        self._resources = {}

        ad.principal.user = sugar.uid()

        for cls in resources:
            self._resources[cls.metadata.name] = cls

        for cls in self._resources.values():
            for __ in cls.populate():
                gevent.sleep()

    def close(self):
        while self._resources:
            __, cls = self._resources.popitem()
            cls.close()

    def ping(self, socket, hello=None):
        return 'pong: %s' % hello

    def create(self, socket, resource, props):
        obj = self._resource(resource).create(props)
        return {'guid': obj.guid}

    def update(self, socket, resource, guid, props):
        self._resource(resource).update(guid, props)

    def get(self, socket, resource, guid, reply=None):
        obj = self._resource(resource)(guid)
        return obj.properties(reply)

    def find(self, socket, resource, reply=None, **params):
        result, total = self._resource(resource).find(reply=reply, **params)
        return {'total': total.value,
                'result': [i.properties(reply) for i in result],
                }

    def delete(self, socket, resource, guid):
        self._resource(resource).delete(guid)

    def get_blob(self, socket, resource, guid, prop):
        path, mime_type = storage.get_blob(resource, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}

    def set_blob(self, socket, resource, guid, prop, files=None,
            url=None):
        raise NotImplementedError()

    def _resource(self, name):
        enforce(name in self._resources, _('Unknow "%s" resource'), name)
        return self._resources[name]
