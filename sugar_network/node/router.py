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

import json
import logging

import active_document as ad
from sugar_network.node import obs
from sugar_network.toolkit import router
from active_toolkit import enforce


_logger = logging.getLogger('node.router')


class Router(router.Router):

    def __init__(self, commands):
        router.Router.__init__(self, commands)
        self._repos = None

    @router.route('GET', '/packages')
    def packages(self, request, response):
        response.content_type = 'application/json'
        if len(request.path) == 1:
            return self._list_repos()
        elif len(request.path) == 2:
            return self._list_packages(request)
        elif len(request.path) == 3:
            return self._get_package(request.path[1], request.path[2])
        else:
            raise RuntimeError('Incorrect path')

    @router.route('HEADER', '/packages')
    def try_packages(self, request, response):
        enforce(len(request.path) == 3, 'Incorrect path')
        self._get_package(request.path[1], request.path[2])

    def _list_repos(self):
        if self._repos is None:
            if self.commands.is_master:
                # Node should not depend on OBS
                self._repos = obs.get_presolve_repos()
            else:
                self._repos = []
        return {'total': len(self._repos), 'result': self._repos}

    def _list_packages(self, request):
        directory = self.commands.volume['context']
        documents, total = directory.find(type='package',
                offset=request.get('offset'), limit=request.get('limit'))
        return {'total': total.value, 'result': [i.guid for i in documents]}

    def _get_package(self, repo, package):
        directory = self.commands.volume['context']
        context = directory.get(package)
        meta = context.meta('feed')
        enforce(meta is not None and 'path' in meta, ad.NotFound, 'No feed')

        with file(meta['path']) as f:
            feed = json.load(f)
        enforce('presolve' in feed and repo in feed['presolve'], ad.NotFound,
                'No presolve info')

        return feed['presolve'][repo]
