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

from os.path import join

import active_document as ad

from sugar_network.toolkit import router
from sugar_network.local import sugar, hub_root


class Router(router.Router):

    def authenticate(self, request):
        return sugar.uid()

    def call(self, request, response):
        if request.path and request.path[0] == 'hub':
            # Since SSE doesn't support CORS for now, have to serve Hub
            # from HTTP server instead of file:// for IPC users
            return self._serve_hub(request, response)
        request.access_level = ad.ACCESS_LOCAL
        return router.Router.call(self, request, response)

    def _serve_hub(self, request, response):
        if request.environ['PATH_INFO'] == '/hub':
            raise ad.Redirect('/hub/')
        path = request.path[1:]
        if not path:
            path = ['index.html']
        path = join(hub_root.value, *path)
        if path.endswith('.js'):
            response.content_type = 'text/javascript'
        if path.endswith('.css'):
            response.content_type = 'text/css'
        return router.stream_reader(file(path, 'rb'))
