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

import logging
from xml.etree import cElementTree as ElementTree

from sugar_network.toolkit import http
from active_toolkit.options import Option
from active_toolkit import enforce


obs_url = Option(
        'OBS API url; the entire OBS related functionality makes sense only '
        'for master server',
        default='https://obs.sugarlabs.org')

obs_presolve_project = Option(
        'OBS project to use with packagekit-backend-presolve',
        default='resolve')


_logger = logging.getLogger('node.obs')
_client = None


def get_presolve_repos():
    result = []
    reply = _request('GET', ['build', obs_presolve_project.value])
    for i in reply.findall('entry'):
        result.append(i.get('name'))
    return result


def _request(*args, **kwargs):
    global _client

    if _client is None:
        _client = http.Client(obs_url.value)

    response = _client.request(*args, **kwargs)
    enforce(response.headers.get('Content-Type') == 'text/xml',
            'Irregular OBS response')
    # pylint: disable-msg=E1103
    return ElementTree.parse(response.raw).getroot()
