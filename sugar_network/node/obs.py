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

obs_project = Option(
        'OBS project to use unattended building',
        default='base')

obs_presolve_project = Option(
        'OBS project to use with packagekit-backend-presolve',
        default='resolve')


_logger = logging.getLogger('node.obs')
_client = None
_repos = None
_presolve_repos = None


def get_repos():
    global _repos

    if _repos is None:
        _repos = []
        repos = _request('GET', ['build', obs_project.value])
        for repo in repos.findall('entry'):
            repo = repo.get('name')
            if '-' not in repo:
                continue
            arches = _request('GET', ['build', obs_project.value, repo])
            _repos.append({
                'distributor_id': repo.split('-', 1)[0],
                'name': repo,
                'arches': [i.get('name') for i in arches.findall('entry')],
                })

    return _repos


def get_presolve_repos():
    global _presolve_repos

    if _presolve_repos is None:
        _presolve_repos = []
        repos = _request('GET', ['build', obs_presolve_project.value])
        for repo in repos.findall('entry'):
            repo = repo.get('name')
            arches = _request('GET',
                    ['build', obs_presolve_project.value, repo])
            for arch in arches.findall('entry'):
                _presolve_repos.append({
                    'name': repo,
                    'arch': arch.get('name'),
                    })

    return _presolve_repos


def resolve(repo, arch, names):
    for package in names:
        _request('GET', ['resolve'], params={
            'project': obs_project.value,
            'repository': repo,
            'arch': arch,
            'package': package,
            })


def presolve(repo, arch, names):
    result = []

    for package in names:
        reply = _request('GET', ['resolve'], params={
            'project': obs_presolve_project.value,
            'repository': repo,
            'arch': arch,
            'package': package,
            'withdeps': '1',
            # TODO exclude package might be different on different platforms
            'exclude': 'sugar',
            })
        for pkg in reply.findall('binary'):
            result.append({
                # TODO more distros after supporting them PK backend
                'distributor_id': 'Fedora',
                'name': pkg.get('name'),
                'url': pkg.get('url'),
                })

    return result


def _request(*args, **kwargs):
    global _client

    if _client is None:
        _client = http.Client(obs_url.value)

    response = _client.request(*args, allowed=(400, 404), **kwargs)
    enforce(response.headers.get('Content-Type') == 'text/xml',
            'Irregular OBS response')
    # pylint: disable-msg=E1103
    reply = ElementTree.parse(response.raw).getroot()

    if response.status_code != 200:
        summary = reply.find('summary')
        enforce(summary is not None, 'Unknown OBS error')
        raise RuntimeError(summary.text)

    return reply
