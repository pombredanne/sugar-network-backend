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
import logging
from xml.etree import cElementTree as ElementTree
from os.path import join, exists, basename

from sugar_network import toolkit
from sugar_network.toolkit import Option, http, enforce


obs_url = Option(
        'OBS API url; the entire OBS related functionality makes sense only '
        'for master server',
        default='https://obs.sugarlabs.org')

obs_project = Option(
        'OBS project to use unattended building',
        default='base')

obs_presolve_project = Option(
        'OBS project to use with packagekit-backend-presolve',
        default='presolve')

_logger = logging.getLogger('node.obs')
_conn = None
_repos = {}


def get_repos():
    return _get_repos(obs_project.value)


def resolve(repo, arch, packages):
    response = _request('GET', ['resolve'], params={
        'project': obs_project.value,
        'repository': repo,
        'arch': arch,
        'package': packages,
        })
    return dict(response.find('binary').items())


def presolve(repo_name, packages, dst_path):
    # TODO The only repo is OLPC which is Fedora-14 based
    if repo_name != 'Fedora-14':
        return
    for repo in _get_repos(obs_presolve_project.value):
        dst_dir = join(dst_path, 'packages',
                obs_presolve_project.value, repo['name'])
        result = {}
        to_download = []

        for package in packages:
            files = result.setdefault(package, {})
            try:
                for repo_arch in repo['arches']:
                    response = _request('GET', ['resolve'], params={
                        'project': obs_presolve_project.value,
                        'repository': repo['name'],
                        'arch': repo_arch,
                        'package': package,
                        'withdeps': '1',
                        'exclude': 'sweets-sugar',
                        })
                    for binary in response.findall('binary'):
                        binary = dict(binary.items())
                        arch = binary.pop('arch')
                        url = binary.pop('url')
                        filename = binary['path'] = basename(url)
                        path = join(dst_dir, filename)
                        if not exists(path):
                            to_download.append((url, path))
                        files.setdefault(arch, []).append(binary)
            except Exception:
                _logger.exception('Failed to presolve %r on %s',
                        packages, repo['name'])
                continue

        _logger.debug('Presolve %r on %s', packages, repo['name'])

        if not exists(dst_dir):
            os.makedirs(dst_dir)
        for url, path in to_download:
            _conn.download(url, path)
        for package, info in result.items():
            with toolkit.new_file(join(dst_dir, package)) as f:
                json.dump(info, f)

        return {'repo': repo['name'], 'packages': result}


def _request(*args, **kwargs):
    global _conn

    if _conn is None:
        _conn = http.Connection(obs_url.value)

    response = _conn.request(*args, allowed=(400, 404), **kwargs)
    enforce(response.headers.get('Content-Type') == 'text/xml',
            'Irregular OBS response')
    reply = ElementTree.fromstring(response.content)

    if response.status_code != 200:
        summary = reply.find('summary')
        enforce(summary is not None, 'Unknown OBS error')
        raise RuntimeError(summary.text)

    return reply


def _get_repos(project):
    if project in _repos:
        return _repos[project]

    if not obs_url.value:
        return []

    repos = _repos[project] = []
    for repo in _request('GET', ['build', project]).findall('entry'):
        name = repo.get('name')
        arches = _request('GET', ['build', project, name])
        info = {'name': name,
                'arches': [i.get('name') for i in arches.findall('entry')],
                }
        if '-' in name:
            info['lsb_id'], info['lsb_release'] = name.split('-', 1)
        repos.append(info)

    return repos
