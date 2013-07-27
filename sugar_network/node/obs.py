# Copyright (C) 2012-2013 Aleksey Lim
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
_client = None
_repos = {}


def get_repos():
    return _get_repos(obs_project.value)


def resolve(repo, arch, names):
    for package in names:
        _request('GET', ['resolve'], params={
            'project': obs_project.value,
            'repository': repo,
            'arch': arch,
            'package': package,
            })


def presolve(aliases, dst_path):
    for repo in _get_repos(obs_presolve_project.value):
        # Presolves make sense only for XO, thus, for Fedora
        alias = aliases.get('Fedora')
        if not alias:
            continue

        name_variants = alias['binary']
        while name_variants:
            names = name_variants.pop()
            presolves = []
            try:
                for arch in repo['arches']:
                    for package in names:
                        response = _request('GET', ['resolve'], params={
                            'project': obs_presolve_project.value,
                            'repository': repo['name'],
                            'arch': arch,
                            'package': package,
                            'withdeps': '1',
                            'exclude': 'sugar',
                            })
                        binaries = []
                        for pkg in response.findall('binary'):
                            binaries.append(dict(pkg.items()))
                        presolves.append((package, binaries))
            except Exception:
                toolkit.exception(_logger, 'Failed to presolve %r on %s',
                        names, repo['name'])
                continue

            _logger.debug('Presolve %r on %s', names, repo['name'])

            dst_dir = join(dst_path, 'packages',
                    obs_presolve_project.value, repo['name'])
            if not exists(dst_dir):
                os.makedirs(dst_dir)
            result = {}

            for package, binaries in presolves:
                files = []
                for binary in binaries:
                    arch = binary.pop('arch')
                    if not files:
                        result.setdefault(package, {})[arch] = files
                    url = binary.pop('url')
                    filename = binary['path'] = basename(url)
                    path = join(dst_dir, filename)
                    if not exists(path):
                        _client.download(url, path)
                    files.append(binary)

            for package, info in result.items():
                with toolkit.new_file(join(dst_dir, package)) as f:
                    json.dump(info, f)

            return {'repo': repo['name'], 'packages': result}


def _request(*args, **kwargs):
    global _client

    if _client is None:
        _client = http.Client(obs_url.value)

    response = _client.request(*args, allowed=(400, 404), **kwargs)
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
        repo = repo.get('name')
        arches = _request('GET', ['build', project, repo])
        repos.append({
            'distributor_id': repo.split('-', 1)[0],
            'name': repo,
            'arches': [i.get('name') for i in arches.findall('entry')],
            })

    return repos
