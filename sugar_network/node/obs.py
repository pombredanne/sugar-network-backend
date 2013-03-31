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

import os
import json
import logging
from xml.etree import cElementTree as ElementTree
from os.path import join, exists

from sugar_network.toolkit import Option, http, util, exception, enforce


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

obs_presolve_path = Option(
        'filesystem path to store presolved packages',
        default='/var/lib/presolve')

_PRESOLVE_REPO_MAP = {
        'OLPC': 'Fedora',
        }

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


def presolve(aliases):
    for repo in _get_repos(obs_presolve_project.value):
        alias = aliases.get(_PRESOLVE_REPO_MAP[repo['distributor_id']])
        if not alias:
            continue

        binaries = alias['binary']
        while binaries:
            names = binaries.pop()
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
                        presolves.append((package, arch, response))
            except Exception:
                exception(_logger, 'Failed to presolve %r on %s',
                        names, repo['name'])
                continue

            _logger.debug('Presolve %r on %s', names, repo['name'])
            dep_graphs = {}

            for package, arch, response in presolves:
                dirname = join(obs_presolve_path.value, repo['name'], arch)
                if not exists(dirname):
                    os.makedirs(dirname)
                deps = dep_graphs[package] = []
                for pkg in response.findall('binary'):
                    deps.append(dict(pkg.items()))
                with util.new_file(join(dirname, package)) as f:
                    json.dump(deps, f)

            return {'repo': repo['name'], 'packages': dep_graphs}


def _request(*args, **kwargs):
    global _client

    if _client is None:
        _client = http.Client(obs_url.value)

    response = _client.request(*args, allowed=(400, 404), **kwargs)
    enforce(response.headers.get('Content-Type') == 'text/xml',
            'Irregular OBS response')
    reply = ElementTree.parse(response.raw).getroot()

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
        if '-' not in repo:
            continue
        arches = _request('GET', ['build', project, repo])
        repos.append({
            'distributor_id': repo.split('-', 1)[0],
            'name': repo,
            'arches': [i.get('name') for i in arches.findall('entry')],
            })

    return repos
