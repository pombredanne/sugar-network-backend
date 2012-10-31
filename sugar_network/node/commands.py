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
import hashlib
from os.path import exists, join

import active_document as ad
from sugar_network import node, toolkit
from sugar_network.node import auth, obs
from sugar_network.resources.volume import Commands, VolumeCommands
from sugar_network.toolkit import router
from active_toolkit import util, enforce


_DEFAULT_MASTER_GUID = 'api-testing.network.sugarlabs.org'
_MAX_STATS_LENGTH = 100

_logger = logging.getLogger('node.commands')


class NodeCommands(VolumeCommands, Commands):

    def __init__(self, volume, stats=None):
        VolumeCommands.__init__(self, volume)
        Commands.__init__(self)
        self._is_master = False
        self._stats = stats

        node_path = join(volume.root, 'node')
        master_path = join(volume.root, 'master')

        if exists(node_path):
            with file(node_path) as f:
                self._guid = f.read().strip()
        elif exists(master_path):
            with file(master_path) as f:
                self._guid = f.read().strip()
            self._is_master = True
        else:
            self._guid = ad.uuid()
            with file(node_path, 'w') as f:
                f.write(self._guid)

        if not self._is_master and not exists(master_path):
            with file(master_path, 'w') as f:
                f.write(_DEFAULT_MASTER_GUID)

    @property
    def is_master(self):
        return self._is_master

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

    @ad.volume_command(method='GET', mime_type='text/html')
    def hello(self):
        return _HELLO_HTML

    @ad.volume_command(method='GET', cmd='info',
            mime_type='application/json')
    def info(self):
        documents = {}
        for name, directory in self.volume.items():
            documents[name] = {'mtime': directory.mtime}
        return {'guid': self._guid,
                'master': self._is_master,
                'documents': documents,
                }

    @ad.volume_command(method='GET', cmd='stats',
            mime_type='application/json', arguments={
                'start': ad.to_int,
                'end': ad.to_int,
                'resolution': ad.to_int,
                'source': ad.to_list,
                })
    def stats(self, start, end, resolution, source):
        if not source:
            return {}

        enforce(self._stats is not None, 'Node stats is disabled')
        enforce(start < end, "Argument 'start' should be less than 'end'")
        enforce(resolution > 0, "Argument 'resolution' should be more than 0")

        min_resolution = (end - start) / _MAX_STATS_LENGTH
        if resolution < min_resolution:
            _logger.debug('Resulution is too short, use %s instead',
                    min_resolution)
            resolution = min_resolution

        dbs = {}
        for i in source:
            enforce('.' in i, 'Misnamed source name')
            db_name, ds_name = i.split('.', 1)
            dbs.setdefault(db_name, []).append(ds_name)
        result = {}

        for db in self._stats.rrd:
            if db.name not in dbs:
                continue
            stats = result[db.name] = []
            for ts, ds_values in db.get(start, end, resolution):
                values = {}
                for name in dbs[db.name]:
                    values[name] = ds_values.get(name)
                stats.append((ts, values))

        return result

    @ad.document_command(method='DELETE',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def delete(self, document, guid):
        # Servers data should not be deleted immediately
        # to let master-node synchronization possible
        directory = self.volume[document]
        directory.update(guid, {'layer': ['deleted']})

    @ad.document_command(method='PUT', cmd='attach',
            permissions=ad.ACCESS_AUTH)
    def attach(self, document, guid, request):
        auth.validate(request, 'root')
        directory = self.volume[document]
        doc = directory.get(guid)
        # TODO Reading layer here is a race
        layer = list(set(doc['layer']) | set(request.content))
        directory.update(guid, {'layer': layer})

    @ad.document_command(method='PUT', cmd='detach',
            permissions=ad.ACCESS_AUTH)
    def detach(self, document, guid, request):
        auth.validate(request, 'root')
        directory = self.volume[document]
        doc = directory.get(guid)
        # TODO Reading layer here is a race
        layer = list(set(doc['layer']) - set(request.content))
        directory.update(guid, {'layer': layer})

    @ad.document_command(method='PUT', cmd='merge',
            permissions=ad.ACCESS_AUTH)
    def merge(self, document, guid, request):
        auth.validate(request, 'root')
        directory = self.volume[document]
        directory.merge(guid, request.content)

    @ad.volume_command(method='GET', cmd='whoami',
            mime_type='application/json')
    def whoami(self, request):
        roles = []
        if auth.try_validate(request, 'root'):
            roles.append('root')
        return {'roles': roles, 'guid': request.principal}

    def call(self, request, response=None):
        try:
            return VolumeCommands.call(self, request, response)
        except router.HTTPStatusPass:
            if self._stats is not None:
                self._stats.log(request)
            raise
        else:
            if self._stats is not None:
                self._stats.log(request)

    def resolve(self, request):
        cmd = VolumeCommands.resolve(self, request)
        if cmd is None:
            return

        if cmd.permissions & ad.ACCESS_AUTH:
            enforce(auth.try_validate(request, 'user'), router.Unauthorized,
                    'User is not authenticated')

        if cmd.permissions & ad.ACCESS_AUTHOR and 'guid' in request:
            if request['document'] == 'user':
                allowed = (request.principal == request['guid'])
            else:
                doc = self.volume[request['document']].get(request['guid'])
                allowed = (request.principal in doc['user'])
            enforce(allowed or auth.try_validate(request, 'root'),
                    ad.Forbidden, 'Operation is permitted only for authors')

        return cmd

    def connect(self, callback, condition=None, **kwargs):
        self.volume.connect(callback, condition)

    def before_create(self, request, props):
        if request['document'] == 'user':
            props['guid'], props['pubkey'] = _load_pubkey(props['pubkey'])
        elif request.principal:
            props['user'] = [request.principal]
            self._set_author(props)

        if self._is_master and 'implement' in props:
            implement = props['implement']
            if not isinstance(implement, basestring):
                implement = implement[0]
            props['guid'] = implement

        VolumeCommands.before_create(self, request, props)

    def before_update(self, request, props):
        if 'user' in props:
            self._set_author(props)
        VolumeCommands.before_update(self, request, props)

    @ad.directory_command_pre(method='GET')
    def _NodeCommands_find_pre(self, request):
        if 'limit' not in request:
            request['limit'] = node.find_limit.value
        elif request['limit'] > node.find_limit.value:
            _logger.warning('The find limit is restricted to %s',
                    node.find_limit.value)
            request['limit'] = node.find_limit.value

        layer = request.get('layer', ['public'])
        if 'deleted' in layer:
            _logger.warning('Requesting "deleted" layer')
            layer.remove('deleted')
        request['layer'] = layer

    @ad.document_command_post(method='GET')
    def _NodeCommands_get_post(self, request, response, result):
        directory = self.volume[request['document']]
        doc = directory.get(request['guid'])
        enforce('deleted' not in doc['layer'], ad.NotFound,
                'Document deleted')
        return result

    def _set_author(self, props):
        users = self.volume['user']
        authors = []
        for user_guid in props['user']:
            if not users.exists(user_guid):
                _logger.warning('No %r user to set author property',
                        user_guid)
                continue
            user = users.get(user_guid)
            if user['name']:
                authors.append(user['name'])
        props['author'] = authors

    def _list_repos(self):
        if self.is_master:
            # Node should not depend on OBS
            repos = obs.get_presolve_repos()
        else:
            repos = []
        return {'total': len(repos), 'result': repos}

    def _list_packages(self, request):
        directory = self.volume['context']
        documents, total = directory.find(type='package',
                offset=request.get('offset'), limit=request.get('limit'))
        return {'total': total, 'result': [i.guid for i in documents]}

    def _get_package(self, repo, package):
        directory = self.volume['context']
        context = directory.get(package)
        enforce('package' in context.get('type'), ad.NotFound,
                'Is not a package')
        presolve = context.get('presolve', {}).get(repo)
        enforce(presolve and 'binary' in presolve, ad.NotFound,
                'No presolve info')
        return presolve['binary']


def _load_pubkey(pubkey):
    pubkey = pubkey.strip()
    try:
        with toolkit.NamedTemporaryFile() as key_file:
            key_file.file.write(pubkey)
            key_file.file.flush()
            # SSH key needs to be converted to PKCS8 to ket M2Crypto read it
            pubkey_pkcs8 = util.assert_call(
                    ['ssh-keygen', '-f', key_file.name, '-e', '-m', 'PKCS8'])
    except Exception:
        message = 'Cannot read DSS public key gotten for registeration'
        util.exception(message)
        if node.trust_users.value:
            logging.warning('Failed to read registration pubkey, '
                    'but we trust users')
            # Keep SSH key for further converting to PKCS8
            pubkey_pkcs8 = pubkey
        else:
            raise ad.Forbidden(message)

    return str(hashlib.sha1(pubkey.split()[1]).hexdigest()), pubkey_pkcs8


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Consult <a href="http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""
