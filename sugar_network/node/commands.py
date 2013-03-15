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
import logging
import hashlib
from os.path import exists, join

from sugar_network import db, node
from sugar_network.node import auth, obs, stats_node
from sugar_network.resources.volume import Commands
from sugar_network.toolkit import router, util, coroutine, exception, enforce


_MAX_STATS_LENGTH = 100

_logger = logging.getLogger('node.commands')


class NodeCommands(db.VolumeCommands, Commands):

    def __init__(self, guid, volume):
        db.VolumeCommands.__init__(self, volume)
        Commands.__init__(self)

        self._guid = guid
        self._stats = None

        if stats_node.stats_node.value:
            self._stats = stats_node.Sniffer(volume)
            coroutine.spawn(self._commit_stats)

        self.volume.connect(self.broadcast)

    @property
    def guid(self):
        return self._guid

    @router.route('GET', '/packages')
    def packages(self, request, response):
        response.content_type = 'application/json'
        if len(request.path) <= 3:
            path = join(obs.obs_presolve_path.value, *request.path[1:])
            return os.listdir(path)
        elif len(request.path) == 4:
            path = join(obs.obs_presolve_path.value, *request.path[1:])
            return db.PropertyMetadata(blob=path, filename=request.path[-1],
                    mime_type='application/json')
        else:
            raise RuntimeError('Incorrect path')

    @router.route('HEADER', '/packages')
    def try_packages(self, request, response):
        enforce(len(request.path) == 4, 'Incorrect path')
        path = join(obs.obs_presolve_path.value, *request.path[1:])
        enforce(exists(path), db.NotFound, 'No such package')

    @db.volume_command(method='GET', cmd='stat',
            mime_type='application/json')
    def stat(self):
        # TODO Remove, it is deprecated
        return self.info()

    @db.volume_command(method='GET', cmd='info',
            mime_type='application/json')
    def info(self):
        documents = {}
        for name, directory in self.volume.items():
            documents[name] = {'mtime': directory.mtime}
        return {'guid': self._guid, 'documents': documents}

    @db.volume_command(method='GET', cmd='stats',
            mime_type='application/json', arguments={
                'start': db.to_int,
                'end': db.to_int,
                'resolution': db.to_int,
                'source': db.to_list,
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
            enforce('.' in i, 'Misnamed source')
            db_name, ds_name = i.split('.', 1)
            dbs.setdefault(db_name, []).append(ds_name)
        result = {}

        for rdb in self._stats.rrd:
            if rdb.name not in dbs:
                continue
            info = result[rdb.name] = []
            for ts, ds_values in rdb.get(start, end, resolution):
                values = {}
                for name in dbs[rdb.name]:
                    values[name] = ds_values.get(name)
                info.append((ts, values))

        return result

    @db.document_command(method='DELETE',
            permissions=db.ACCESS_AUTH | db.ACCESS_AUTHOR)
    def delete(self, document, guid):
        # Servers data should not be deleted immediately
        # to let master-slave synchronization possible
        directory = self.volume[document]
        directory.update(guid, {'layer': ['deleted']})

    @db.document_command(method='PUT', cmd='attach',
            permissions=db.ACCESS_AUTH)
    def attach(self, document, guid, request):
        auth.validate(request, 'root')
        directory = self.volume[document]
        doc = directory.get(guid)
        # TODO Reading layer here is a race
        layer = list(set(doc['layer']) | set(request.content))
        directory.update(guid, {'layer': layer})

    @db.document_command(method='PUT', cmd='detach',
            permissions=db.ACCESS_AUTH)
    def detach(self, document, guid, request):
        auth.validate(request, 'root')
        directory = self.volume[document]
        doc = directory.get(guid)
        # TODO Reading layer here is a race
        layer = list(set(doc['layer']) - set(request.content))
        directory.update(guid, {'layer': layer})

    @db.volume_command(method='GET', cmd='whoami',
            mime_type='application/json')
    def whoami(self, request):
        roles = []
        if auth.try_validate(request, 'root'):
            roles.append('root')
        return {'roles': roles, 'guid': request.principal, 'route': 'direct'}

    @db.document_command(method='GET', cmd='clone',
            arguments={'requires': db.to_list})
    def clone(self, document, guid, version, requires, stability='stable'):
        enforce(document == 'context', 'No way to clone')
        request = router.Request(method='GET', document='implementation',
                context=guid, version=version, stability=stability,
                requires=requires, order_by='-version', limit=1,
                reply=['guid'])
        impls = self.call(request, db.Response())['result']
        enforce(impls, db.NotFound, 'No implementations found')
        request = router.Request(method='GET', document='implementation',
                guid=impls[0]['guid'], prop='data')
        return self.call(request, db.Response())

    @db.document_command(method='GET', cmd='deplist',
            mime_type='application/json')
    def deplist(self, document, guid, repo):
        """List of native packages context is dependening on.

        Command return only GNU/Linux package names and ignores
        Sugar Network dependencies.

        :param repo:
            OBS repository name to get package names for, e.g.,
            Fedora-14
        :returns:
            list of package names

        """
        enforce(document == 'context')
        enforce(repo, 'Argument %r should be set', 'repo')
        context = self.volume['context'].get(guid)

        result = []

        for package in context['dependencies']:
            dep = self.volume['context'].get(package)
            enforce(repo in dep['packages'],
                    'No packages for %r on %r', package, repo)
            result.extend(dep['packages'][repo].get('binary') or [])

        return result

    @db.document_command(method='GET', cmd='feed',
            mime_type='application/json')
    def feed(self, document, guid, layer, distro, request):
        enforce(document == 'context')
        context = self.volume['context'].get(guid)

        versions = []
        impls, __ = self.volume['implementation'].find(limit=db.MAX_LIMIT,
                context=context.guid, layer=layer)
        for impl in impls:
            for arch, spec in impl['spec'].items():
                spec['guid'] = impl.guid
                spec['version'] = impl['version']
                spec['arch'] = arch
                spec['stability'] = impl['stability']
                if context['dependencies']:
                    requires = spec.setdefault('requires', {})
                    for i in context['dependencies']:
                        requires.setdefault(i, {})
                versions.append(spec)

        result = {
                'name': context.get('title',
                    accept_language=request.accept_language),
                'implementations': versions,
                }
        if distro:
            aliases = context['aliases'].get(distro)
            if aliases and 'binary' in aliases:
                result['packages'] = aliases['binary']

        return result

    def call(self, request, response=None):
        if node.static_url.value:
            request.static_prefix = node.static_url.value
        try:
            result = db.VolumeCommands.call(self, request, response)
        except router.HTTPStatusPass:
            if self._stats is not None:
                self._stats.log(request)
            raise
        else:
            if self._stats is not None:
                self._stats.log(request)
        return result

    def resolve(self, request):
        cmd = db.VolumeCommands.resolve(self, request)
        if cmd is None:
            return

        if cmd.permissions & db.ACCESS_AUTH:
            enforce(auth.try_validate(request, 'user'), router.Unauthorized,
                    'User is not authenticated')

        if cmd.permissions & db.ACCESS_AUTHOR and 'guid' in request:
            if request['document'] == 'user':
                allowed = (request.principal == request['guid'])
            else:
                doc = self.volume[request['document']].get(request['guid'])
                allowed = (request.principal in doc['author'])
            enforce(allowed or auth.try_validate(request, 'root'),
                    db.Forbidden, 'Operation is permitted only for authors')

        return cmd

    def before_create(self, request, props):
        if request['document'] == 'user':
            props['guid'], props['pubkey'] = _load_pubkey(props['pubkey'])
        db.VolumeCommands.before_create(self, request, props)

    @db.directory_command_pre(method='GET')
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

    @db.document_command_post(method='GET')
    def _NodeCommands_get_post(self, request, response, result):
        directory = self.volume[request['document']]
        doc = directory.get(request['guid'])
        enforce('deleted' not in doc['layer'], db.NotFound,
                'Document deleted')
        return result

    def _commit_stats(self):
        while True:
            coroutine.sleep(stats_node.stats_node_step.value)
            self._stats.commit()


def _load_pubkey(pubkey):
    pubkey = pubkey.strip()
    try:
        with util.NamedTemporaryFile() as key_file:
            key_file.file.write(pubkey)
            key_file.file.flush()
            # SSH key needs to be converted to PKCS8 to ket M2Crypto read it
            pubkey_pkcs8 = util.assert_call(
                    ['ssh-keygen', '-f', key_file.name, '-e', '-m', 'PKCS8'])
    except Exception:
        message = 'Cannot read DSS public key gotten for registeration'
        exception(message)
        if node.trust_users.value:
            logging.warning('Failed to read registration pubkey, '
                    'but we trust users')
            # Keep SSH key for further converting to PKCS8
            pubkey_pkcs8 = pubkey
        else:
            raise db.Forbidden(message)

    return str(hashlib.sha1(pubkey.split()[1]).hexdigest()), pubkey_pkcs8
