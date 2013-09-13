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
import shutil
import gettext
import logging
import hashlib
from contextlib import contextmanager
from ConfigParser import ConfigParser
from os.path import join, isdir, exists

from sugar_network import node, toolkit, model
from sugar_network.node import stats_node, stats_user
from sugar_network.model.context import Context
# pylint: disable-msg=W0611
from sugar_network.toolkit.router import route, preroute, postroute
from sugar_network.toolkit.router import Request, ACL, fallbackroute
from sugar_network.toolkit.spec import EMPTY_LICENSE
from sugar_network.toolkit.spec import parse_requires, ensure_requires
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import http, coroutine, exception, enforce


_MAX_STATS_LENGTH = 100

_logger = logging.getLogger('node.routes')


class NodeRoutes(model.VolumeRoutes, model.FrontRoutes):

    def __init__(self, guid, volume):
        model.VolumeRoutes.__init__(self, volume)
        model.FrontRoutes.__init__(self)
        volume.broadcast = self.broadcast

        self._guid = guid
        self._stats = None
        self._authenticated = set()
        self._auth_config = None
        self._auth_config_mtime = 0

        if stats_node.stats_node.value:
            self._stats = stats_node.Sniffer(volume)
            coroutine.spawn(self._commit_stats)

    @property
    def guid(self):
        return self._guid

    @route('GET', cmd='status',
            mime_type='application/json')
    def status(self):
        return {'route': 'direct'}

    @route('GET', cmd='info',
            mime_type='application/json')
    def info(self):
        documents = {}
        for name, directory in self.volume.items():
            documents[name] = {'mtime': directory.mtime}
        return {'guid': self._guid, 'documents': documents}

    @route('GET', cmd='stats', arguments={
                'start': int, 'end': int, 'resolution': int, 'source': list},
            mime_type='application/json')
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

    @fallbackroute('GET', ['packages'])
    def route_packages(self, request, response):
        enforce(node.files_root.value, http.BadRequest, 'Disabled')

        if request.path and request.path[-1] == 'updates':
            root = join(node.files_root.value, *request.path[:-1])
            enforce(isdir(root), http.NotFound, 'Directory was not found')
            result = []
            last_modified = 0
            for filename in os.listdir(root):
                if '.' in filename:
                    continue
                path = join(root, filename)
                mtime = int(os.stat(path).st_mtime)
                if mtime > request.if_modified_since:
                    result.append(filename)
                    last_modified = max(last_modified, mtime)
            response.content_type = 'application/json'
            if last_modified:
                response.last_modified = last_modified
            return result

        path = join(node.files_root.value, *request.path)
        enforce(exists(path), http.NotFound, 'File was not found')
        if not isdir(path):
            return toolkit.iter_file(path)

        result = []
        for filename in os.listdir(path):
            if filename.endswith('.rpm') or filename.endswith('.deb'):
                continue
            result.append(filename)

        response.content_type = 'application/json'
        return result

    @route('POST', ['implementation'], cmd='submit',
            arguments={'initial': False},
            mime_type='application/json', acl=ACL.AUTH | ACL.AUTHOR)
    def submit_implementation(self, request, document):
        with toolkit.NamedTemporaryFile() as blob:
            shutil.copyfileobj(request.content_stream, blob)
            blob.flush()
            with load_bundle(self.volume, request, blob.name) as impl:
                impl['data']['blob'] = blob.name
        return impl['guid']

    @route('DELETE', [None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def delete(self, request):
        # Servers data should not be deleted immediately
        # to let master-slave synchronization possible
        request.call(method='PUT', path=request.path,
                content={'layer': ['deleted']})

    @route('PUT', [None, None], cmd='attach', acl=ACL.AUTH | ACL.SUPERUSER)
    def attach(self, request):
        # TODO Reading layer here is a race
        directory = self.volume[request.resource]
        doc = directory.get(request.guid)
        layer = list(set(doc['layer']) | set(request.content))
        directory.update(request.guid, {'layer': layer})

    @route('PUT', [None, None], cmd='detach', acl=ACL.AUTH | ACL.SUPERUSER)
    def detach(self, request):
        # TODO Reading layer here is a race
        directory = self.volume[request.resource]
        doc = directory.get(request.guid)
        layer = list(set(doc['layer']) - set(request.content))
        directory.update(request.guid, {'layer': layer})

    @route('GET', ['context', None], cmd='clone',
            arguments={'requires': list})
    def get_clone(self, request, response):
        return self._get_clone(request, response)

    @route('HEAD', ['context', None], cmd='clone',
            arguments={'requires': list})
    def head_clone(self, request, response):
        self._get_clone(request, response)

    @route('GET', ['context', None], cmd='deplist',
            mime_type='application/json', arguments={'requires': list})
    def deplist(self, request, repo):
        """List of native packages context is dependening on.

        Command return only GNU/Linux package names and ignores
        Sugar Network dependencies.

        :param repo:
            OBS repository name to get package names for, e.g.,
            Fedora-14
        :returns:
            list of package names

        """
        enforce(repo, 'Argument %r should be set', 'repo')

        spec = self._solve(request).meta('data')['spec']['*-*']
        common_deps = self.volume['context'].get(request.guid)['dependencies']
        result = []

        for package in set(spec.get('requires') or []) | set(common_deps):
            if package == 'sugar':
                continue
            dep = self.volume['context'].get(package)
            enforce(repo in dep['packages'],
                    'No packages for %r on %r', package, repo)
            result.extend(dep['packages'][repo].get('binary') or [])

        return result

    @route('GET', ['user', None], cmd='stats-info',
            mime_type='application/json', acl=ACL.AUTH)
    def user_stats_info(self, request):
        status = {}
        for rdb in stats_user.get_rrd(request.guid):
            status[rdb.name] = rdb.last + stats_user.stats_user_step.value

        # TODO Process client configuration in more general manner
        return {'enable': True,
                'step': stats_user.stats_user_step.value,
                'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
                'status': status,
                }

    @route('POST', ['user', None], cmd='stats-upload', acl=ACL.AUTH)
    def user_stats_upload(self, request):
        name = request.content['name']
        values = request.content['values']
        rrd = stats_user.get_rrd(request.guid)
        for timestamp, values in values:
            rrd[name].put(values, timestamp)

    @route('GET', ['report', None], cmd='log', mime_type='text/html')
    def log(self, request):
        # In further implementations, `data` might be a tarball
        data = self.volume[request.resource].get(request.guid).meta('data')
        if data and 'blob' in data:
            return file(data['blob'], 'rb')
        else:
            return ''

    @preroute
    def preroute(self, op, request):
        user = request.environ.get('HTTP_X_SN_LOGIN')
        if user and user not in self._authenticated and \
                (request.path != ['user'] or request.method != 'POST'):
            _logger.debug('Logging %r user', user)
            enforce(self.volume['user'].exists(user), http.Unauthorized,
                    'Principal does not exist')
            # TODO Process X-SN-signature
            self._authenticated.add(user)
        request.principal = user

        if op.acl & ACL.AUTH:
            enforce(self.authorize(user, 'user'), http.Unauthorized,
                    'User is not authenticated')
        if op.acl & ACL.AUTHOR and request.guid:
            if request.resource == 'user':
                allowed = (user == request.guid)
            else:
                doc = self.volume[request.resource].get(request.guid)
                allowed = (user in doc['author'])
            enforce(allowed or self.authorize(user, 'root'),
                    http.Forbidden, 'Operation is permitted only for authors')
        if op.acl & ACL.SUPERUSER:
            enforce(self.authorize(user, 'root'), http.Forbidden,
                    'Operation is permitted only for superusers')

    @postroute
    def postroute(self, request, response, result, error):
        if error is None or isinstance(error, http.StatusPass):
            if self._stats is not None:
                self._stats.log(request)

    def on_create(self, request, props, event):
        if request.resource == 'user':
            props['guid'], props['pubkey'] = _load_pubkey(props['pubkey'])
        model.VolumeRoutes.on_create(self, request, props, event)

    def on_update(self, request, props, event):
        model.VolumeRoutes.on_update(self, request, props, event)
        if 'deleted' in props.get('layer', []):
            event['event'] = 'delete'

    def find(self, request, reply):
        limit = request.get('limit')
        if limit is None or limit < 0:
            request['limit'] = node.find_limit.value
        elif limit > node.find_limit.value:
            _logger.warning('The find limit is restricted to %s',
                    node.find_limit.value)
            request['limit'] = node.find_limit.value
        layer = request.setdefault('layer', [])
        if 'deleted' in layer:
            _logger.warning('Requesting "deleted" layer')
            layer.remove('deleted')
        request.add('not_layer', 'deleted')
        return model.VolumeRoutes.find(self, request, reply)

    def get(self, request, reply):
        doc = self.volume[request.resource].get(request.guid)
        enforce('deleted' not in doc['layer'], http.NotFound,
                'Resource deleted')
        return model.VolumeRoutes.get(self, request, reply)

    def authorize(self, user, role):
        if role == 'user' and user:
            return True

        config_path = join(node.data_root.value, 'authorization.conf')
        if exists(config_path):
            mtime = os.stat(config_path).st_mtime
            if mtime > self._auth_config_mtime:
                self._auth_config_mtime = mtime
                self._auth_config = ConfigParser()
                self._auth_config.read(config_path)
        if self._auth_config is None:
            return False

        if not user:
            user = 'anonymous'
        if not self._auth_config.has_section(user):
            user = 'DEFAULT'
        if self._auth_config.has_option(user, role):
            return self._auth_config.get(user, role).strip().lower() in \
                    ('true', 'on', '1', 'allow')

    def _commit_stats(self):
        while True:
            coroutine.sleep(stats_node.stats_node_step.value)
            self._stats.commit()

    def _solve(self, request):
        requires = {}
        if 'requires' in request:
            for i in request['requires']:
                requires.update(parse_requires(i))
            request.pop('requires')
        else:
            request['limit'] = 1

        if 'stability' not in request:
            request['stability'] = 'stable'

        impls, __ = self.volume['implementation'].find(
                context=request.guid, order_by='-version', not_layer='deleted',
                **request)
        impl = None
        for impl in impls:
            if requires:
                impl_deps = impl.meta('data')['spec']['*-*']['requires']
                if not ensure_requires(impl_deps, requires):
                    continue
            break
        else:
            raise http.NotFound('No implementations found')
        return impl

    def _get_clone(self, request, response):
        impl = self._solve(request)
        result = request.call(method=request.method,
                path=['implementation', impl['guid'], 'data'],
                response=response)
        props = impl.properties(
                ['guid', 'context', 'license', 'version', 'stability'])
        props['data'] = response.meta
        response.meta = props
        return result


@contextmanager
def load_bundle(volume, request, bundle_path):
    impl = request.copy()
    initial = False
    if 'initial' in impl:
        initial = impl.pop('initial')
    data = impl.setdefault('data', {})
    data['blob'] = bundle_path
    contexts = volume['context']
    context = None

    try:
        bundle = Bundle(bundle_path, mime_type='application/zip')
    except Exception:
        _logger.debug('Load unrecognized bundle from %r', bundle_path)
        context_type = 'content'
    else:
        _logger.debug('Load Sugar Activity bundle from %r', bundle_path)
        context_type = 'activity'
        unpack_size = 0

        with bundle:
            for arcname in bundle.get_names():
                unpack_size += bundle.getmember(arcname).size
            spec = bundle.get_spec()
            context = _load_context_metadata(bundle, spec)
        if 'requires' in impl:
            spec.requires.update(parse_requires(impl.pop('requires')))

        impl['context'] = spec['context']
        impl['version'] = spec['version']
        impl['stability'] = spec['stability']
        impl['license'] = spec['license']
        data['spec'] = {'*-*': {
            'commands': spec.commands,
            'requires': spec.requires,
            }}
        data['unpack_size'] = unpack_size
        data['mime_type'] = 'application/vnd.olpc-sugar'

        if initial and not contexts.exists(impl['context']):
            context['guid'] = impl['context']
            context['type'] = 'activity'
            request.call(method='POST', path=['context'], content=context)
            context = None

    enforce('context' in impl, 'Context is not specified')
    enforce('version' in impl, 'Version is not specified')
    enforce(context_type in contexts.get(impl['context'])['type'],
            http.BadRequest, 'Inappropriate bundle type')
    if impl.get('license') in (None, EMPTY_LICENSE):
        existing, total = volume['implementation'].find(
                context=impl['context'], order_by='-version',
                not_layer='deleted')
        enforce(total, 'License is not specified')
        impl['license'] = next(existing)['license']

    yield impl

    existing, __ = volume['implementation'].find(
            context=impl['context'], version=impl['version'],
            not_layer='deleted')
    impl['guid'] = \
            request.call(method='POST', path=['implementation'], content=impl)
    for i in existing:
        layer = i['layer'] + ['deleted']
        volume['implementation'].update(i.guid, {'layer': layer})

    if context:
        request.call(method='PUT', path=['context', impl['context']],
                content=context)


def _load_context_metadata(bundle, spec):
    result = {}
    for prop in ('homepage', 'mime_types'):
        if spec[prop]:
            result[prop] = spec[prop]

    try:
        with bundle.extractfile(join(bundle.rootdir, spec['icon'])) as svg:
            result.update(Context.image_props(svg))
    except Exception:
        exception(_logger, 'Failed to load icon')

    msgids = {}
    for prop, confname in [
            ('title', 'name'),
            ('summary', 'summary'),
            ('description', 'description'),
            ]:
        if spec[confname]:
            msgids[prop] = spec[confname]
            result[prop] = {'en': spec[confname]}
    with toolkit.mkdtemp() as tmpdir:
        for path in bundle.get_names():
            if not path.endswith('.mo'):
                continue
            mo_path = path.strip(os.sep).split(os.sep)
            if len(mo_path) != 5 or mo_path[1] != 'locale':
                continue
            lang = mo_path[2]
            bundle.extract(path, tmpdir)
            try:
                i18n = gettext.translation(spec['context'],
                        join(tmpdir, *mo_path[:2]), [lang])
                for prop, value in msgids.items():
                    msgstr = i18n.gettext(value)
                    if msgstr != value or lang == 'en':
                        result[prop][lang] = msgstr
            except Exception:
                exception(_logger, 'Gettext failed to read %r', mo_path[-1])

    return result


def _load_pubkey(pubkey):
    pubkey = pubkey.strip()
    try:
        with toolkit.NamedTemporaryFile() as key_file:
            key_file.file.write(pubkey)
            key_file.file.flush()
            # SSH key needs to be converted to PKCS8 to ket M2Crypto read it
            pubkey_pkcs8 = toolkit.assert_call(
                    ['ssh-keygen', '-f', key_file.name, '-e', '-m', 'PKCS8'])
    except Exception:
        message = 'Cannot read DSS public key gotten for registeration'
        exception(_logger, message)
        if node.trust_users.value:
            logging.warning('Failed to read registration pubkey, '
                    'but we trust users')
            # Keep SSH key for further converting to PKCS8
            pubkey_pkcs8 = pubkey
        else:
            raise http.Forbidden(message)

    return str(hashlib.sha1(pubkey.split()[1]).hexdigest()), pubkey_pkcs8
