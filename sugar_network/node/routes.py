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
import time
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
from sugar_network.toolkit.router import route, preroute, postroute, ACL
from sugar_network.toolkit.router import Unauthorized, Request, fallbackroute
from sugar_network.toolkit.spec import EMPTY_LICENSE
from sugar_network.toolkit.spec import parse_requires, ensure_requires
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import pylru, http, coroutine, exception, enforce


_MAX_STAT_RECORDS = 100
_AUTH_POOL_SIZE = 1024

_logger = logging.getLogger('node.routes')


class NodeRoutes(model.VolumeRoutes, model.FrontRoutes):

    def __init__(self, guid, volume):
        model.VolumeRoutes.__init__(self, volume)
        model.FrontRoutes.__init__(self)
        volume.broadcast = self.broadcast

        self._guid = guid
        self._stats = None
        self._auth_pool = pylru.lrucache(_AUTH_POOL_SIZE)
        self._auth_config = None
        self._auth_config_mtime = 0

        if stats_node.stats_node.value:
            stats_path = join(node.stats_root.value, 'node')
            self._stats = stats_node.Sniffer(volume, stats_path)
            coroutine.spawn(self._commit_stats)

    def close(self):
        if self._stats is not None:
            self._stats.suspend()

    @property
    def guid(self):
        return self._guid

    @route('GET', cmd='logon', acl=ACL.AUTH)
    def logon(self):
        pass

    @route('GET', cmd='whoami', mime_type='application/json')
    def whoami(self, request, response):
        roles = []
        if self.authorize(request.principal, 'root'):
            roles.append('root')
        return {'roles': roles, 'guid': request.principal, 'route': 'direct'}

    @route('GET', cmd='status', mime_type='application/json')
    def status(self):
        documents = {}
        for name, directory in self.volume.items():
            documents[name] = {'mtime': directory.mtime}
        return {'guid': self._guid, 'resources': documents}

    @route('GET', cmd='stats', arguments={
                'start': int, 'end': int, 'records': int, 'source': list},
            mime_type='application/json')
    def stats(self, start, end, records, source):
        enforce(self._stats is not None, 'Node stats is disabled')
        if not source:
            return {}

        if records > _MAX_STAT_RECORDS:
            _logger.debug('Decrease %d stats records number to %d',
                    records, _MAX_STAT_RECORDS)
            records = _MAX_STAT_RECORDS
        elif records <= 0:
            records = _MAX_STAT_RECORDS / 10

        stats = {}
        for i in source:
            enforce('.' in i, 'Misnamed source')
            db_name, ds_name = i.split('.', 1)
            stats.setdefault(db_name, []).append(ds_name)

        return self._stats.report(stats, start, end, records)

    @route('POST', ['user'], mime_type='application/json')
    def register(self, request):
        # To avoid authentication while registering new user
        self.create(request)

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
            mime_type='application/json', acl=ACL.AUTH)
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
    def preroute(self, op, request, response):
        if op.acl & ACL.AUTH and request.principal is None:
            if not request.authorization:
                enforce(self.authorize(None, 'user'),
                        Unauthorized, 'No credentials')
            else:
                if request.authorization not in self._auth_pool:
                    self.authenticate(request.authorization)
                    self._auth_pool[request.authorization] = True
                enforce(not request.authorization.nonce or
                        request.authorization.nonce >= time.time(),
                        Unauthorized, 'Credentials expired')
                request.principal = request.authorization.login

        if op.acl & ACL.AUTHOR and request.guid:
            if request.resource == 'user':
                allowed = (request.principal == request.guid)
            else:
                doc = self.volume[request.resource].get(request.guid)
                allowed = (request.principal in doc['author'])
            enforce(allowed or self.authorize(request.principal, 'root'),
                    http.Forbidden, 'Operation is permitted only for authors')

        if op.acl & ACL.SUPERUSER:
            enforce(self.authorize(request.principal, 'root'), http.Forbidden,
                    'Operation is permitted only for superusers')

    @postroute
    def postroute(self, request, response, result, error):
        if error is None or isinstance(error, http.StatusPass):
            if self._stats is not None:
                self._stats.log(request)

    def on_create(self, request, props, event):
        if request.resource == 'user':
            with file(props['pubkey']['blob']) as f:
                props['guid'] = str(hashlib.sha1(f.read()).hexdigest())
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

    def authenticate(self, auth):
        enforce(auth.scheme == 'sugar', http.BadRequest,
                'Unknown authentication scheme')
        if not self.volume['user'].exists(auth.login):
            raise Unauthorized('Principal does not exist', auth.nonce)

        from M2Crypto import RSA

        data = hashlib.sha1('%s:%s' % (auth.login, auth.nonce)).digest()
        key = RSA.load_pub_key(self.volume['user'].path(auth.login, 'pubkey'))
        enforce(key.verify(data, auth.signature.decode('hex')),
                http.Forbidden, 'Bad credentials')

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
        response.meta = impl.properties([
            'guid', 'ctime', 'layer', 'author', 'tags',
            'context', 'version', 'stability', 'license', 'notes',
            ])
        response.meta['data'] = data = impl.meta('data')
        for key in ('mtime', 'seqno', 'blob'):
            if key in data:
                del data[key]
        return result


def generate_node_stats(volume, path):
    tmp_path = toolkit.mkdtemp()
    new_stats = stats_node.Sniffer(volume, tmp_path, True)
    old_stats = stats_node.Sniffer(volume, path)

    def timeline(ts):
        ts = long(ts)
        end = long(time.time())
        step = None

        archives = {}
        for rra in stats_node.stats_node_rras.value:
            a_step, a_size = [long(i) for i in rra.split(':')[-2:]]
            a_step *= stats_node.stats_node_step.value
            a_start = end - min(end, a_step * a_size)
            if archives.setdefault(a_start, a_step) > a_step:
                archives[a_start] = a_step
        archives = list(sorted(archives.items()))

        try:
            while ts <= end:
                while not step or archives and ts >= archives[0][0]:
                    archive_start, step = archives.pop(0)
                    ts = max(ts / step * step, archive_start)
                yield ts, ts + step - 1, step
                ts += step
        except GeneratorExit:
            shutil.rmtree(tmp_path, ignore_errors=True)

    start = next(volume['context'].find(limit=1, order_by='ctime')[0])['ctime']
    for left, right, step in timeline(start):
        for resource, props in [
                ('user', []),
                ('context', []),
                ('implementation', ['context']),
                ('artifact', ['context', 'type']),
                ('feedback', ['context']),
                ('solution', ['context', 'feedback']),
                ('review', ['context', 'artifact', 'rating']),
                ('report', ['context', 'implementation']),
                ('comment', ['context', 'review', 'feedback', 'solution']),
                ]:
            objs, __ = volume[resource].find(
                    query='ctime:%s..%s' % (left, right))
            for obj in objs:
                request = Request(method='POST', path=[resource],
                        content=obj.properties(props))
                new_stats.log(request)
        for resource, props in [
                ('user', ['layer']),
                ('context', ['layer']),
                ('implementation', ['layer']),
                ('artifact', ['layer']),
                ('feedback', ['layer', 'solution']),
                ('solution', ['layer']),
                ('review', ['layer']),
                ('report', ['layer']),
                ('comment', ['layer']),
                ]:
            objs, __ = volume[resource].find(
                    query='mtime:%s..%s' % (left, right))
            for obj in objs:
                if 'deleted' in obj['layer']:
                    request = Request(method='DELETE',
                            path=[resource, obj.guid])
                else:
                    request = Request(method='PUT', path=[resource, obj.guid],
                            content=obj.properties(props))
                new_stats.log(request)
        downloaded = {}
        for resource in ('context', 'artifact'):
            stats = old_stats.report(
                    {resource: ['downloaded']}, left - step, right, 1)
            if not stats.get(resource):
                continue
            stats = stats[resource][-1][1].get('downloaded')
            if stats:
                downloaded[resource] = {'downloaded': stats}
        new_stats.commit(left + (right - left) / 2, downloaded)

    new_stats.commit_objects(True)
    shutil.rmtree(path)
    shutil.move(tmp_path, path)


@contextmanager
def load_bundle(volume, request, bundle_path):
    impl = request.copy()
    initial = False
    if 'initial' in impl:
        initial = impl.pop('initial')
    data = impl.setdefault('data', {})
    contexts = volume['context']
    context = impl.get('context')
    context_meta = None
    impls = volume['implementation']

    try:
        bundle = Bundle(bundle_path, mime_type='application/zip')
    except Exception:
        _logger.debug('Load unrecognized bundle from %r', bundle_path)
        context_type = 'book'
    else:
        _logger.debug('Load Sugar Activity bundle from %r', bundle_path)
        context_type = 'activity'
        unpack_size = 0

        with bundle:
            changelog = join(bundle.rootdir, 'CHANGELOG')
            for arcname in bundle.get_names():
                if changelog and arcname == changelog:
                    with bundle.extractfile(changelog) as f:
                        impl['notes'] = f.read()
                    changelog = None
                unpack_size += bundle.getmember(arcname).size
            spec = bundle.get_spec()
            context_meta = _load_context_metadata(bundle, spec)
        if 'requires' in impl:
            spec.requires.update(parse_requires(impl.pop('requires')))

        context = impl['context'] = spec['context']
        impl['version'] = spec['version']
        impl['stability'] = spec['stability']
        if spec['license'] is not EMPTY_LICENSE:
            impl['license'] = spec['license']
        requires = impl['requires'] = []
        for dep_name, dep in spec.requires.items():
            found = False
            for version in dep.versions_range():
                requires.append('%s-%s' % (dep_name, version))
                found = True
            if not found:
                requires.append(dep_name)

        data['spec'] = {'*-*': {
            'commands': spec.commands,
            'requires': spec.requires,
            }}
        data['unpack_size'] = unpack_size
        data['mime_type'] = 'application/vnd.olpc-sugar'

        if initial and not contexts.exists(context):
            context_meta['type'] = 'activity'
            request.call(method='POST', path=['context'], content=context_meta)
            context_meta = None

    enforce(context, 'Context is not specified')
    enforce('version' in impl, 'Version is not specified')
    enforce(context_type in contexts.get(context)['type'],
            http.BadRequest, 'Inappropriate bundle type')
    if 'license' not in impl:
        existing, total = impls.find(
                context=context, order_by='-version', not_layer='deleted')
        enforce(total, 'License is not specified')
        impl['license'] = next(existing)['license']

    digest = hashlib.sha1()
    with file(bundle_path, 'rb') as f:
        while True:
            chunk = f.read(toolkit.BUFFER_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    data['digest'] = digest.hexdigest()

    yield impl

    existing, __ = impls.find(
            context=context, version=impl['version'], not_layer='deleted')
    if 'url' not in data:
        data['blob'] = bundle_path
    impl['guid'] = \
            request.call(method='POST', path=['implementation'], content=impl)
    for i in existing:
        layer = i['layer'] + ['deleted']
        impls.update(i.guid, {'layer': layer})

    if 'origin' in impls.get(impl['guid']).layer:
        diff = contexts.patch(context, context_meta)
        if diff:
            request.call(method='PUT', path=['context', context], content=diff)


def _load_context_metadata(bundle, spec):
    result = {}
    for prop in ('homepage', 'mime_types'):
        if spec[prop]:
            result[prop] = spec[prop]
    result['guid'] = spec['context']

    try:
        icon_file = bundle.extractfile(join(bundle.rootdir, spec['icon']))
        Context.populate_images(result, icon_file.read())
        icon_file.close()
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
                    msgstr = i18n.gettext(value).decode('utf8')
                    if lang == 'en' or msgstr != value:
                        result[prop][lang] = msgstr
            except Exception:
                exception(_logger, 'Gettext failed to read %r', mo_path[-1])

    return result
