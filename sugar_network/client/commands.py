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
import logging
from os.path import join

from sugar_network import db, client, node, toolkit
from sugar_network.toolkit import netlink, mountpoints
from sugar_network.client import journal, clones, injector
from sugar_network.client.spec import Spec
from sugar_network.resources.volume import Volume, Commands
from sugar_network.node.slave import PersonalCommands
from sugar_network.toolkit import zeroconf, coroutine, util, http
from sugar_network.toolkit import exception, enforce


# Top-level directory name to keep SN data on mounted devices
_SN_DIRNAME = 'sugar-network'
_LOCAL_PROPS = frozenset(['favorite', 'clone'])

# If disconnect happned in more than `_RECONNECT_MINIMUM` seconds, reconnect
_RECONNECT_MINIMUM = 60

_logger = logging.getLogger('client.commands')


class ClientCommands(db.CommandsProcessor, Commands, journal.Commands):

    def __init__(self, home_volume, server_mode=False, offline=False,
            no_subscription=False):
        db.CommandsProcessor.__init__(self)
        Commands.__init__(self)
        if not client.no_dbus.value:
            journal.Commands.__init__(self)

        self._home = _VolumeCommands(home_volume)
        self._inline = coroutine.Event()
        self._remote_urls = []
        self._node = None
        self._node_job = coroutine.Pool()
        self._jobs = coroutine.Pool()
        self._static_prefix = 'http://localhost:%s' % client.ipc_port.value
        self._offline = offline
        self._no_subscription = no_subscription
        self._server_mode = server_mode

        home_volume.connect(self._home_event_cb)

        if not offline:
            if server_mode:
                mountpoints.connect(_SN_DIRNAME,
                        self._found_mount, self._lost_mount)
            else:
                if client.discover_server.value:
                    self._jobs.spawn(self._discover_node)
                else:
                    self._remote_urls.append(client.api_url.value)
                self._jobs.spawn(self._wait_for_connectivity)

    def close(self):
        self._jobs.kill()
        self._got_offline()
        self._home.volume.close()

    @db.route('GET', '/hub')
    def hub(self, request, response):
        """Serve Hub via HTTP instead of file:// for IPC users.

        Since SSE doesn't support CORS for now.

        """
        if request.environ['PATH_INFO'] == '/hub':
            raise http.Redirect('/hub/')

        path = request.path[1:]
        if not path:
            path = ['index.html']
        path = join(client.hub_root.value, *path)

        mtime = os.stat(path).st_mtime
        if request.if_modified_since >= mtime:
            raise http.NotModified()

        if path.endswith('.js'):
            response.content_type = 'text/javascript'
        if path.endswith('.css'):
            response.content_type = 'text/css'
        response.last_modified = mtime

        return file(path, 'rb')

    @db.volume_command(method='GET', cmd='status',
            mime_type='application/json')
    def status(self):
        result = {'route': 'proxy' if self._inline.is_set() else 'offline'}
        if self._inline.is_set():
            result['node'] = self._node.api_url
        return result

    @db.volume_command(method='GET', cmd='inline',
            mime_type='application/json')
    def inline(self):
        if not self._offline and not self._server_mode and \
                not self._inline.is_set():
            self._remote_connect()
        return self._inline.is_set()

    @db.volume_command(method='GET', cmd='whoami',
            mime_type='application/json')
    def whoami(self, request, response):
        try:
            result = self._node_call(request, response)
        except db.CommandNotFound:
            result = {'roles': [], 'guid': request.principal}
        return result

    @db.directory_command(method='GET',
            arguments={
                'reply': db.to_list,
                'clone': db.to_int,
                'favorite': db.to_bool,
                },
            mime_type='application/json')
    def find(self, request, response, document, reply, clone, favorite):
        if not self._inline.is_set() or clone or favorite:
            return self._home.call(request, response)
        else:
            return self._proxy_get(request, response)

    @db.document_command(method='GET',
            arguments={'reply': db.to_list}, mime_type='application/json')
    def get(self, request, response):
        return self._proxy_get(request, response)

    @db.property_command(method='GET', mime_type='application/json')
    def get_prop(self, request, response, document, guid):
        try:
            return self._node_call(request, response)
        except http.NotFound:
            if self._inline.is_set() and \
                    self._home.volume[document].exists(guid):
                # In case if user got offline guids (clone=2 requests)
                # that don't exist in online
                return self._home.call(request, response)
            else:
                raise

    @db.document_command(method='GET', cmd='make')
    def make(self, document, guid):
        enforce(document == 'context', 'Only contexts can be launched')

        for event in injector.make(guid):
            event['event'] = 'make'
            self.broadcast(event)

    @db.document_command(method='GET', cmd='launch',
            arguments={'args': db.to_list})
    def launch(self, document, guid, args, activity_id=None,
            object_id=None, uri=None, color=None, no_spawn=None):
        enforce(document == 'context', 'Only contexts can be launched')

        def do_launch():
            for event in injector.launch(guid, args,
                    activity_id=activity_id, object_id=object_id, uri=uri,
                    color=color):
                event['event'] = 'launch'
                self.broadcast(event)

        if no_spawn:
            do_launch()
        else:
            self._jobs.spawn(do_launch)

    @db.document_command(method='PUT', cmd='clone',
            arguments={
                'force': db.to_int,
                'nodeps': db.to_int,
                'requires': db.to_list,
                })
    def clone(self, request, document, guid, force):
        enforce(self._inline.is_set(), 'Not available in offline')

        if document == 'context':
            context_type = self._node_call(method='GET', document='context',
                guid=guid, prop='type')
            if 'activity' in context_type:
                self._clone_activity(guid, request)
            elif 'content' in context_type:

                def get_props():
                    impls = self._node_call(method='GET',
                            document='implementation', context=guid,
                            stability='stable', order_by='-version', limit=1,
                            reply=['guid'])['result']
                    enforce(impls, http.NotFound, 'No implementations')
                    impl_id = impls[0]['guid']
                    props = self._node_call(method='GET', document='context',
                            guid=guid, reply=['title', 'description'])
                    props['preview'] = self._node_call(method='GET',
                            document='context', guid=guid, prop='preview')
                    data_response = db.Response()
                    props['data'] = self._node_call(response=data_response,
                            method='GET', document='implementation',
                            guid=impl_id, prop='data')
                    props['mime_type'] = data_response.content_type or \
                            'application/octet'
                    props['activity_id'] = impl_id
                    return props

                self._clone_jobject(guid, request.content, get_props, force)
            else:
                raise RuntimeError('No way to clone')
        elif document == 'artifact':

            def get_props():
                props = self._node_call(method='GET', document='artifact',
                        guid=guid, reply=['title', 'description', 'context'])
                props['preview'] = self._node_call(method='GET',
                        document='artifact', guid=guid, prop='preview')
                props['data'] = self._node_call(method='GET',
                        document='artifact', guid=guid, prop='data')
                props['activity'] = props.pop('context')
                return props

            self._clone_jobject(guid, request.content, get_props, force)
        else:
            raise RuntimeError('Command is not supported for %r' % document)

    @db.document_command(method='PUT', cmd='favorite')
    def favorite(self, request, document, guid):
        if document == 'context':
            if request.content or self._home.volume['context'].exists(guid):
                self._checkin_context(guid, {'favorite': request.content})
        else:
            raise RuntimeError('Command is not supported for %r' % document)

    @db.document_command(method='GET', cmd='feed',
            mime_type='application/json')
    def feed(self, document, guid, layer, distro, request, response):
        enforce(document == 'context')

        try:
            context = self._home.volume['context'].get(guid)
        except http.NotFound:
            context = None
        if context is None or context['clone'] != 2:
            return self._node_call(request, response)

        versions = []
        for path in clones.walk(context.guid):
            try:
                spec = Spec(root=path)
            except Exception:
                exception(_logger, 'Failed to read %r spec file', path)
                continue
            versions.append({
                'guid': spec.root,
                'version': spec['version'],
                'arch': '*-*',
                'stability': 'stable',
                'commands': {
                    'activity': {
                        'exec': spec['Activity', 'exec'],
                        },
                    },
                'requires': spec.requires,
                })

        return {'name': context.get('title',
                    accept_language=request.accept_language),
                'implementations': versions,
                }

    def call(self, request, response=None):
        request.static_prefix = self._static_prefix
        request.accept_language = [toolkit.default_lang()]
        request.allow_redirects = True
        try:
            return db.CommandsProcessor.call(self, request, response)
        except db.CommandNotFound:
            return self._node_call(request, response)

    def _node_call(self, request=None, response=None, **kwargs):
        if request is None:
            request = db.Request(**kwargs)
            request.static_prefix = self._static_prefix
            request.accept_language = [toolkit.default_lang()]
            request.allow_redirects = True
        if self._inline.is_set():
            if client.layers.value and request.get('document') in \
                    ('context', 'implementation') and \
                    'layer' not in request:
                request['layer'] = client.layers.value
            return self._node.call(request, response)
        else:
            return self._home.call(request, response)

    def _got_online(self):
        enforce(not self._inline.is_set())
        self._inline.set()
        self.broadcast({'event': 'inline', 'state': 'online'})

    def _got_offline(self, initiate=False):
        if not self._inline.is_set():
            return
        self._inline.clear()
        self.broadcast({'event': 'inline', 'state': 'offline'})

    def _discover_node(self):
        for host in zeroconf.browse_workstations():
            url = 'http://%s:%s' % (host, node.port.default)
            if url not in self._remote_urls:
                self._remote_urls.append(url)
            self._remote_connect()

    def _wait_for_connectivity(self):
        for route in netlink.wait_for_route():
            self._node_job.kill()
            if route:
                self._remote_connect()

    def _remote_connect(self):

        def listen_for_events():
            while True:
                ts = time.time()
                try:
                    for event in self._node.subscribe():
                        if event.get('document') == 'implementation':
                            mtime = event.get('props', {}).get('mtime')
                            if mtime:
                                injector.invalidate_solutions(mtime)
                        self.broadcast(event)
                except Exception:
                    exception(_logger, 'Failed on subscription')
                if time.time() - ts < _RECONNECT_MINIMUM:
                    _logger.info('Subscription aborted')
                    break

        def connect():
            for url in self._remote_urls:
                self.broadcast({'event': 'inline', 'state': 'connecting'})
                try:
                    _logger.debug('Connecting to %r node', url)
                    self._node = client.Client(url)
                    info = self._node.get(cmd='info')
                    impl_info = info['documents'].get('implementation')
                    if impl_info:
                        injector.invalidate_solutions(impl_info['mtime'])
                    _logger.info('Connected to %r node', url)
                    self._got_online()
                    if self._no_subscription:
                        break
                    listen_for_events()
                except Exception:
                    exception(_logger, 'Connection to %r failed', url)
                finally:
                    if not self._no_subscription:
                        self._node.close()
                        self._got_offline()

        if not self._node_job:
            self._node_job.spawn(connect)

    def _found_mount(self, root):
        if self._inline.is_set():
            _logger.debug('Found %r node mount but %r is already active',
                    root, self._node.volume.root)
            return

        _logger.debug('Found %r node mount', root)

        db_path = join(root, _SN_DIRNAME, 'db')
        node.data_root.value = db_path
        node.stats_root.value = join(root, _SN_DIRNAME, 'stats')
        node.files_root.value = join(root, _SN_DIRNAME, 'files')

        volume = Volume(db_path, lazy_open=client.lazy_open.value)
        self._node = _PersonalCommands(join(db_path, 'node'), volume,
                self.broadcast)
        self._node.api_url = 'http://localhost:%s' % node.port.value
        self._jobs.spawn(volume.populate)

        logging.info('Start %r node on %s port', volume.root, node.port.value)
        server = coroutine.WSGIServer(('0.0.0.0', node.port.value),
                db.Router(self._node))
        self._node_job.spawn(server.serve_forever)
        volume.connect(self.broadcast)
        self._got_online()

    def _lost_mount(self, root):
        if not self._inline.is_set() or \
                not self._node.volume.root.startswith(root):
            return
        _logger.debug('Lost %r node mount', root)
        self._node_job.kill()
        self._node.volume.disconnect(self.broadcast)
        self._node.volume.close()
        self._got_offline()

    def _home_event_cb(self, event):
        if not self._inline.is_set():
            self.broadcast(event)
        elif event.get('document') == 'context' and 'props' in event:
            # Broadcast events related to proxy properties
            event_props = event['props']
            broadcast_props = event['props'] = {}
            for name in _LOCAL_PROPS:
                if name in event_props:
                    broadcast_props[name] = event_props[name]
            if broadcast_props:
                self.broadcast(event)

    def _clone_jobject(self, uid, value, get_props, force):
        if value:
            if force or not journal.exists(uid):
                self.journal_update(uid, **get_props())
                self.broadcast({'event': 'show_journal', 'uid': uid})
        else:
            if journal.exists(uid):
                self.journal_delete(uid)

    def _checkin_context(self, guid, props):
        contexts = self._home.volume['context']

        if contexts.exists(guid):
            contexts.update(guid, props)
        else:
            copy = self._node_call(method='GET', document='context', guid=guid,
                    reply=[
                        'type', 'implement', 'title', 'summary', 'description',
                        'homepage', 'mime_types', 'dependencies',
                        ])
            copy.update(props)
            copy['guid'] = guid
            contexts.create(copy)
            for prop in ('icon', 'artifact_icon', 'preview'):
                blob = self._node_call(method='GET', document='context',
                        guid=guid, prop=prop)
                if blob is not None:
                    contexts.set_blob(guid, prop, blob)

    def _clone_activity(self, guid, request):
        if not request.content:
            clones.wipeout(guid)
            return

        for __ in clones.walk(guid):
            if not request.get('force'):
                return
            break

        self._checkin_context(guid, {'clone': 1})

        if request.get('nodeps'):
            pipe = injector.clone_impl(guid,
                    stability=request.get('stability'),
                    requires=request.get('requires'))
        else:
            pipe = injector.clone(guid)

        for event in pipe:
            event['event'] = 'clone'
            self.broadcast(event)

        for __ in clones.walk(guid):
            break
        else:
            # Cloning was failed
            self._checkin_context(guid, {'clone': 0})

    def _proxy_get(self, request, response):
        document = request['document']
        mixin = None

        if self._inline.is_set() and document in ('context', 'artifact'):
            reply = request.setdefault('reply', ['guid'])
            mixin = set(reply) & _LOCAL_PROPS
            if mixin:
                # Otherwise there is no way to mixin _LOCAL_PROPS
                if 'guid' not in request and 'guid' not in reply:
                    reply.append('guid')
                if document == 'context' and 'type' not in reply:
                    reply.append('type')

        result = self._node_call(request, response)
        if not mixin:
            return result

        request_guid = request.get('guid')
        if request_guid:
            items = [result]
        else:
            items = result['result']

        def mixin_jobject(props, guid):
            if 'clone' in mixin:
                props['clone'] = 2 if journal.exists(guid) else 0
            if 'favorite' in mixin:
                props['favorite'] = bool(int(journal.get(guid, 'keep') or 0))

        if document == 'context':
            contexts = self._home.volume['context']
            for props in items:
                guid = request_guid or props['guid']
                if 'activity' in props['type']:
                    if contexts.exists(guid):
                        patch = contexts.get(guid).properties(mixin)
                    else:
                        patch = dict([(i, contexts.metadata[i].default)
                                for i in mixin])
                    props.update(patch)
                elif 'content' in props['type']:
                    mixin_jobject(props, guid)
        elif document == 'artifact':
            for props in items:
                mixin_jobject(props, request_guid or props['guid'])

        return result


class CachedClientCommands(ClientCommands):

    def __init__(self, home_volume, server_mode=False, offline=False):
        ClientCommands.__init__(self, home_volume, server_mode, offline)
        self._push_seq = util.PersistentSequence(
                join(home_volume.root, 'push.sequence'), [1, None])
        self._push_job = coroutine.Pool()

    def _got_online(self):
        ClientCommands._got_online(self)
        self._push_job.spawn(self._push)

    def _got_offline(self, initiate=False):
        self._push_job.kill()
        ClientCommands._got_offline(self, initiate)

    def _push(self):
        pushed_seq = util.Sequence()
        skiped_seq = util.Sequence()

        def push(request, seq):
            try:
                self._node.call(request)
            except Exception:
                exception(_logger, 'Cannot push %r, will postpone', request)
                skiped_seq.include(seq)
            else:
                pushed_seq.include(seq)

        for document, directory in self._home.volume.items():
            if directory.mtime <= self._push_seq.mtime:
                continue

            _logger.debug('Check %r local cache to push', document)

            for guid, patch in directory.diff(self._push_seq, layer='local'):
                diff = {}
                diff_seq = util.Sequence()
                post_requests = []
                for prop, meta, seqno in patch:
                    if 'blob' in meta:
                        request = db.Request(method='PUT', document=document,
                                guid=guid, prop=prop)
                        request.content_type = meta['mime_type']
                        request.content_length = os.stat(meta['blob']).st_size
                        request.content_stream = util.iter_file(meta['blob'])
                        post_requests.append((request, seqno))
                    elif 'url' in meta:
                        request = db.Request(method='PUT', document=document,
                            guid=guid, prop=prop)
                        request.content_type = 'application/json'
                        request.content = meta
                        post_requests.append((request, seqno))
                    else:
                        diff[prop] = meta['value']
                        diff_seq.include(seqno, seqno)
                if not diff:
                    continue
                request = db.Request(document=document)
                if 'guid' in diff:
                    request['method'] = 'POST'
                    access = db.ACCESS_CREATE | db.ACCESS_WRITE
                else:
                    request['method'] = 'PUT'
                    request['guid'] = guid
                    access = db.ACCESS_WRITE
                for name in diff.keys():
                    if not (directory.metadata[name].permissions & access):
                        del diff[name]
                request.content_type = 'application/json'
                request.content = diff
                push(request, diff_seq)
                for request, seqno in post_requests:
                    push(request, [[seqno, seqno]])

        if not pushed_seq:
            self.broadcast({'event': 'push'})
            return

        _logger.info('Pushed %r local cache', pushed_seq)

        self._push_seq.exclude(pushed_seq)
        if not skiped_seq:
            self._push_seq.stretch()
            # No any decent reasons to keep fail reports after uploding.
            # TODO The entire offlile synchronization should be improved,
            # for now, it is possible to have a race here
            self._home.volume['report'].wipe()
        self._push_seq.commit()
        self.broadcast({'event': 'push'})


class _VolumeCommands(db.VolumeCommands):

    def __init__(self, volume):
        db.VolumeCommands.__init__(self, volume)

    def before_create(self, request, props):
        props['layer'] = tuple(props['layer']) + ('local',)
        db.VolumeCommands.before_create(self, request, props)


class _PersonalCommands(PersonalCommands):

    api_url = None
