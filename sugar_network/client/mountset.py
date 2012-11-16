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
import socket
import logging
from os.path import join, exists
from gettext import gettext as _

import active_document as ad

from sugar_network import client, node
from sugar_network.toolkit import netlink, network, mountpoints, router
from sugar_network.client import journal, zeroconf
from sugar_network.client.mounts import LocalMount, NodeMount
from sugar_network.node.commands import NodeCommands
from sugar_network.node.sync_node import SyncCommands
from sugar_network.zerosugar import clones, injector
from sugar_network.resources.volume import Volume, Commands
from active_toolkit import util, coroutine, enforce


_DB_DIRNAME = '.sugar-network'

_logger = logging.getLogger('client.mountset')


class Mountset(dict, ad.CommandsProcessor, Commands, journal.Commands,
        SyncCommands):

    def __init__(self, home_volume):
        self.opened = coroutine.Event()
        self._subscriptions = {}
        self._lang = ad.default_lang()
        self._jobs = coroutine.Pool()
        self._servers = coroutine.Pool()

        dict.__init__(self)
        ad.CommandsProcessor.__init__(self, home_volume)
        SyncCommands.__init__(self, client.path('sync'))
        Commands.__init__(self)
        journal.Commands.__init__(self)

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, 'Unknown mountpoint %r', mountpoint)
        return self.get(mountpoint)

    def __setitem__(self, mountpoint, mount):
        dict.__setitem__(self, mountpoint, mount)
        mount.mountpoint = mountpoint
        mount.publisher = self.publish
        mount.set_mounted(True)

    def __delitem__(self, mountpoint):
        mount = self[mountpoint]
        mount.set_mounted(False)
        dict.__delitem__(self, mountpoint)

    @router.route('GET', '/hub')
    def hub(self, request, response):
        """Serve Hub via HTTP instead of file:// for IPC users.

        Since SSE doesn't support CORS for now.

        """
        if request.environ['PATH_INFO'] == '/hub':
            raise router.Redirect('/hub/')

        path = request.path[1:]
        if not path:
            path = ['index.html']
        path = join(client.hub_root.value, *path)

        mtime = os.stat(path).st_mtime
        if request.if_modified_since >= mtime:
            raise router.NotModified()

        if path.endswith('.js'):
            response.content_type = 'text/javascript'
        if path.endswith('.css'):
            response.content_type = 'text/css'
        response.last_modified = mtime

        return router.stream_reader(file(path, 'rb'))

    @ad.volume_command(method='GET', cmd='mounts',
            mime_type='application/json')
    def mounts(self):
        result = []
        for path, mount in self.items():
            if path == '/' or mount.mounted.is_set():
                result.append({
                    'mountpoint': path,
                    'name': mount.name,
                    'private': mount.private,
                    })
        return result

    @ad.volume_command(method='GET', cmd='mounted',
            mime_type='application/json')
    def mounted(self, mountpoint):
        mount = self.get(mountpoint)
        if mount is None:
            return False
        if mountpoint == '/':
            mount.set_mounted(True)
        return mount.mounted.is_set()

    @ad.volume_command(method='POST', cmd='publish')
    def publish(self, event, request=None):
        if request is not None:
            event = request.content

        for callback, condition in self._subscriptions.items():
            for key, value in condition.items():
                if event.get(key) != value:
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    util.exception(_logger, 'Failed to dispatch %r', event)

    @ad.document_command(method='GET', cmd='make')
    def make(self, mountpoint, document, guid):
        enforce(document == 'context', 'Only contexts can be launched')

        for event in injector.make(mountpoint, guid):
            event['event'] = 'make'
            self.publish(event)

    @ad.document_command(method='GET', cmd='launch',
            arguments={'args': ad.to_list})
    def launch(self, mountpoint, document, guid, args, activity_id=None,
            object_id=None, uri=None, color=None, no_spawn=None):
        enforce(document == 'context', 'Only contexts can be launched')

        def do_launch():
            for event in injector.launch(mountpoint, guid, args,
                    activity_id=activity_id, object_id=object_id, uri=uri,
                    color=color):
                event['event'] = 'launch'
                self.publish(event)

        if no_spawn:
            do_launch()
        else:
            self._jobs.spawn(do_launch)

    @ad.document_command(method='PUT', cmd='clone',
            arguments={'force': ad.to_int})
    def clone(self, request, mountpoint, document, guid, force):
        mount = self[mountpoint]

        if document == 'context':
            context_type = mount(method='GET', document='context', guid=guid,
                    prop='type')
            if 'activity' in context_type:
                self._clone_activity(mountpoint, guid, request.content, force)
            elif 'content' in context_type:

                def get_props():
                    impls = mount(method='GET', document='implementation',
                            context=guid, stability='stable',
                            order_by='-version', limit=1,
                            reply=['guid'])['result']
                    enforce(impls, ad.NotFound, 'No implementations')
                    impl_id = impls[0]['guid']
                    props = mount(method='GET', document='context', guid=guid,
                            reply=['title', 'description'])
                    props['preview'] = mount(method='GET', document='context',
                            guid=guid, prop='preview')
                    data_response = ad.Response()
                    props['data'] = mount.call(
                            router.Request(method='GET',
                                document='implementation', guid=impl_id,
                                prop='data'),
                            data_response)
                    props['mime_type'] = data_response.content_type or \
                            'application/octet'
                    props['activity_id'] = impl_id
                    return props

                self._clone_jobject(guid, request.content, get_props, force)
            else:
                raise RuntimeError('No way to clone')
        elif document == 'artifact':

            def get_props():
                props = mount(method='GET', document='artifact', guid=guid,
                        reply=['title', 'description', 'context'])
                props['preview'] = mount(method='GET', document='artifact',
                        guid=guid, prop='preview')
                props['data'] = mount(method='GET', document='artifact',
                        guid=guid, prop='data')
                props['activity'] = props.pop('context')
                return props

            self._clone_jobject(guid, request.content, get_props, force)
        else:
            raise RuntimeError('Command is not supported for %r' % document)

    @ad.document_command(method='PUT', cmd='favorite')
    def favorite(self, request, mountpoint, document, guid):
        if document == 'context':
            if request.content or self.volume['context'].exists(guid):
                self._checkin_context(guid, {'favorite': request.content})
        else:
            raise RuntimeError('Command is not supported for %r' % document)

    @ad.volume_command(method='GET', cmd='whoami',
            mime_type='application/json')
    def whoami(self, request):
        result = self['/'].call(request)
        result['route'] = 'proxy'
        return result

    def super_call(self, request, response):
        mount = self[request.mountpoint]
        if request.mountpoint == '/':
            mount.set_mounted(True)
        enforce(mount.mounted.is_set(), '%r is unmounted', request.mountpoint)
        return mount.call(request, response)

    def call(self, request, response=None):
        request.accept_language = [self._lang]
        request.mountpoint = request.get('mountpoint')
        if not request.mountpoint:
            request.mountpoint = request['mountpoint'] = '/'
        try:
            return ad.CommandsProcessor.call(self, request, response)
        except ad.CommandNotFound:
            return self.super_call(request, response)

    def connect(self, callback, condition=None, **kwargs):
        self._subscriptions[callback] = condition or kwargs

    def disconnect(self, callback):
        if callback in self._subscriptions:
            del self._subscriptions[callback]

    def open(self):
        try:
            mountpoints.connect(_DB_DIRNAME,
                    self._found_mount, self._lost_mount)
            if '/' in self:
                if client.api_url.value:
                    crawler = self._wait_for_server
                else:
                    crawler = self._discover_server
                self._jobs.spawn(crawler)
        finally:
            self.opened.set()

    def close(self):
        self.break_sync()
        self._servers.kill()
        self._jobs.kill()
        for mountpoint in self.keys():
            del self[mountpoint]
        if self.volume is not None:
            self.volume.close()

    def _discover_server(self):
        for host in zeroconf.browse_workstations():
            url = 'http://%s:%s' % (host, node.port.default)
            self['/'].mount(url)

    def _wait_for_server(self):
        with netlink.Netlink(socket.NETLINK_ROUTE, netlink.RTMGRP_IPV4_ROUTE |
                netlink.RTMGRP_IPV6_ROUTE | netlink.RTMGRP_NOTIFY) as monitor:
            while True:
                self['/'].mount(client.api_url.value)
                coroutine.select([monitor.fileno()], [], [])
                message = monitor.read()
                if message is None:
                    break
                # Otherwise, `socket.gethostbyname()` will return stale resolve
                network.res_init()

    def _found_mount(self, path):
        volume, server_mode = self._mount_volume(path)
        if server_mode:
            _logger.debug('Mount %r in node mode', path)
            self[path] = self.node_mount = NodeMount(volume, self.volume)
        else:
            _logger.debug('Mount %r in node-less mode', path)
            self[path] = LocalMount(volume)

    def _lost_mount(self, path):
        mount = self.get(path)
        if mount is None:
            return
        _logger.debug('Lost %r mount', path)
        if isinstance(mount, NodeMount):
            self.node_mount = None
        del self[path]

    def _mount_volume(self, path):
        lazy_open = client.lazy_open.value
        server_mode = client.server_mode.value and exists(join(path, 'node'))

        if server_mode:
            if self._servers:
                _logger.warning('Do not start server for %r, '
                        'server already started', path)
                server_mode = False
            else:
                lazy_open = False

        volume = Volume(path, lazy_open=lazy_open)
        self._jobs.spawn(volume.populate)

        if server_mode:
            _logger.info('Start %r server on %s port',
                    volume.root, node.port.value)
            server = coroutine.WSGIServer(('0.0.0.0', node.port.value),
                    router.Router(NodeCommands(volume)))
            self._servers.spawn(server.serve_forever)

            # Let servers start before publishing mount event
            coroutine.dispatch()

        return volume, server_mode

    def _clone_jobject(self, uid, value, get_props, force):
        if value:
            if force or not journal.exists(uid):
                self.journal_update(uid, **get_props())
        else:
            if journal.exists(uid):
                self.journal_delete(uid)

    def _checkin_context(self, guid, props):
        contexts = self.volume['context']

        if contexts.exists(guid):
            contexts.update(guid, props)
            return

        if not [i for i in props.values() if i is not None]:
            return

        mount = self['/']
        copy = mount(method='GET', document='context', guid=guid,
                reply=[
                    'type', 'implement', 'title', 'summary', 'description',
                    'homepage', 'mime_types', 'dependencies',
                    ])
        props.update(copy)
        props['guid'] = guid
        contexts.create(props)

        for prop in ('icon', 'artifact_icon', 'preview'):
            blob = mount(method='GET',
                    document='context', guid=guid, prop=prop)
            if blob:
                contexts.set_blob(guid, prop, blob)

    def _clone_activity(self, mountpoint, guid, value, force):
        if not value:
            clones.wipeout(guid)
            return

        for __ in clones.walk(guid):
            if not force:
                return
            break

        self._checkin_context(guid, {'clone': 1})

        for event in injector.clone(mountpoint, guid):
            # TODO Publish clone progress
            if event['state'] == 'failure':
                self.publish({
                    'event': 'alert',
                    'mountpoint': mountpoint,
                    'severity': 'error',
                    'message': _('Fail to clone %s') % guid,
                    })

        for __ in clones.walk(guid):
            break
        else:
            # Cloning was failed
            self._checkin_context(guid, {'clone': 0})
