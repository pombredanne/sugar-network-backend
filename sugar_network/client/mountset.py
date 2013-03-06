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
import socket
import logging
from os.path import join

from sugar_network import db, client, node
from sugar_network.toolkit import netlink, mountpoints, router
from sugar_network.toolkit import coroutine, util, enforce
from sugar_network.client import journal, zeroconf
from sugar_network.client.mounts import LocalMount, NodeMount
from sugar_network.zerosugar import clones, injector
from sugar_network.resources.volume import Volume, Commands


_DB_DIRNAME = '.sugar-network'

_logger = logging.getLogger('client.mountset')


class Mountset(dict, db.CommandsProcessor, Commands, journal.Commands):

    def __init__(self, home_volume):
        self.opened = coroutine.Event()
        self._jobs = coroutine.Pool()
        self.node_mount = None

        dict.__init__(self)
        db.CommandsProcessor.__init__(self)
        Commands.__init__(self)
        if not client.no_dbus.value:
            journal.Commands.__init__(self)
        self.volume = home_volume

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, 'Unknown mountpoint %r', mountpoint)
        return self.get(mountpoint)

    def __setitem__(self, mountpoint, mount):
        dict.__setitem__(self, mountpoint, mount)
        mount.mountpoint = mountpoint
        mount.broadcast = self.broadcast
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

    @db.volume_command(method='GET', cmd='mounts',
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

    @db.volume_command(method='GET', cmd='mounted',
            mime_type='application/json')
    def mounted(self, mountpoint):
        mount = self.get(mountpoint)
        if mount is None:
            return False
        if mountpoint == '/':
            mount.set_mounted(True)
        return mount.mounted.is_set()

    @db.document_command(method='GET', cmd='make')
    def make(self, mountpoint, document, guid):
        enforce(document == 'context', 'Only contexts can be launched')

        for event in injector.make(mountpoint, guid):
            event['event'] = 'make'
            self.broadcast(event)

    @db.document_command(method='GET', cmd='launch',
            arguments={'args': db.to_list})
    def launch(self, mountpoint, document, guid, args, activity_id=None,
            object_id=None, uri=None, color=None, no_spawn=None):
        enforce(document == 'context', 'Only contexts can be launched')

        def do_launch():
            for event in injector.launch(mountpoint, guid, args,
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
    def clone(self, request, mountpoint, document, guid, force):
        mount = self[mountpoint]

        if document == 'context':
            context_type = mount(method='GET', document='context', guid=guid,
                    prop='type')
            if 'activity' in context_type:
                self._clone_activity(mountpoint, guid, request)
            elif 'content' in context_type:

                def get_props():
                    impls = mount(method='GET', document='implementation',
                            context=guid, stability='stable',
                            order_by='-version', limit=1,
                            reply=['guid'])['result']
                    enforce(impls, db.NotFound, 'No implementations')
                    impl_id = impls[0]['guid']
                    props = mount(method='GET', document='context', guid=guid,
                            reply=['title', 'description'])
                    props['preview'] = mount(method='GET', document='context',
                            guid=guid, prop='preview')
                    data_response = db.Response()
                    props['data'] = mount(data_response, method='GET',
                            document='implementation', guid=impl_id,
                            prop='data')
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

    @db.document_command(method='PUT', cmd='favorite')
    def favorite(self, request, mountpoint, document, guid):
        if document == 'context':
            if request.content or self.volume['context'].exists(guid):
                self._checkin_context(guid, {'favorite': request.content})
        else:
            raise RuntimeError('Command is not supported for %r' % document)

    @db.volume_command(method='GET', cmd='whoami',
            mime_type='application/json')
    def whoami(self, request):
        result = self['/'].call(request)
        result['route'] = 'proxy'
        return result

    def super_call(self, request, response):
        mount = self[request.mountpoint]
        return mount.call(request, response)

    def call(self, request, response=None):
        request.accept_language = [db.default_lang()]
        request.mountpoint = request.get('mountpoint')
        if not request.mountpoint:
            request.mountpoint = request['mountpoint'] = '/'
        try:
            return db.CommandsProcessor.call(self, request, response)
        except db.CommandNotFound:
            return self.super_call(request, response)

    def open(self):
        try:
            mountpoints.connect(_DB_DIRNAME,
                    self._found_mount, self._lost_mount)
            if '/' in self and not client.server_mode.value:
                if client.discover_server.value:
                    crawler = self._discover_server
                else:
                    crawler = self._wait_for_server
                self._jobs.spawn(crawler)
        finally:
            self.opened.set()

    def close(self):
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
                util.res_init()

    def _found_mount(self, path):
        volume = Volume(path, lazy_open=client.lazy_open.value)
        self._jobs.spawn(volume.populate)
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

    def _clone_jobject(self, uid, value, get_props, force):
        if value:
            if force or not journal.exists(uid):
                self.journal_update(uid, **get_props())
                self.broadcast({'event': 'show_journal', 'uid': uid})
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

    def _clone_activity(self, mountpoint, guid, request):
        if not request.content:
            clones.wipeout(guid)
            return

        for __ in clones.walk(guid):
            if not request.get('force'):
                return
            break

        self._checkin_context(guid, {'clone': 1})

        if request.get('nodeps'):
            impls = self[mountpoint](method='GET', document='implementation',
                    context=guid, stability=request.get('stability'),
                    requires=request.get('requires'),
                    order_by='-version', limit=1,
                    reply=['guid', 'spec'])['result']
            enforce(impls, db.NotFound, 'No implementations')
            pipe = injector.clone_impl(mountpoint, guid, **impls[0])
        else:
            pipe = injector.clone(mountpoint, guid)

        for event in pipe:
            event['event'] = 'clone'
            self.broadcast(event)

        for __ in clones.walk(guid):
            break
        else:
            # Cloning was failed
            self._checkin_context(guid, {'clone': 0})
