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

import socket
import logging
from os.path import join, exists

import active_document as ad

from sugar_network import local, node
from sugar_network.toolkit import zeroconf, netlink, network, mounts_monitor
from sugar_network.local.mounts import LocalMount, NodeMount
from sugar_network.node.subscribe_socket import SubscribeSocket
from sugar_network.node.commands import NodeCommands
from sugar_network.node.sync_node import SyncCommands
from sugar_network.node.router import Router
from sugar_network.resources.volume import Volume
from active_toolkit import util, coroutine, enforce


_DB_DIRNAME = '.sugar-network'

_logger = logging.getLogger('local.mountset')


class Mountset(dict, ad.CommandsProcessor, SyncCommands):

    def __init__(self, home_volume):
        dict.__init__(self)
        ad.CommandsProcessor.__init__(self)
        SyncCommands.__init__(self, local.path('sync'))

        self.opened = coroutine.Event()
        self.home_volume = home_volume
        self._subscriptions = {}
        self._lang = ad.default_lang()
        self._jobs = coroutine.Pool()
        self._servers = coroutine.Pool()

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

    @ad.volume_command(method='GET', cmd='mounts')
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

    @ad.volume_command(method='GET', cmd='mounted')
    def mounted(self, mountpoint):
        mount = self.get(mountpoint)
        if mount is None:
            return False
        if mountpoint == '/':
            mount.set_mounted(True)
        return mount.mounted.is_set()

    @ad.volume_command(method='PUT', cmd='checkin')
    def checkin(self, mountpoint, request):
        mount = self.get(mountpoint)
        enforce(mount is not None, 'No such mountpoint')
        mount.mounted.wait()

        for guid in (request.content or '').split():
            _logger.info('Checkin %r context', guid)
            mount.call(
                    ad.Request(method='PUT', document='context', guid=guid,
                        accept_language=[self._lang],
                        content={'keep_impl': 2, 'keep': False}),
                    ad.Response())

    @ad.volume_command(method='PUT', cmd='keep')
    def keep(self, mountpoint, request):
        mount = self.get(mountpoint)
        enforce(mount is not None, 'No such mountpoint')
        mount.mounted.wait()

        for guid in (request.content or '').split():
            _logger.info('Keep %r context', guid)
            mount.call(
                    ad.Request(method='PUT', document='context', guid=guid,
                        accept_language=[self._lang],
                        content={'keep': True}),
                    ad.Response())

    @ad.volume_command(method='GET', cmd='requires')
    def requires(self, mountpoint, context):
        mount = self.get(mountpoint)
        enforce(mount is not None, 'No such mountpoint')
        mount.mounted.wait()

        requires = set()

        for guid in [context] if isinstance(context, basestring) else context:
            feed = mount.call(
                    ad.Request(method='GET', document='context', guid=guid,
                        prop='feed', accept_language=[self._lang]),
                    ad.Response())
            for impls in feed.values():
                for impl in impls.values():
                    if 'requires' in impl:
                        requires.update(impl['requires'].keys())

        return list(requires)

    @ad.volume_command(method='POST', cmd='publish')
    def republish(self, request):
        self.publish(request.content)

    def call(self, request, response=None):
        if response is None:
            response = ad.Response()
        request.accept_language = [self._lang]
        mountpoint = request.get('mountpoint')

        try:
            try:
                result = ad.CommandsProcessor.call(self, request, response)
            except ad.CommandNotFound:
                enforce('mountpoint' in request, 'No \'mountpoint\' argument')
                request.pop('mountpoint')
                mount = self[mountpoint]
                if mountpoint == '/':
                    mount.set_mounted(True)
                enforce(mount.mounted.is_set(),
                        '%r is not mounted', mountpoint)
                result = mount.call(request, response)
        except Exception:
            util.exception(_logger, 'Failed to call %s on %r',
                    request, mountpoint)
            raise

        _logger.debug('Called %s on %r: %r', request, mountpoint, result)
        return result

    def connect(self, callback, condition=None, **kwargs):
        self._subscriptions[callback] = condition or kwargs

    def disconnect(self, callback):
        if callback in self._subscriptions:
            del self._subscriptions[callback]

    def publish(self, event):
        for callback, condition in self._subscriptions.items():
            for key, value in condition.items():
                if event.get(key) != value:
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    util.exception(_logger, 'Failed to dispatch %r', event)

    def open(self):
        try:
            mounts_monitor.connect(_DB_DIRNAME,
                    self._found_mount, self._lost_mount)
            if '/' in self:
                if local.api_url.value:
                    crawler = self._wait_for_master
                else:
                    crawler = self._discover_masters
                self._jobs.spawn(crawler)
        finally:
            self.opened.set()

    def close(self):
        self.break_sync()
        self._servers.kill()
        self._jobs.kill()
        for mountpoint in self.keys():
            del self[mountpoint]
        if self.home_volume is not None:
            self.home_volume.close()

    def _discover_masters(self):
        for host in zeroconf.browse_workstations():
            url = 'http://%s:%s' % (host, node.port.default)
            self['/'].mount(url)

    def _wait_for_master(self):
        with netlink.Netlink(socket.NETLINK_ROUTE, netlink.RTMGRP_IPV4_ROUTE |
                netlink.RTMGRP_IPV6_ROUTE | netlink.RTMGRP_NOTIFY) as monitor:
            while True:
                self['/'].mount(local.api_url.value)
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
            self[path] = self.node_mount = NodeMount(volume, self.home_volume)
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
        lazy_open = local.lazy_open.value
        server_mode = local.server_mode.value and exists(join(path, 'node'))

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
            subscriber = SubscribeSocket(volume,
                    node.host.value, node.subscribe_port.value)
            cp = NodeCommands(volume, subscriber)

            _logger.info('Start %r server on %s port',
                    volume.root, node.port.value)
            server = coroutine.WSGIServer(('0.0.0.0', node.port.value),
                    Router(cp))
            self._servers.spawn(server.serve_forever)
            self._servers.spawn(subscriber.serve_forever)

            # Let servers start before publishing mount event
            coroutine.dispatch()

        return volume, server_mode
