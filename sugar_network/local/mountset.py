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
from sugar_network.node.commands import NodeCommands
from sugar_network.node.router import Router
from sugar_network.node.sync_node import SyncCommands
from sugar_network.resources.volume import Volume, Commands, Request
from active_toolkit import util, coroutine, enforce


_DB_DIRNAME = '.sugar-network'

_logger = logging.getLogger('local.mountset')


class Mountset(dict, ad.CommandsProcessor, Commands, SyncCommands):

    def __init__(self, home_volume):
        self.opened = coroutine.Event()
        self._subscriptions = {}
        self._lang = ad.default_lang()
        self._jobs = coroutine.Pool()
        self._servers = coroutine.Pool()

        dict.__init__(self)
        ad.CommandsProcessor.__init__(self, home_volume)
        SyncCommands.__init__(self, local.path('sync'))
        Commands.__init__(self)

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

    @ad.volume_command(method='PUT', cmd='checkin')
    def checkin(self, mountpoint, request):
        mount = self.get(mountpoint)
        enforce(mount is not None, 'No such mountpoint')
        mount.mounted.wait()

        for guid in (request.content or '').split():
            _logger.info('Checkin %r context', guid)
            mount.call(
                    Request(method='PUT', document='context', guid=guid,
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
                    Request(method='PUT', document='context', guid=guid,
                        accept_language=[self._lang],
                        content={'keep': True}),
                    ad.Response())

    def super_call(self, request, response):
        if 'mountpoint' in request:
            mountpoint = request.mountpoint = request.pop('mountpoint')
        else:
            mountpoint = '/'
        mount = self[mountpoint]
        if mountpoint == '/':
            mount.set_mounted(True)
        enforce(mount.mounted.is_set(), '%r is not mounted', mountpoint)
        return mount.call(request, response)

    def call(self, request, response=None):
        if response is None:
            response = ad.Response()
        request.accept_language = [self._lang]
        try:
            return ad.CommandsProcessor.call(self, request, response)
        except ad.CommandNotFound:
            return self.super_call(request, response)

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
        if self.volume is not None:
            self.volume.close()

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
            _logger.info('Start %r server on %s port',
                    volume.root, node.port.value)
            server = coroutine.WSGIServer(('0.0.0.0', node.port.value),
                    Router(NodeCommands(volume)))
            self._servers.spawn(server.serve_forever)

            # Let servers start before publishing mount event
            coroutine.dispatch()

        return volume, server_mode
