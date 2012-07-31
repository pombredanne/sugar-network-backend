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
import locale
import socket
import logging
from os.path import join, exists

import active_document as ad

from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_MOVED_TO, IN_MOVED_FROM
from sugar_network import local, node
from sugar_network.toolkit import zeroconf, netlink, network
from sugar_network.toolkit.collection import MutableStack
from sugar_network.toolkit.files_sync import Leechers
from sugar_network.local.mounts import LocalMount, NodeMount
from sugar_network.node.subscribe_socket import SubscribeSocket
from sugar_network.node.commands import NodeCommands
from sugar_network.node.router import Router
from sugar_network.resources.volume import Volume
from active_toolkit import util, coroutine, enforce


_DB_DIRNAME = '.sugar-network'
_SYNC_DIRNAME = '.sugar-network-sync'

_COMPLETE_MOUNT_TIMEOUT = 3

_logger = logging.getLogger('local.mountset')


class Mountset(dict, ad.CommandsProcessor):

    def __init__(self, home_volume, sync_dirs=None):
        dict.__init__(self)
        ad.CommandsProcessor.__init__(self)

        self.opened = coroutine.Event()

        self.home_volume = home_volume
        if sync_dirs is None:
            self._file_syncs = {}
        else:
            self._file_syncs = Leechers(sync_dirs,
                    join(home_volume.root, 'files'))
        self._subscriptions = {}
        self._locale = locale.getdefaultlocale()[0].replace('_', '-')
        self._jobs = coroutine.Pool()
        self._servers = coroutine.Pool()
        self._sync_dirs = MutableStack()
        self._sync = coroutine.Pool()

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
            if path == '/' or mount.mounted:
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
        return mount.mounted

    @ad.volume_command(method='POST', cmd='start_sync')
    def start_sync(self, rewind=False, path=None):
        if self._sync:
            return

        enforce(path or self._sync_dirs, 'No mounts to synchronize with')

        for mount in self.values():
            if isinstance(mount, NodeMount):
                if rewind:
                    self._sync_dirs.rewind()
                self._sync.spawn(mount.sync_session, self._sync_dirs, path)
                break
        else:
            raise RuntimeError('No mounted servers')

    @ad.volume_command(method='POST', cmd='break_sync')
    def break_sync(self):
        self._sync.kill()

    @ad.volume_command(method='POST', cmd='publish')
    def republish(self, request):
        self.publish(request.content)

    def call(self, request, response=None):
        if response is None:
            response = ad.Response()
        request.accept_language = [self._locale]
        mountpoint = None

        def process_call():
            try:
                return ad.CommandsProcessor.call(self, request, response)
            except ad.CommandNotFound:
                mountpoint = request.pop('mountpoint')
                mount = self[mountpoint]
                if mountpoint == '/':
                    mount.set_mounted(True)
                enforce(mount.mounted, '%r is not mounted', mountpoint)
                return mount.call(request, response)

        try:
            result = process_call()
        except Exception:
            util.exception(_logger,
                    'Failed to call %s on %r', request, mountpoint)
            raise
        else:
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
            mounts_root = local.mounts_root.value
            if mounts_root:
                for filename in os.listdir(mounts_root):
                    self._found_mount(join(mounts_root, filename))
                # In case if sync mounts processed before server mounts
                # TODO More obvious code
                for filename in os.listdir(mounts_root):
                    self._found_mount(join(mounts_root, filename))
                self._jobs.spawn(self._mounts_monitor)

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

    def _mounts_monitor(self):
        root = local.mounts_root.value
        _logger.info('Start monitoring %r for mounts', root)

        with Inotify() as monitor:
            monitor.add_watch(root, IN_DELETE_SELF | IN_CREATE | \
                    IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
            while not monitor.closed:
                coroutine.select([monitor.fileno()], [], [])
                for filename, event, __ in monitor.read():
                    path = join(root, filename)
                    try:
                        if event & IN_DELETE_SELF:
                            _logger.warning('Lost %r, cannot monitor anymore',
                                    root)
                            monitor.close()
                            break
                        elif event & (IN_DELETE | IN_MOVED_FROM):
                            self._lost_mount(path)
                        elif event & (IN_CREATE | IN_MOVED_TO):
                            # Right after moutning, access to directory
                            # might be restricted; let system enough time
                            # to complete mounting
                            coroutine.sleep(_COMPLETE_MOUNT_TIMEOUT)
                            self._found_mount(path)
                    except Exception:
                        util.exception(_logger, 'Mount %r failed', path)

    def _found_mount(self, path):
        if exists(join(path, _DB_DIRNAME)) and path not in self:
            _logger.debug('Found %r server mount', path)
            volume, server_mode = self._mount_volume(path)
            if server_mode:
                self[path] = NodeMount(volume, self.home_volume,
                        self._file_syncs)
            else:
                self[path] = LocalMount(volume)

        if exists(join(path, _SYNC_DIRNAME)):
            self._sync_dirs.add(path)
            if self._servers:
                _logger.debug('Found %r sync mount', path)
                self.start_sync()
            else:
                _logger.debug('Found %r sync mount but no servers', path)

    def _lost_mount(self, path):
        _logger.debug('Lost %r mount', path)

        self._sync_dirs.remove(join(path, _SYNC_DIRNAME))
        if not self._sync_dirs:
            self.break_sync()

        if path in self:
            del self[path]

    def _mount_volume(self, path):
        lazy_open = local.lazy_open.value
        server_mode = local.server_mode.value and exists(join(path, 'node'))

        if server_mode:
            if self._servers:
                _logger.warning('Do not start server for %r, ' \
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
