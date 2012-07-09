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
from os.path import join, isdir
from gettext import gettext as _

import active_document as ad

from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_MOVED_TO, IN_MOVED_FROM
from sugar_network import local, node
from sugar_network.toolkit import zeroconf, netlink
from sugar_network.toolkit.collection import MutableStack
from sugar_network.local.mounts import LocalMount, NodeMount
from sugar_network.node.subscribe_socket import SubscribeSocket
from sugar_network.node.commands import NodeCommands
from sugar_network.node.router import Router
from sugar_network.resources.volume import Volume
from active_toolkit import util, coroutine, enforce


_DB_DIRNAME = 'sugar-network'
_SYNC_DIRNAME = 'sugar-network-sync'

_LOCAL_PROPS = {
        'keep': False,
        'keep_impl': 0,
        'position': (-1, -1),
        }

_logger = logging.getLogger('local.mountset')


class Mountset(dict, ad.CommandsProcessor):

    def __init__(self, home_volume):
        dict.__init__(self)
        ad.CommandsProcessor.__init__(self)

        self.opened = coroutine.Event()

        self.home_volume = home_volume
        self._subscriptions = {}
        self._locale = locale.getdefaultlocale()[0].replace('_', '-')
        self._jobs = coroutine.Pool()
        self._servers = coroutine.ServersPool()
        self._sync_dirs = MutableStack()
        self._sync = coroutine.Pool()

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, _('Unknown mountpoint %r'), mountpoint)
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
                result.append({'mountpoint': path, 'name': mount.name})
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

        enforce(path or self._sync_dirs, _('No mounts to synchronize with'))

        for mount in self.values():
            if isinstance(mount, NodeMount):
                if rewind:
                    self._sync_dirs.rewind()
                self._sync.spawn(mount.sync_session, self._sync_dirs, path)
                break
        else:
            raise RuntimeError(_('No mounted servers'))

    @ad.volume_command(method='POST', cmd='break_sync')
    def break_sync(self):
        self._sync.kill()

    def call(self, request, response=None):
        if response is None:
            response = ad.Response()
        request.accept_language = [self._locale]

        try:
            return ad.CommandsProcessor.call(self, request, response)
        except ad.CommandNotFound:
            pass

        mountpoint = request.pop('mountpoint')
        mount = self[mountpoint]

        if mountpoint == '/':
            mount.set_mounted(True)
        enforce(mount.mounted, _('%r is not mounted'), mountpoint)

        try:
            result = mount.call(request, response)
        except Exception, error:
            util.exception(_logger, _('Failed to process %s on %r mount: %s'),
                    request, mountpoint, error)
            raise
        else:
            _logger.debug('Processed %s on %r mount: %r',
                    request, mountpoint, result)

        return result

    def connect(self, callback, condition=None):
        self._subscriptions[callback] = condition or {}

    def disconnect(self, callback):
        if callback in self._subscriptions:
            del self._subscriptions[callback]

    def publish(self, event):
        for callback, condition in self._subscriptions.items():
            for key, value in condition.items():
                if event.get(key) not in ('*', value):
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    util.exception(_logger, _('Failed to dispatch %r'), event)

    def open(self):
        try:
            mounts_root = local.mounts_root.value
            if mounts_root:
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
        self._servers.stop()
        self._jobs.kill()
        for mountpoint in self.keys():
            del self[mountpoint]
        if self.home_volume is not None:
            self.home_volume.close()

    def _discover_masters(self):
        with zeroconf.ServiceBrowser() as monitor:
            for host in monitor.browse():
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

    def _mounts_monitor(self):
        root = local.mounts_root.value

        _logger.info(_('Start monitoring %r for mounts'), root)
        try:
            with Inotify() as monitor:
                monitor.add_watch(root, IN_DELETE_SELF | IN_CREATE | \
                        IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
                while not monitor.closed:
                    coroutine.select([monitor.fileno()], [], [])
                    for filename, event, __ in monitor.read():
                        path = join(root, filename)
                        try:
                            if event & IN_DELETE_SELF:
                                _logger.warning(
                                    _('Lost %r, cannot monitor anymore'), root)
                                monitor.close()
                                break
                            elif event & (IN_DELETE | IN_MOVED_FROM):
                                self._lost_mount(path)
                            elif event & (IN_CREATE | IN_MOVED_TO):
                                self._found_mount(path)
                        except Exception:
                            util.exception(_logger,
                                    _('Cannot process %r mount'), path)
        finally:
            _logger.info(_('Stop monitoring %r for mounts'), root)

    def _found_mount(self, path):
        sync_path = join(path, _SYNC_DIRNAME)
        if isdir(sync_path):
            self._sync_dirs.add(sync_path)
            if self._servers:
                _logger.debug('Found sync %r mount', path)
                self.start_sync()
            else:
                _logger.debug('Found sync %r mount but no servers', path)
            return

        sn_path = join(path, _DB_DIRNAME)
        if isdir(sn_path):
            _logger.debug('Found server %r mount', path)
            if path not in self:
                volume, server_mode = self._mount_volume(sn_path)
                if server_mode:
                    self[path] = NodeMount(volume, self.home_volume)
                else:
                    self[path] = LocalMount(volume)
            return

        _logger.debug('Ignore %r mount', path)

    def _lost_mount(self, path):
        _logger.debug('Lost %r mount', path)

        self._sync_dirs.remove(join(path, _SYNC_DIRNAME))
        if not self._sync_dirs:
            self.break_sync()

        if path in self:
            del self[path]

    def _mount_volume(self, path):
        lazy_open = local.lazy_open.value
        server_mode = local.server_mode.value

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
            self._servers.spawn(coroutine.WSGIServer,
                    ('0.0.0.0', node.port.value), Router(cp))

            _logger.info('Listen for client subscribtions on %s port',
                    node.subscribe_port.value)
            self._servers.spawn(subscriber)

            # Let servers start before publishing mount event
            coroutine.dispatch()

        return volume, server_mode
