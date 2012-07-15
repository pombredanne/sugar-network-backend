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
from os.path import join, isdir, exists

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
        enforce(mount.mounted, '%r is not mounted', mountpoint)

        try:
            result = mount.call(request, response)
        except Exception, error:
            util.exception(_logger, 'Failed to process %s on %r mount: %s',
                    request, mountpoint, error)
            raise
        else:
            _logger.debug('Processed %s on %r mount: %r',
                    request, mountpoint, result)

        return result

    def connect(self, callback, condition=None, **kwargs):
        self._subscriptions[callback] = condition or kwargs

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
                    util.exception(_logger, 'Failed to dispatch %r', event)

    def open(self):
        try:
            mounts_root = local.mounts_root.value
            if mounts_root:
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
        roots = []
        with _Inotify(self._found_mount, self._lost_mount) as monitor:
            roots.append(_MountRoot(monitor, local.mounts_root.value))
            while True:
                coroutine.select([monitor.fileno()], [], [])
                if monitor.closed:
                    break
                for filename, event, cb in monitor.read():
                    try:
                        cb(filename, event)
                    except Exception:
                        util.exception('Cannot dispatch 0x%X event ' \
                                'for %r mount', event, filename)

    def _found_mount(self, root, dirnames):
        if _DB_DIRNAME in dirnames and root not in self:
            _logger.debug('Found %r server mount', root)
            volume, server_mode = self._mount_volume(join(root, _DB_DIRNAME))
            if server_mode:
                self[root] = NodeMount(volume, self.home_volume)
            else:
                self[root] = LocalMount(volume)

        if _SYNC_DIRNAME in dirnames:
            _logger.debug('Found %r sync mount', root)
            self._sync_dirs.add(join(root, _SYNC_DIRNAME))
            if self._servers:
                self.start_sync()

    def _lost_mount(self, root, dirnames):
        if _SYNC_DIRNAME in dirnames:
            _logger.debug('Lost %r sync mount', root)
            self._sync_dirs.remove(join(root, _SYNC_DIRNAME))
            if not self._sync_dirs:
                self.break_sync()

        if _DB_DIRNAME in dirnames and root in self:
            _logger.debug('Lost %r server mount', root)
            del self[root]

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
            self._servers.spawn(coroutine.WSGIServer,
                    ('0.0.0.0', node.port.value), Router(cp))

            _logger.info('Listen for client subscribtions on %s port',
                    node.subscribe_port.value)
            self._servers.spawn(subscriber)

            # Let servers start before publishing mount event
            coroutine.dispatch()

        return volume, server_mode


class _Inotify(Inotify):

    def __init__(self, found_cb, lost_cb):
        Inotify.__init__(self)
        self.found_cb = found_cb
        self.lost_cb = lost_cb


class _MountRoot(object):

    def __init__(self, monitor, path):
        self.path = path
        self._monitor = monitor
        self._nodes = {}

        _logger.info('Start monitoring %r for mounts', self.path)

        monitor.add_watch(self.path,
                IN_DELETE_SELF | IN_CREATE | IN_DELETE | \
                        IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        for filename in os.listdir(self.path):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _MountDir(monitor, path)

    def __watch_cb(self, filename, event):
        if event & IN_DELETE_SELF:
            _logger.warning('Lost ourselves, cannot monitor anymore')
            self._nodes.clear()

        elif event & (IN_CREATE | IN_MOVED_TO):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _MountDir(self._monitor, path)

        elif event & (IN_DELETE | IN_MOVED_FROM):
            item = self._nodes.get(filename)
            if item is not None:
                item.unlink()
                del self._nodes[filename]


class _MountDir(object):

    def __init__(self, monitor, root):
        self._root = root
        self._monitor = monitor
        self._found = set([i for i in os.listdir(root) \
                if isdir(join(root, i))])

        _logger.debug('Start monitoring %r mount', root)

        self._wd = monitor.add_watch(root,
                IN_CREATE | IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)
        if self._found:
            monitor.found_cb(self._root, self._found)

    def unlink(self):
        if self._found:
            self._monitor.lost_cb(self._root, self._found)
            self._found.clear()
        _logger.debug('Stop monitoring %r mount', self._root)
        self._monitor.rm_watch(self._wd)

    def __watch_cb(self, filename, event):
        path = join(self._root, filename)

        if event & (IN_CREATE | IN_MOVED_TO) and isdir(path):
            if filename not in self._found:
                _logger.debug('Found %r mount directory', path)
                self._found.add(filename)
                self._monitor.found_cb(self._root, [filename])

        elif event & (IN_DELETE | IN_MOVED_FROM):
            if filename in self._found:
                _logger.debug('Lost %r mount directory', path)
                self._found.remove(filename)
                self._monitor.lost_cb(self._root, [filename])
