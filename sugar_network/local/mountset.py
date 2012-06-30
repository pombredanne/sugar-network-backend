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
from sugar_network.local.mounts import LocalProxyMount
from sugar_network.node.subscribe_socket import SubscribeSocket
from sugar_network.node.commands import NodeCommands
from sugar_network.node.router import Router
from active_toolkit import util, coroutine, enforce


_DB_DIRECTORY = '.network'

_LOCAL_PROPS = {
        'keep': False,
        'keep_impl': 0,
        'position': (-1, -1),
        }

_logger = logging.getLogger('local.mountset')


class Mountset(dict):

    def __init__(self, home_volume):
        self.home_volume = home_volume
        self._subscriptions = {}
        self._locale = locale.getdefaultlocale()[0].replace('_', '-')
        self._jobs = coroutine.Pool()

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

    def call(self, request, response=None):
        if request.get('cmd') == 'mounts':
            return [path for path, mount in self.items() if mount.mounted]

        mountpoint = request.pop('mountpoint')
        mount = self[mountpoint]
        if mountpoint == '/':
            mount.set_mounted(True)
        if request.get('cmd') == 'mounted':
            return mount.mounted
        enforce(mount.mounted, _('%r is not mounted'), mountpoint)

        if response is None:
            response = ad.Response()
        request.accept_language = [self._locale]

        try:
            result = mount.call(request, response)
        except Exception, error:
            util.exception(_logger, _('Failed to process %s: %s'),
                    request, error)
            raise
        else:
            _logger.debug('Processed %s: %r', request, result)

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
        if local.mounts_root.value:
            self._jobs.spawn(_MountsCrawler, self)
        if '/' in self:
            if local.api_url.value:
                crawler = self._wait_for_master
            else:
                crawler = self._discover_masters
            self._jobs.spawn(crawler)

    def close(self):
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


class _MountsCrawler(object):

    def __init__(self, mountset):
        self._mountset = mountset
        self._root = local.mounts_root.value
        self._volumes = {}
        self._jobs = coroutine.Pool()
        self._servers = coroutine.ServersPool()

        self._jobs.spawn(self._populate)

        _logger.info(_('Start monitoring %r for mounts'), self._root)
        try:
            self._dispatch()
        finally:
            _logger.info(_('Stop monitoring %r for mounts'), self._root)
            self._servers.stop()
            self._jobs.kill()

    def _dispatch(self):
        with Inotify() as monitor:
            monitor.add_watch(self._root, IN_DELETE_SELF | IN_CREATE | \
                    IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
            while not monitor.closed:
                coroutine.select([monitor.fileno()], [], [])
                for filename, event, __ in monitor.read():
                    path = join(self._root, filename)
                    try:
                        if event & IN_DELETE_SELF:
                            _logger.warning(
                                _('Lost ourselves, cannot monitor anymore'))
                            monitor.close()
                            break
                        elif event & (IN_DELETE | IN_MOVED_FROM):
                            self._lost(path)
                        elif event & (IN_CREATE | IN_MOVED_TO):
                            self._found(path)
                    except Exception:
                        util.exception(_('Cannot process %r directoryr'), path)

    def _populate(self):
        for filename in os.listdir(self._root):
            self._found(join(self._root, filename))

    def _found(self, path):
        db_path = join(path, _DB_DIRECTORY)
        if not isdir(db_path) or path in self._volumes:
            return

        _logger.debug('Found %r mount', path)

        volume = self._mount_volume(db_path)
        self._mountset[path] = LocalProxyMount(volume,
                self._mountset.home_volume)
        self._volumes[path] = volume

    def _lost(self, path):
        if path not in self._volumes:
            return

        _logger.debug('Lost %r mount', path)

        self._volumes.pop(path)
        del self._mountset[path]

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

        volume = ad.SingleVolume(path, node.DOCUMENTS, lazy_open=lazy_open)
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

        return volume
