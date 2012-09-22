# Copyright (C) 2011-2012 Aleksey Lim
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

import logging

from zeroinstall.injector.config import Config
from zeroinstall.injector.driver import Driver

from sugar_network.zerosugar import packagekit
from active_toolkit import enforce


DEEP = 2

_logger = logging.getLogger('zerosugar.solution')


def solve(req):
    config = Config()
    driver = Driver(config, req)
    driver.solver.record_details = True

    while True:
        driver.solver.solve(req.interface_uri,
                driver.target_arch, command_name=req.command)
        if driver.solver.ready:
            break

        missed = []
        packaged_feeds = []
        to_resolve = []

        for url in driver.solver.feeds_used:
            feed = config.iface_cache.get_feed(url)
            if feed is None:
                missed.append(url)
            elif feed.to_resolve:
                packaged_feeds.append(feed)
                to_resolve.extend(feed.to_resolve)

        enforce(not missed, 'Cannot find feed(s) for %s', ', '.join(missed))
        if not to_resolve:
            break

        resolved = packagekit.resolve(to_resolve)
        for feed in packaged_feeds:
            feed.resolve([resolved[i] for i in feed.to_resolve])

    _logger.debug('\n'.join(
        ['Solve results:'] +
        ['  %s: %s' % (k.uri, v) for k, v in driver.solver.details.items()]))

    if not driver.solver.ready:
        # pylint: disable-msg=W0212
        reason = driver.solver._failure_reason
        if not reason:
            missed = [iface.uri for iface, impl in
                    driver.solver.selections.items() if impl is None]
            reason = 'Cannot find implementations for %s' % ', '.join(missed)
        raise RuntimeError(reason)

    return Solution(driver.solver.selections)


class Solution(object):

    interface = property(lambda self: self.value.interface)
    selections = property(lambda self: self.value.selections)

    def __init__(self, value):
        self.value = value

    def __getitem__(self, url):
        selection = self.selections.get(url)
        if selection is not None:
            return _SelectedImpl(selection)

    def __iter__(self):
        for i in self.selections.values():
            yield _SelectedImpl(i)

    def __hash__(self):
        return hash(self.id)

    def __cmp__(self, other):
        return cmp(self.id, other.id)

    @property
    def id(self):
        return (self.interface, tuple([i.path for i in self.commands]))

    @property
    def top(self):
        return self[self.interface]

    @property
    def commands(self):
        if self.top is None:
            return []
        else:
            return self.value.commands

    def walk(self, reverse=False, depth=DEEP, uniq=True, include_top=True):
        done = set()

        def process_node(interface, parent_dep, extra_deps, path):
            if uniq:
                if interface in done:
                    return
                done.add(interface)

            sel = self[interface]
            if sel is None:
                yield _SelectedImpl(None, interface), parent_dep, path
                return

            if reverse:
                if include_top or interface != self.interface:
                    yield sel, parent_dep, path

            if _is_shallow(len(path) + 1, depth):
                for dep in sel.dependencies + extra_deps:
                    for i in process_node(dep.interface,
                            dep, [], path + [sel]):
                        yield i

            if not reverse:
                if include_top or interface != self.interface:
                    yield sel, parent_dep, path

        extra_deps = []
        for i in self.commands:
            extra_deps += i.requires

        return process_node(self.interface, None, extra_deps, [])


class _SelectedImpl(object):

    def __init__(self, orig, interface=None):
        self._value = orig
        self._installed = None
        self._to_install = None
        self._interface = interface

    def __repr__(self):
        return self.interface

    @property
    def nil(self):
        return self._value is None

    @property
    def interface(self):
        if self._value is None:
            return self._interface
        else:
            return self['interface']

    @property
    def dependencies(self):
        return self._value.dependencies

    @property
    def local_path(self):
        return self._value.impl.local_path

    @local_path.setter
    def local_path(self, value):
        self._value.impl.local_path = value

    def __getattr__(self, name):
        return getattr(self._value.impl, name)

    def __contains__(self, key):
        return key in self._value.attrs

    def __getitem__(self, key):
        return self._value.attrs.get(key)

    def __setitem__(self, key, value):
        self._value.attrs[key] = value

    def __hash__(self):
        return hash(self['id'])

    def __cmp__(self, other):
        return cmp(self['id'], other['id'])


def _is_shallow(node_depth, max_depth=None):
    if max_depth is None:
        max_depth = DEEP
    max_depth = max(0, max_depth)
    return node_depth <= max_depth or max_depth >= DEEP
