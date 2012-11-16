# Copyright (C) 2010-2012 Aleksey Lim
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

import sys
import logging
from os.path import isabs, join, abspath, dirname

from sugar_network import IPCClient
from sugar_network.zerosugar import packagekit, lsb_release
from sugar_network.zerosugar.spec import parse_version
from active_toolkit import util

sys.path.insert(0, join(abspath(dirname(__file__)), 'zeroinstall-injector'))

from zeroinstall.injector import reader, model, distro
from zeroinstall.injector.config import Config
from zeroinstall.injector.driver import Driver
from zeroinstall.injector.requirements import Requirements


def Interface_init(self, url):
    self.uri = url
    self.reset()


model.Interface.__init__ = Interface_init
reader.load_feed_from_cache = lambda url, * args, ** kwargs: _load_feed(url)
reader.check_readable = lambda * args, ** kwargs: True

try_cleanup_distro_version = distro.try_cleanup_distro_version
canonical_machine = distro.canonical_machine

_logger = logging.getLogger('zeroinstall')
_mountpoints = None
_client = None


def solve(mountpoint, context):
    global _mountpoints, _client

    _mountpoints = [mountpoint]
    if mountpoint != '~':
        _mountpoints.append('~')
    if mountpoint != '/':
        _mountpoints.append('/')

    _client = IPCClient()
    try:
        requirement = Requirements(context)
        # TODO
        requirement.command = 'activity'
        return _solve(requirement)
    finally:
        _client.close()


def _solve(requirement):
    config = Config()
    driver = Driver(config, requirement)
    driver.solver.record_details = True

    while True:
        driver.solver.solve(requirement.interface_uri,
                driver.target_arch, command_name=requirement.command)
        if driver.solver.ready:
            break

        packaged_feeds = []
        to_resolve = []

        for url in driver.solver.feeds_used:
            feed = config.iface_cache.get_feed(url)
            if feed is not None and feed.to_resolve:
                packaged_feeds.append(feed)
                to_resolve.extend(feed.to_resolve)

        if not to_resolve:
            break

        resolved = packagekit.resolve(to_resolve)
        for feed in packaged_feeds:
            feed.resolve([resolved[i] for i in feed.to_resolve])

    _logger.debug('\n'.join(
        ['Solve results:'] +
        ['  %s: %s' % (k.uri, v) for k, v in driver.solver.details.items()]))
    selections = driver.solver.selections.selections

    if not driver.solver.ready:
        # pylint: disable-msg=W0212
        reason = driver.solver._failure_reason
        if not reason:
            missed = [iface for iface, impl in
                    selections.items() if impl is None]
            reason = 'Cannot find implementations for %s' % ', '.join(missed)
        raise RuntimeError(reason)

    solution = []
    solution.append(_impl_new(
        config,
        requirement.interface_uri,
        selections[requirement.interface_uri],
        ))
    for iface, sel in selections.items():
        if sel is not None and iface != requirement.interface_uri:
            solution.append(_impl_new(config, iface, sel))

    return solution


def _impl_new(config, iface, sel):
    feed = config.iface_cache.get_feed(iface)
    impl = {'id': sel.id,
            'context': iface,
            'version': sel.version,
            'name': feed.name,
            }
    if isabs(sel.id):
        impl['spec'] = join(sel.id, 'activity', 'activity.info')
    if not feed.packaged:
        impl['mountpoint'] = feed.mountpoint
    if sel.local_path:
        impl['path'] = sel.local_path
    if sel.impl.to_install:
        impl['install'] = sel.impl.to_install
    if sel.impl.download_sources:
        prefix = sel.impl.download_sources[0].extract
        if prefix:
            impl['prefix'] = prefix
    commands = sel.get_commands()
    if commands:
        impl['command'] = commands.values()[0].path.split()
    return impl


def _load_feed(context):
    feed = _Feed(context)

    mountpoint = None
    feed_content = None
    for mountpoint in _mountpoints:
        try:
            feed_content = _client.get(['context', context],
                    reply=['title', 'packages', 'versions', 'dependencies'],
                    mountpoint=mountpoint)
            _logger.debug('Found %r in %r mountpoint', context, mountpoint)
            break
        except Exception:
            util.exception(_logger,
                    'Failed to fetch %r feed from %r mountpoint',
                    context, mountpoint)

    if feed_content is None:
        _logger.warning('No feed for %r context', context)
        return None

    feed.mountpoint = mountpoint
    feed.name = feed_content['title']

    distr = feed_content['packages'].get(lsb_release.distributor_id())
    if distr:
        feed.to_resolve = distr.get('binary')

    for release in feed_content['versions']:
        impl_id = release['guid']

        impl = _Implementation(feed, impl_id, None)
        impl.version = parse_version(release['version'])
        impl.released = 0
        impl.arch = release['arch']
        impl.upstream_stability = model.stability_levels[release['stability']]
        for i in feed_content['dependencies']:
            impl.requires.append(_Dependency(i, {}))
        impl.requires.extend(_read_requires(release.get('requires')))

        if isabs(impl_id):
            impl.local_path = impl_id
        else:
            impl.add_download_source(impl_id,
                    release.get('size') or 0, release.get('extract'))

        for name, command in release['commands'].items():
            impl.commands[name] = _Command(name, command)

        for name, insert, mode in release.get('bindings') or []:
            binding = model.EnvironmentBinding(name, insert, mode=mode)
            impl.bindings.append(binding)

        feed.implementations[impl_id] = impl

    if not feed.to_resolve and not feed.implementations:
        _logger.debug('No implementations for %r', context)

    return feed


class _Feed(model.ZeroInstallFeed):
    # pylint: disable-msg=E0202

    def __init__(self, context):
        self.context = context
        self.local_path = None
        self.implementations = {}
        self.last_modified = None
        self.feeds = []
        self.metadata = []
        self.last_checked = None
        self.to_resolve = None
        self._package_implementations = []

        self.packaged = False
        self.mountpoint = None

    @property
    def url(self):
        return self.context

    @property
    def feed_for(self):
        return set([self.context])

    def resolve(self, packages):
        self.packaged = True
        top_package = packages[0]

        impl = _Implementation(self, self.context, None)
        impl.version = parse_version(top_package['version'])
        impl.released = 0
        impl.arch = '*-%s' % top_package['arch']
        impl.upstream_stability = model.stability_levels['packaged']
        impl.to_install = [i for i in packages if not i['installed']]
        impl.add_download_source(self.context, 0, None)

        self.implementations[self.context] = impl
        self.to_resolve = None


class _Implementation(model.ZeroInstallImplementation):

    to_install = None


class _Dependency(model.InterfaceDependency):

    def __init__(self, guid, data):
        self._importance = data.get('importance', model.Dependency.Essential)
        self._metadata = {}
        self.qdom = None
        self.interface = guid
        self.restrictions = []
        self.bindings = []

        for not_before, before in data.get('restrictions') or []:
            restriction = model.VersionRangeRestriction(
                    not_before=parse_version(not_before),
                    before=parse_version(before))
            self.restrictions.append(restriction)

    @property
    def context(self):
        return self.interface

    @property
    def metadata(self):
        return self._metadata

    @property
    def importance(self):
        return self._importance

    def get_required_commands(self):
        return []

    @property
    def command(self):
        pass


class _Command(model.Command):

    def __init__(self, name, data):
        self.qdom = None
        self.name = name
        self._path = data['exec']
        self._requires = _read_requires(data.get('requires'))

    @property
    def path(self):
        return self._path

    @property
    def requires(self):
        return self._requires

    def get_runner(self):
        pass

    def __str__(self):
        return ''

    @property
    def bindings(self):
        return []


def _read_requires(data):
    result = []
    for guid, dep_data in (data or {}).items():
        result.append(_Dependency(guid, dep_data))
    return result


if __name__ == '__main__':
    from pprint import pprint
    logging.basicConfig(level=logging.DEBUG)
    pprint(solve(*sys.argv[1:]))
