# Copyright (C) 2010-2013 Aleksey Lim
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
from os.path import isabs, join, dirname

from sugar_network.client import packagekit, SUGAR_API_COMPATIBILITY
from sugar_network.toolkit import http, util, lsb_release, pipe, exception

sys.path.insert(0, join(dirname(__file__), '..', 'lib', 'zeroinstall'))

from zeroinstall.injector import reader, model
from zeroinstall.injector.config import Config
from zeroinstall.injector.driver import Driver
from zeroinstall.injector.requirements import Requirements
from zeroinstall.injector.arch import canonicalize_machine, machine_ranks
# pylint: disable-msg=W0611
from zeroinstall.injector.distro import try_cleanup_distro_version


def _interface_init(self, url):
    self.uri = url
    self.reset()


model.Interface.__init__ = _interface_init
reader.check_readable = lambda * args, ** kwargs: True
reader.update_from_cache = lambda * args, ** kwargs: None

_logger = logging.getLogger('zeroinstall')


def select_architecture(arches):
    """Select most appropriate, for the host system, machine architecture

    :param arches:
        list of architecture names to select
    :returns:
        one of passed architecture names, or, `None` if not any

    """
    result_rank = 9999
    result_arch = None
    for arch in arches:
        rank = machine_ranks.get(canonicalize_machine(arch))
        if rank is not None and rank < result_rank:
            result_rank = rank
            result_arch = arch
    return result_arch


def solve(conn, context):
    reader.load_feed_from_cache = lambda url, *args, **kwargs: \
            _load_feed(conn, url)

    req = Requirements(context)
    # TODO
    req.command = 'activity'
    config = Config()
    driver = Driver(config, req)
    solver = driver.solver
    solver.record_details = True
    status = None
    ready = False

    while True:
        solver.solve(context, driver.target_arch, command_name=req.command)
        if ready and solver.ready:
            break
        ready = solver.ready

        resolved = None
        for url in solver.feeds_used:
            feed = config.iface_cache.get_feed(url)
            if feed is None:
                continue
            while feed.to_resolve:
                try:
                    resolved = packagekit.resolve(feed.to_resolve.pop(0))
                except Exception, error:
                    if feed.to_resolve:
                        continue
                    if status is None:
                        status = conn.get(cmd='status')
                    if status['route'] == 'offline':
                        raise http.ServiceUnavailable(str(error))
                    else:
                        raise
                feed.resolve(resolved.values())
                feed.to_resolve = None
        if not resolved:
            break

    selections = solver.selections.selections
    missed = []

    top_summary = []
    dep_summary = []
    for iface, impls in solver.details.items():
        summary = (top_summary if iface.uri == context else dep_summary)
        summary.append(iface.uri)
        if impls:
            sel = selections.get(iface.uri)
            for impl, reason in impls:
                if not reason and sel is None:
                    reason = 'wrong version'
                    missed.append(iface.uri)
                if reason:
                    reason = '(%s)' % reason
                summary.append('%s v%s %s' % (
                    '*' if sel is not None and sel.impl is impl else ' ',
                    impl.get_version(),
                    reason or '',
                    ))
        else:
            summary.append('  (no versions)')
            missed.append(iface.uri)
    pipe.trace('\n  '.join(['Solving results:'] + top_summary + dep_summary))

    if not ready:
        # pylint: disable-msg=W0212
        reason_exception = solver.get_failure_reason()
        if reason_exception is not None:
            reason = reason_exception.message
        else:
            reason = 'Cannot find implementations for %s' % ', '.join(missed)
        raise RuntimeError(reason)

    solution = []
    solution.append(_impl_new(config, context, selections[context]))
    for iface, sel in selections.items():
        if sel is not None and iface != context:
            solution.append(_impl_new(config, iface, sel))

    return solution


def _impl_new(config, iface, sel):
    feed = config.iface_cache.get_feed(iface)
    impl = {'id': sel.id,
            'context': iface,
            'version': sel.version,
            'name': feed.name,
            'stability': sel.impl.upstream_stability.name,
            }
    if isabs(sel.id):
        impl['spec'] = join(sel.id, 'activity', 'activity.info')
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


def _load_feed(conn, context):
    feed = _Feed(context)

    if context == 'sugar':
        try:
            # pylint: disable-msg=F0401
            from jarabe import config
            host_versin = '.'.join(config.version.split('.', 2)[:2])
            for version in SUGAR_API_COMPATIBILITY.get(host_versin) or []:
                feed.implement_sugar(version)
            feed.name = context
            return feed
        except ImportError:
            pass

    feed_content = None
    try:
        feed_content = conn.get(['context', context], cmd='feed',
                # TODO stability='stable'
                distro=lsb_release.distributor_id())
        pipe.trace('Found %s feed: %r', context, feed_content)
    except http.ServiceUnavailable:
        pipe.trace('Failed to fetch %s feed', context)
        raise
    except Exception:
        exception(_logger, 'Failed to fetch %r feed', context)
        pipe.trace('No feeds for %s', context)
        return None

    # XXX 0install fails on non-ascii name
    feed.name = feed_content['name'].encode('ascii', 'backslashreplace')
    feed.to_resolve = feed_content.get('packages')
    if not feed.to_resolve:
        pipe.trace('No compatible packages for %s', context)
    for impl in feed_content['implementations']:
        feed.implement(impl)
    if not feed.to_resolve and not feed.implementations:
        pipe.trace('No implementations for %s', context)

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

    @property
    def url(self):
        return self.context

    @property
    def feed_for(self):
        return set([self.context])

    def resolve(self, packages):
        top_package = packages[0]

        impl = _Implementation(self, self.context, None)
        impl.version = util.parse_version(top_package['version'])
        impl.released = 0
        impl.arch = '*-%s' % top_package['arch']
        impl.upstream_stability = model.stability_levels['packaged']
        impl.to_install = [i for i in packages if not i['installed']]
        impl.add_download_source(self.context, 0, None)

        self.implementations[self.context] = impl

    def implement(self, release):
        impl_id = release['guid']

        impl = _Implementation(self, impl_id, None)
        impl.version = util.parse_version(release['version'])
        impl.released = 0
        impl.arch = release['arch']
        impl.upstream_stability = model.stability_levels[release['stability']]
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

        self.implementations[impl_id] = impl

    def implement_sugar(self, sugar_version):
        impl_id = 'sugar-%s' % sugar_version
        impl = _Implementation(self, impl_id, None)
        impl.version = util.parse_version(sugar_version)
        impl.released = 0
        impl.arch = '*-*'
        impl.upstream_stability = model.stability_levels['packaged']
        impl.local_path = '/'
        self.implementations[impl_id] = impl


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
                    not_before=util.parse_version(not_before),
                    before=util.parse_version(before))
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
    pipe.trace = logging.info
    pprint(solve(*sys.argv[1:]))
