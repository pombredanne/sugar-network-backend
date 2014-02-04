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

# pylint: disable-msg=W0611,F0401,W0201,E1101,W0232

import sys
import logging
from os.path import isabs, join, dirname

from sugar_network.client import packagekit
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.spec import parse_version
from sugar_network.toolkit import http, lsb_release

sys.path.insert(0, join(dirname(__file__), '..', 'lib', 'zeroinstall'))

from zeroinstall.injector import reader, model, arch as _arch
from zeroinstall.injector.config import Config
from zeroinstall.injector.driver import Driver
from zeroinstall.injector.requirements import Requirements
from zeroinstall.injector.arch import machine_ranks
from zeroinstall.injector.distro import try_cleanup_distro_version


_SUGAR_API_COMPATIBILITY = {
        '0.94': frozenset(['0.86', '0.88', '0.90', '0.92', '0.94']),
        }

model.Interface.__init__ = lambda *args: _interface_init(*args)
reader.check_readable = lambda *args, **kwargs: True
reader.update_from_cache = lambda *args, **kwargs: None
reader.load_feed_from_cache = lambda url, **kwargs: _load_feed(url)

_logger = logging.getLogger('solver')
_stability = None
_call = None


def canonicalize_machine(arch):
    if arch in ('noarch', 'all'):
        return None
    return _arch.canonicalize_machine(arch)


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


def solve(call, context, stability):
    global _call, _stability

    _call = call
    _stability = stability

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
                        status = call(method='GET', cmd='whoami')
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
    _logger.debug('[%s] Solving results:\n%s',
            context, '\n'.join(top_summary + dep_summary))

    if not ready:
        # pylint: disable-msg=W0212
        reason_exception = solver.get_failure_reason()
        if reason_exception is not None:
            reason = reason_exception.message
        else:
            reason = 'Cannot find releases for %s' % ', '.join(missed)
        raise http.NotFound(reason)

    solution = []
    solution.append(_impl_new(config, context, selections[context]))
    for iface, sel in selections.items():
        if sel is not None and iface != context:
            solution.append(_impl_new(config, iface, sel))

    return solution


def _interface_init(self, url):
    self.uri = url
    self.reset()


def _impl_new(config, iface, sel):
    impl = sel.impl.sn_impl
    impl['context'] = iface
    if sel.local_path:
        impl['path'] = sel.local_path
    if sel.impl.to_install:
        impl['install'] = sel.impl.to_install
    return impl


def _load_feed(context):
    feed = _Feed(context)

    if context == 'sugar':
        try:
            from jarabe import config
            host_version = '.'.join(config.version.split('.', 2)[:2])
        except ImportError:
            # XXX sweets-sugar binding might be not sourced
            host_version = '0.94'
        for version in _SUGAR_API_COMPATIBILITY.get(host_version) or []:
            feed.implement_sugar(version)
        feed.name = context
        return feed

    releases = None
    try:
        releases = _call(method='GET', path=['context', context, 'releases'])
        _logger.trace('[%s] Found feed: %r', context, releases)
    except http.ServiceUnavailable:
        _logger.trace('[%s] Failed to fetch the feed', context)
        raise
    except Exception:
        _logger.exception('[%s] Failed to fetch the feed', context)
        return None

    """
    for digest, release in releases:
        if [i for i in release['author'].values()
                if i['role'] & ACL.ORIGINAL] and \
            release['stability'] == _stability and \
            f





                stability=_stability,
                distro=lsb_release.distributor_id())
    """

    for impl in feed_content['releases']:
        feed.implement(impl)



    # XXX 0install fails on non-ascii `name` values
    feed.name = context
    feed.to_resolve = feed_content.get('packages')
    if not feed.to_resolve:
        _logger.trace('[%s] No compatible packages', context)


    if not feed.to_resolve and not feed.implementations:
        _logger.trace('[%s] No releases', context)

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

        impl = _Release(self, self.context, None)
        impl.version = parse_version(top_package['version'])
        impl.released = 0
        impl.arch = '*-%s' % (top_package['arch'] or '*')
        impl.upstream_stability = model.stability_levels['packaged']
        impl.to_install = [i for i in packages if not i['installed']]
        impl.add_download_source(self.context, 0, None)
        impl.sn_impl = {
                'guid': self.context,
                'license': None,
                'version': top_package['version'],
                'stability': 'packaged',
                }

        self.implementations[self.context] = impl

    def implement(self, release):
        impl_id = release['guid']
        spec = release['data']['spec']['*-*']

        impl = _Release(self, impl_id, None)
        impl.version = parse_version(release['version'])
        impl.released = 0
        impl.arch = '*-*'
        impl.upstream_stability = model.stability_levels['stable']
        impl.license = release.get('license') or []
        impl.requires = _read_requires(spec.get('requires'))
        impl.requires.extend(_read_requires(release.get('requires')))
        impl.sn_impl = release

        if isabs(impl_id):
            impl.local_path = impl_id
        else:
            impl.add_download_source(impl_id, 0, None)

        for name, command in spec['commands'].items():
            impl.commands[name] = _Command(name, command)

        for name, insert, mode in spec.get('bindings') or []:
            binding = model.EnvironmentBinding(name, insert, mode=mode)
            impl.bindings.append(binding)

        self.implementations[impl_id] = impl

    def implement_sugar(self, sugar_version):
        impl_id = 'sugar-%s' % sugar_version
        impl = _Release(self, impl_id, None)
        impl.version = parse_version(sugar_version)
        impl.released = 0
        impl.arch = '*-*'
        impl.upstream_stability = model.stability_levels['packaged']
        self.implementations[impl_id] = impl
        impl.sn_impl = {
                'guid': impl_id,
                'license': None,
                'version': sugar_version,
                'stability': 'packaged',
                }


class _Release(model.ZeroInstallImplementation):

    to_install = None
    sn_impl = None
    license = None

    def is_available(self, stores):
        # Simplify solving
        return True


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

    def __init__(self, name, command):
        self.qdom = None
        self.name = name
        self._requires = _read_requires(command.get('requires'))

    @property
    def path(self):
        return 'doesnt_matter'

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
