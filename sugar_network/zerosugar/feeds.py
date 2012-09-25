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
from os.path import isabs

from zeroinstall.injector import model

from sugar_network.zerosugar import lsb_release, parse_version
from active_toolkit import util


clients = []

_logger = logging.getLogger('zerosugar.feeds')


def read(context):
    feed = _Feed(context)

    feed_content = None
    client = None
    for client in clients:
        try:
            feed_content = client.get(['context', context],
                    reply=['versions', 'packages'])
            _logger.debug('Found %r in %r mountpoint',
                    context, client.params['mountpoint'])
            break
        except Exception:
            util.exception(_logger,
                    'Failed to fetch %r feed from %r mountpoint',
                    context, client.params['mountpoint'])

    if feed_content is None:
        _logger.warning('No feed for %r context', context)
        return None

    distro = feed_content['packages'].get(lsb_release.distributor_id())
    if distro:
        feed.to_resolve = distro.get('binary')

    for version, version_data in feed_content['versions'].items():
        for arch, impl_data in version_data.items():
            impl_id = impl_data['guid']

            impl = _Implementation(feed, impl_id, None)
            impl.client = client
            impl.version = parse_version(version)
            impl.released = 0
            impl.arch = arch
            impl.upstream_stability = \
                    model.stability_levels[impl_data['stability']]
            impl.requires.extend(_read_requires(impl_data.get('requires')))

            if isabs(impl_id):
                impl.local_path = impl_id
            else:
                impl.add_download_source(impl_id,
                        impl_data.get('size') or 0, impl_data.get('extract'))

            for name, command in impl_data['commands'].items():
                impl.commands[name] = _Command(name, command)

            for name, insert, mode in impl_data.get('bindings') or []:
                binding = model.EnvironmentBinding(name, insert, mode=mode)
                impl.bindings.append(binding)

            feed.implementations[impl_id] = impl

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

    @property
    def name(self):
        return self.context

    @property
    def summaries(self):
        # TODO i18n
        return {}

    @property
    def first_summary(self):
        return self.context

    @property
    def descriptions(self):
        # TODO i18n
        return {}

    @property
    def first_description(self):
        return self.context

    def resolve(self, packages):
        top_package = packages[0]

        impl = _Implementation(self, self.context, None)
        impl.version = parse_version(top_package['version'])
        impl.released = 0
        impl.arch = '*-%s' % top_package['arch']
        impl.upstream_stability = model.stability_levels['packaged']
        impl.to_install = [i for i in packages if not i['installed']]

        self.implementations[self.context] = impl
        self.to_resolve = None


class _Implementation(model.ZeroInstallImplementation):

    client = None
    to_install = None

    def is_available(self, stores=None):
        return self.to_install is not None or bool(self.local_path)


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
        # TODO
        #self._requires = _read_requires(data.get('requires'))
        self._requires = []

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
