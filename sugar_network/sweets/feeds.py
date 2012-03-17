# Copyright (C) 2011-2012, Aleksey Lim
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

import re
from gettext import gettext as _

from zeroinstall.injector import model

import sugar_network as client
from sugar_network.util import enforce


_VERSION_RE = re.compile('(\s*(>=|<|=)\s*([.0-9]*[0-9]))')


def load(context):
    feed = _Feed(context)

    for src in client.Implementation.find(context=context):
        enforce(src['stability'] in model.stability_levels,
                _('Unknown stability "%s" for %s implementation'),
                src['stability'], src['guid'])

        stability = model.stability_levels[src['stability']]
        requires = []

        for guid, props in src['feed'].get('requires', {}).items():
            dep = _Dependency(guid,
                props.get('importance', model.Dependency.Essential))
            dep.restrictions.extend(
                    _parse_version(props.get('constraints') or ''))
            requires.append(dep)

        impl_id = src['guid']
        impl = model.ZeroInstallImplementation(feed, impl_id, None)
        impl.version = model.parse_version(src['version'])
        impl.released = src['date']
        impl.arch = '*-*'
        impl.upstream_stability = stability
        impl.commands['run'] = _Command()
        impl.requires.extend(requires)
        impl.add_download_source(impl_id, 0, None)

        feed.implementations[impl_id] = impl

    return feed


def _parse_version(args):
    result = []
    line = ''.join(args)

    while line:
        match = _VERSION_RE.match(line)
        if match is None:
            break
        word, relation, version = match.groups()
        line = line[len(word):]
        if relation == '>=':
            before = None
            not_before = version
        elif relation == '<':
            before = version
            not_before = None
        elif relation == '=':
            not_before = version
            parts = version.split('.')
            before = '.'.join(parts[:-1] + [str(int(parts[-1]) + 1)])
        else:
            continue
        result.append(model.VersionRangeRestriction(
            not_before=model.parse_version(not_before),
            before=model.parse_version(before)))

    return result


class _Feed(model.ZeroInstallFeed):

    def __init__(self, context):
        self.local_path = None
        self.implementations = {}
        self.name = context
        self.summaries = {}	# { lang: str }
        self.first_summary = context
        self.descriptions = {}	# { lang: str }
        self.first_description = None
        self.last_modified = None
        self.feeds = []
        self.feed_for = set([context])
        self.metadata = []
        self.last_checked = None
        self._package_implementations = []
        self.url = context


class _Dependency(model.InterfaceDependency):

    def __init__(self, guid, importance):
        self._importance = importance
        self._metadata = {}
        self.qdom = None
        self.interface = guid
        self.restrictions = []
        self.bindings = []

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

    def __init__(self):
        self.qdom = None

    @property
    def path(self):
        return ''

    @property
    def requires(self):
        return []

    def get_runner(self):
        pass

    def __str__(self):
        return ''

    @property
    def bindings(self):
        return []
