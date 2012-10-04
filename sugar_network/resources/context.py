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

from os.path import join

import active_document as ad
from sugar_network import resources, static
from sugar_network.resources.volume import Resource
from sugar_network.node import obs
from active_toolkit import coroutine, util


class Context(Resource):

    @ad.active_property(prefix='T', full_text=True,
            typecast=[resources.CONTEXT_TYPES])
    def type(self, value):
        return value

    @ad.active_property(prefix='M',
            full_text=True, default=[], typecast=[])
    def implement(self, value):
        return value

    @ad.active_property(prefix='S', full_text=True, localized=True)
    def title(self, value):
        return value

    @ad.active_property(prefix='R', full_text=True, localized=True)
    def summary(self, value):
        return value

    @ad.active_property(prefix='D', full_text=True, localized=True)
    def description(self, value):
        return value

    @ad.active_property(prefix='H', default='', full_text=True)
    def homepage(self, value):
        return value

    @ad.active_property(prefix='Y', default=[], typecast=[], full_text=True)
    def mime_types(self, value):
        return value

    @ad.active_property(ad.BlobProperty, mime_type='image/png')
    def icon(self, value):
        if value:
            return value
        if 'package' in self['type']:
            return ad.PropertyMeta(
                    url='/static/images/package.png',
                    path=join(static.PATH, 'images', 'package.png'),
                    mime_type='image/png')
        else:
            return ad.PropertyMeta(
                    url='/static/images/missing.png',
                    path=join(static.PATH, 'images', 'missing.png'),
                    mime_type='image/png')

    @ad.active_property(ad.BlobProperty, mime_type='image/svg+xml')
    def artifact_icon(self, value):
        if value:
            return value
        return ad.PropertyMeta(
                url='/static/images/missing.svg',
                path=join(static.PATH, 'images', 'missing.svg'),
                mime_type='image/svg+xml')

    @ad.active_property(ad.BlobProperty, mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        return ad.PropertyMeta(
                url='/static/images/missing.png',
                path=join(static.PATH, 'images', 'missing.png'),
                mime_type='image/png')

    @ad.active_property(prefix='K', typecast=bool, default=False)
    def keep(self, value):
        return value

    @ad.active_property(prefix='L', typecast=[0, 1, 2], default=0)
    def keep_impl(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=[int], default=(-1, -1))
    def position(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=[], default=[])
    def dependencies(self, value):
        """Software dependencies.

        This is a transition method how to improve dependencies handling.
        The regular way should be setting up them in activity.info instead.

        """
        return value

    @ad.active_property(ad.StoredProperty, typecast=dict, default={})
    def aliases(self, value):
        return value

    @aliases.setter
    def aliases(self, value):
        coroutine.spawn(self._process_aliases, value)
        return value

    @ad.active_property(ad.StoredProperty, typecast=dict, default={})
    def packages(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=dict, default={})
    def presolve(self, value):
        return value

    def _process_aliases(self, aliases):
        packages = {}
        for repo in obs.get_repos():
            alias = aliases.get(repo['distributor_id'])
            if not alias or '*' not in alias:
                continue
            alias = alias['*'].copy()
            try:
                to_resolve = alias.get('binary', []) + \
                        alias.get('devel', [])
                if to_resolve:
                    for arch in repo['arches']:
                        obs.resolve(repo['name'], arch, to_resolve)
                    alias['status'] = 'success'
                else:
                    alias['status'] = 'no packages to resolve'
            except Exception, error:
                util.exception('Failed to resolve %r', alias)
                alias = {'status': str(error)}
            packages[repo['name']] = alias

        presolve = {}
        for repo in obs.get_presolve_repos():
            alias = aliases.get(repo['distributor_id'])
            if not alias or '*' not in alias:
                continue
            alias = alias['*'].copy()
            try:
                for key, names in alias.items():
                    alias[key] = \
                            obs.presolve(repo['name'], repo['arch'], names)
                alias['status'] = 'success'
            except Exception, error:
                util.exception('Failed to preresolve %r', alias)
                alias = {'status': str(error)}
            presolve[repo['name']] = alias

        self.request.call('PUT', document='context', guid=self.guid,
                content={'packages': packages, 'presolve': presolve})
