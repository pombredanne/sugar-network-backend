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

import os
import zipfile
from os.path import join, exists, dirname

from sugar_network import sugar, util
from sugar_network.resources import Implementation


DEEP = 2


class Solution(object):

    context = property(lambda self: self.value.interface)
    selections = property(lambda self: self.value.selections)

    def __init__(self, value, req):
        self.value = value
        self.ready = True
        self.details = {}
        self.failure_reason = None
        self.requirements = req

    def __getitem__(self, url):
        selection = self.selections.get(url)
        if selection is not None:
            return _Selection(selection)

    def __iter__(self):
        for i in self.selections.values():
            yield _Selection(i)

    def __hash__(self):
        return hash(self.id)

    def __cmp__(self, other):
        return cmp(self.id, other.id)

    @property
    def id(self):
        return (self.context, tuple([i.path for i in self.commands]))

    @property
    def top(self):
        return self[self.context]

    @property
    def commands(self):
        if self.top is None:
            return []
        else:
            return self.value.commands

    def walk(self, reverse=False, depth=DEEP, uniq=True, include_top=True):
        done = set()

        def process_node(context, parent_dep, extra_deps, path):
            if uniq:
                if context in done:
                    return
                done.add(context)

            sel = self[context]
            if sel is None:
                yield _Selection(None, context), parent_dep, path
                return

            if reverse:
                if include_top or context != self.context:
                    yield sel, parent_dep, path

            if _is_shallow(len(path) + 1, depth):
                for dep in sel.dependencies + extra_deps:
                    for i in process_node(dep.context, dep, [], path + [sel]):
                        yield i

            if not reverse:
                if include_top or context != self.context:
                    yield sel, parent_dep, path

        extra_deps = []
        for i in self.commands:
            extra_deps += i.requires

        return process_node(self.context, None, extra_deps, [])


class _Selection(object):

    def __init__(self, orig, context=None):
        self._value = orig
        self._installed = None
        self._to_install = None
        self._context = context

    def __repr__(self):
        return self.context

    @property
    def nil(self):
        return self._value is None

    @property
    def context(self):
        if self._value is None:
            return self._context
        else:
            return self['interface']

    # pylint: disable-msg=W0212
    guid = property(lambda self: self._value.impl.id)
    bindings = property(lambda self: self._value.impl.bindings)
    dependencies = property(lambda self: self._value.dependencies)
    download_sources = property(lambda self: self._value.impl.download_sources)
    local_path = property(lambda self: self._value.impl.local_path)

    def download(self):
        path = sugar.profile_path('implementations', self.guid)
        if not exists(path):
            tmp_path = util.TempFilePath(dir=dirname(path))
            with file(tmp_path, 'wb') as f:
                for chunk in Implementation(self.guid).get_blob('bundle'):
                    f.write(chunk)
                if not f.tell():
                    return
            bundle = zipfile.ZipFile(tmp_path)
            bundle.extractall(path)

        top_files = os.listdir(path)
        if len(top_files) == 1:
            path = join(path, top_files[0])
        self._value.impl.local_path = path

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
