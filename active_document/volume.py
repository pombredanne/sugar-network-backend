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
import imp
import inspect
import logging
from os.path import exists, basename, join
from gettext import gettext as _

from active_document import env, gthread
from active_document.document import Document
from active_document.index import IndexWriter
from active_document.util import enforce


_logger = logging.getLogger('active_document.volume')


class _Volume(dict):

    def __init__(self, root, document_classes, index_class, extra_props):
        self._root = root

        if type(document_classes) is dict:
            self.update(document_classes)
        elif type(document_classes) in (tuple, list):
            self.update([(i.__name__.lower(), i) for i in document_classes])
        else:
            self.update(_walk_classes(document_classes))

        if not exists(root):
            os.makedirs(root)

        _logger.info(_('Opening documents in %r'), root)

        if extra_props is None:
            extra_props = {}
        for cls in self.values():
            name = cls.__name__.lower()
            cls.init(join(root, name), index_class, extra_props.get(name))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name):
        enforce(name in self, _('Unknow %r document'), name)
        return self.get(name)

    def close(self):
        """Close operations with the server."""
        _logger.info(_('Closing documents in %r'), self._root)

        while self:
            __, cls = self.popitem()
            cls.close()


class SingleVolume(_Volume):

    def __init__(self, root, document_classes, extra_props=None):
        enforce(env.index_write_queue.value > 0,
                _('The active_document.index_write_queue.value should be > 0'))

        _Volume.__init__(self, root, document_classes, IndexWriter,
                extra_props)

        for cls in self.values():
            for __ in cls.populate():
                gthread.dispatch()


def _walk_classes(path):
    classes = set()

    for filename in os.listdir(path):
        if filename == '__init__.py' or not filename.endswith('.py'):
            continue

        mod_name = basename(filename)[:-3]
        fp, pathname, description = imp.find_module(mod_name, [path])
        try:
            mod = imp.load_module(mod_name, fp, pathname, description)
        finally:
            if fp:
                fp.close()

        for __, cls in inspect.getmembers(mod):
            if inspect.isclass(cls) and issubclass(cls, Document):
                classes.add(cls)

    for cls in list(classes):
        if [i for i in classes if i is not cls and issubclass(i, cls)]:
            classes = [i for i in classes if i.__name__ != cls.__name__]

    return [(i.__name__.lower(), i) for i in classes]
