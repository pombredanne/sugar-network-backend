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
from os.path import exists, basename
from gettext import gettext as _

from active_document import env, gthread
from active_document.document import Document
from active_document.index import IndexWriter
from active_document.util import enforce


_logger = logging.getLogger('active_document.folder')


class _Folder(dict):

    def __init__(self, document_classes, index_class):
        enforce(env.data_root.value,
                _('The active_document.data_root.value is not set'))

        if type(document_classes) is dict:
            self.update(document_classes)
        elif type(document_classes) in (tuple, list):
            self.update([(i.__name__, i) for i in document_classes])
        else:
            self.update(_walk_classes(document_classes))

        if not exists(env.data_root.value):
            os.makedirs(env.data_root.value)

        _logger.info(_('Opening documents in "%s"'), env.data_root.value)

        for cls in self.values():
            cls.init(index_class)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """Close operations with the server."""
        _logger.info(_('Closing documents in "%s"'), env.data_root.value)

        while self:
            __, cls = self.popitem()
            cls.close()


class SingleFolder(_Folder):

    def __init__(self, document_classes):
        enforce(env.index_write_queue.value > 0,
                _('The active_document.index_write_queue.value should be > 0'))

        _Folder.__init__(self, document_classes, IndexWriter)

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

    return [(i.__name__, i) for i in classes]
