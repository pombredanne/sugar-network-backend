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

from active_document import util

from active_document.document import Document, active_property

from active_document.env import ACCESS_CREATE, ACCESS_WRITE, ACCESS_READ, \
        ACCESS_DELETE, ACCESS_FULL, \
        data_root, index_flush_timeout, index_flush_threshold, \
        index_write_queue, find_limit, \
        NotFound, Forbidden

from active_document.metadata import Metadata, Property, IndexedProperty, \
        AggregatorProperty, StoredProperty, ActiveProperty, GuidProperty, \
        CounterProperty, BlobProperty


def init(document_classes):
    for cls in document_classes:
        cls.init()
    if index_write_queue.value > 0:
        from active_document import index_queue
        index_queue.init(document_classes)


def close():
    if index_write_queue.value > 0:
        from active_document import index_queue
        index_queue.close()
