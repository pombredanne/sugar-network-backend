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

from active_document import util, optparse

from active_document.util import enforce

from active_document.document import Document

from active_document.env import ACCESS_CREATE, ACCESS_WRITE, ACCESS_READ, \
        ACCESS_DELETE, ACCESS_AUTHOR, ACCESS_FULL, ACCESS_AUTH, \
        index_flush_timeout, index_flush_threshold, \
        index_write_queue, find_limit, \
        NotFound, Forbidden, Unauthorized, principal

from active_document.metadata import Metadata, Property, \
        StoredProperty, ActiveProperty, BlobProperty, BrowsableProperty, \
        active_property, active_command

from active_document.commands import document_command, directory_command, \
        volume_command, Command, Request, Response, call

from active_document.index import connect

from active_document.volume import SingleVolume
