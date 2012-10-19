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

from active_document.document import Document

from active_document.env import ACCESS_CREATE, ACCESS_WRITE, ACCESS_READ, \
        ACCESS_DELETE, ACCESS_AUTHOR, ACCESS_AUTH, ACCESS_PUBLIC, \
        ACCESS_LEVELS, ACCESS_SYSTEM, ACCESS_LOCAL, ACCESS_REMOTE, \
        index_flush_timeout, index_flush_threshold, \
        index_write_queue, \
        NotFound, NotModified, Forbidden, Seqno, DEFAULT_LANG, \
        uuid, default_lang

from active_document.metadata import Metadata, PropertyMeta, Property, \
        StoredProperty, ActiveProperty, BlobProperty, BrowsableProperty, \
        active_property

from active_document.commands import to_int, to_list, \
        volume_command, volume_command_pre, volume_command_post, \
        directory_command, directory_command_pre, directory_command_post, \
        document_command, document_command_pre, document_command_post, \
        property_command, property_command_pre, property_command_post, \
        Request, Response, CommandsProcessor, CommandNotFound

from active_document.volume import SingleVolume, VolumeCommands


MAX_LIMIT = 2147483648
