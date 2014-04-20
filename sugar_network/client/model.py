# Copyright (C) 2014 Aleksey Lim
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

from sugar_network import db
from sugar_network.model.user import User
from sugar_network.model.post import Post
from sugar_network.model.report import Report
from sugar_network.model.context import Context as _Context
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import ACL


_logger = logging.getLogger('client.model')


class Context(_Context):

    @db.indexed_property(db.List, prefix='P', default=[],
            acl=ACL.READ | ACL.LOCAL)
    def pins(self, value):
        return value + this.injector.pins(self.guid)


class Volume(db.Volume):

    def __init__(self, root, resources=None):
        if resources is None:
            resources = [User, Context, Post, Report]
        db.Volume.__init__(self, root, resources)
        for directory in self.values():
            directory.metadata['author'].acl |= ACL.LOCAL
