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

import logging
from gettext import gettext as _

import active_document as ad
from sugar_network import node


_logger = logging.getLogger('resources.user')


class Resource(ad.Document):

    @ad.active_property(prefix='RA', full_text=True, default=[], typecast=[],
            permissions=ad.ACCESS_READ)
    def author(self, value):
        return value

    @ad.active_property(prefix='RT', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value

    @classmethod
    def before_post(cls, props):
        if 'user' in props and 'user' in node.volume:
            directory = node.volume['user']
            authors = []
            for user_guid in props['user']:
                if not directory.exists(user_guid):
                    _logger.warning(_('No %s user to set author property'),
                            user_guid)
                    continue
                user = directory.get(user_guid)
                authors.append(user['nickname'])
                if user['fullname']:
                    authors.append(user['fullname'])
            props['author'] = authors
        super(Resource, cls).before_post(props)
