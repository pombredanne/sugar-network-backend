# Copyright (C) 2010-2012, Aleksey Lim
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

import sys
from os.path import join, abspath, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), 'lib'))

from zeroinstall.injector.requirements import Requirements

from sugar_network.sweets.solver import solve
from sugar_network.sweets.injector import launch, make, execute


def _inject_sweets():
    from zeroinstall.injector import reader, model
    from sugar_network.sweets import feeds
    from sugar_network.util import enforce

    def Interface_init(self, url):
        enforce(url)
        self.uri = url
        self.reset()

    model.Interface.__init__ = Interface_init
    reader.load_feed_from_cache = \
            lambda url, * args, ** kwargs: feeds.load(url)
    reader.check_readable = lambda * args, ** kwargs: True


_inject_sweets()
