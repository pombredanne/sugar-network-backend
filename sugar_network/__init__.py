# Copyright (C) 2012, Aleksey Lim
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

from sugar_network.resources import User, Context, Question, Idea, Problem, \
        Review, Solution, Artifact, Implementation, Report, Notification, \
        Comment

from sugar_network.env import api_url, certfile, no_check_certificate, debug, \
        config, launch

from sugar_network.sugar import guid, profile_path, pubkey, nickname, color, \
        machine_sn, machine_uuid
