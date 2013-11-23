# Copyright (C) 2012-2013 Aleksey Lim
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

from sugar_network.model.routes import VolumeRoutes, FrontRoutes


CONTEXT_TYPES = ['activity', 'project', 'package', 'content']
NOTIFICATION_TYPES = ['create', 'update', 'delete', 'vote']
FEEDBACK_TYPES = ['question', 'idea', 'problem']
ARTIFACT_TYPES = ['instance', 'preview']

NOTIFICATION_OBJECT_TYPES = [
        '', 'content', 'feedback', 'solution', 'artifact', 'version', 'report',
        ]

STABILITIES = [
        'insecure', 'buggy', 'developer', 'testing', 'stable',
        ]

RATINGS = [0, 1, 2, 3, 4, 5]

RESOURCES = (
        'sugar_network.model.artifact',
        'sugar_network.model.comment',
        'sugar_network.model.context',
        'sugar_network.model.implementation',
        'sugar_network.model.notification',
        'sugar_network.model.feedback',
        'sugar_network.model.report',
        'sugar_network.model.review',
        'sugar_network.model.solution',
        'sugar_network.model.user',
        )
