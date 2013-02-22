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

import logging
from os.path import join

from sugar_network import node
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit import Option


stats_node = Option(
        'collect unpersonalized node statistics',
        default=False, type_cast=Option.bool_cast, action='store_true')

stats_node_step = Option(
        'step interval in seconds for node RRD databases',
        default=60 * 5, type_cast=int)

stats_node_rras = Option(
        'space separated list of RRAs for node RRD databases',
        default=[
            'RRA:AVERAGE:0.5:1:288',      # one day with 5min step
            'RRA:AVERAGE:0.5:3:672',      # one week with 15min step
            'RRA:AVERAGE:0.5:12:744',     # one month with 1h step
            'RRA:AVERAGE:0.5:144:732',    # one year with 12h step
            'RRA:AVERAGE:0.5:288:36600',  # hundred years with 24h step
            ],
        type_cast=Option.list_cast, type_repr=Option.list_repr)

_logger = logging.getLogger('node.stats_node')


class Sniffer(object):

    def __init__(self, volume):
        path = join(node.stats_root.value, 'node')
        _logger.info('Start collecting node stats in %r', path)

        self._volume = volume
        self.rrd = Rrd(path, stats_node_step.value, stats_node_rras.value)
        self._stats = {}

        for cls in (_UserStats, _ContextStats, _ImplementationStats,
                _ReportStats, _ReviewStats, _FeedbackStats, _SolutionStats,
                _ArtifactStats, _CommentStats):
            self._stats[cls.DOCUMENT] = cls(self._stats, volume)

    def log(self, request):
        if request.principal is None or 'cmd' in request:
            return
        stats = self._stats.get(request.get('document'))
        if stats is not None:
            stats.log(request)

    def commit(self, timestamp=None):
        _logger.heartbeat('Commit node stats')

        for document, stats in self._stats.items():
            values = stats.commit()
            if values is not None:
                self.rrd[document].put(values, timestamp=timestamp)


class _ObjectStats(object):

    downloaded = 0
    reviews = 0
    rating = 0


class _Stats(object):

    DOCUMENT = None
    OWNERS = []

    active = None

    def __init__(self, stats, volume):
        self._stats = stats
        self._volume = volume

    def log(self, request):
        context = None

        def parse_context(props):
            for owner in self.OWNERS:
                guid = props.get(owner)
                if not guid:
                    continue
                if owner == 'context':
                    return guid
                else:
                    return self._volume[owner].get(guid)['context']

        method = request['method']
        if method == 'GET':
            if 'guid' in request:
                if self.DOCUMENT == 'context':
                    context = request['guid']
                elif self.DOCUMENT != 'user':
                    doc = self._volume[self.DOCUMENT].get(request['guid'])
                    context = doc['context']
            else:
                context = parse_context(request)
        elif method == 'PUT':
            guid = request['guid']
            if self.DOCUMENT == 'context':
                context = guid
            else:
                context = request.content.get('context')
                if not context:
                    context = self._volume[self.DOCUMENT].get(guid)['context']
        elif method == 'POST':
            context = parse_context(request.content)

        stats = self._stats['user']
        if method in ('POST', 'PUT', 'DELETE'):
            stats.effective.add(request.principal)
        stats.active.add(request.principal)

        if context:
            return self._stats['context'].active_object(context)

    def active_object(self, guid):
        result = self.active.get(guid)
        if result is None:
            result = self.active[guid] = _ObjectStats()
        return result

    def commit(self):
        pass


class _ResourceStats(_Stats):

    total = 0
    created = 0
    updated = 0
    deleted = 0
    viewed = 0

    def __init__(self, stats, volume):
        _Stats.__init__(self, stats, volume)
        self.total = volume[self.DOCUMENT].find(limit=0)[1]

    def log(self, request):
        result = _Stats.log(self, request)

        method = request['method']
        if method == 'GET':
            if 'guid' in request and 'prop' not in request:
                self.viewed += 1
        elif method == 'PUT':
            self.updated += 1
        elif method == 'POST':
            self.total += 1
            self.created += 1
        elif method == 'DELETE':
            self.total -= 1
            self.deleted += 1

        return result

    def commit(self):
        if type(self.active) is dict:
            directory = self._volume[self.DOCUMENT]
            for guid, stats in self.active.items():
                if not stats.downloaded and not stats.reviews:
                    continue
                props = {}
                doc = directory.get(guid)
                if stats.downloaded:
                    props['downloads'] = doc['downloads'] + stats.downloaded
                if stats.reviews:
                    reviews, rating = doc['reviews']
                    reviews += stats.reviews
                    rating += stats.rating
                    props['reviews'] = [reviews, rating]
                    props['rating'] = int(round(float(rating) / reviews))
                directory.update(guid, props)

        result = {}
        for attr in dir(self):
            if attr[0] == '_' or attr[0].isupper():
                continue
            value = getattr(self, attr)
            if type(value) in (set, dict):
                value = len(value)
            if type(value) in (int, long):
                result[attr] = value

        self.created = 0
        self.updated = 0
        self.deleted = 0
        self.viewed = 0

        return result


class _UserStats(_ResourceStats):

    DOCUMENT = 'user'

    def __init__(self, stats, volume):
        _ResourceStats.__init__(self, stats, volume)
        self.active = set()
        self.effective = set()

    def commit(self):
        result = _ResourceStats.commit(self)
        self.active.clear()
        self.effective.clear()
        return result


class _ContextStats(_ResourceStats):

    DOCUMENT = 'context'

    downloaded = 0

    released = 0
    failed = 0
    reviewed = 0

    def __init__(self, stats, volume):
        _ResourceStats.__init__(self, stats, volume)
        self.active = {}

    def commit(self):
        result = _ResourceStats.commit(self)
        self.downloaded = 0
        self.released = 0
        self.failed = 0
        self.reviewed = 0
        self.active.clear()
        return result


class _ImplementationStats(_Stats):

    DOCUMENT = 'implementation'
    OWNERS = ['context']

    def log(self, request):
        context = _Stats.log(self, request)

        method = request['method']
        if method == 'GET':
            if request.get('prop') == 'data':
                self._stats['context'].downloaded += 1
                context.downloaded += 1
        elif method == 'POST':
            self._stats['context'].released += 1


class _ReportStats(_Stats):

    DOCUMENT = 'report'
    OWNERS = ['context', 'implementation']

    def log(self, request):
        _Stats.log(self, request)

        if request['method'] == 'POST':
            self._stats['context'].failed += 1


class _ReviewStats(_ResourceStats):

    DOCUMENT = 'review'
    OWNERS = ['artifact', 'context']

    commented = 0

    def log(self, request):
        context = _ResourceStats.log(self, request)

        if request['method'] == 'POST':
            if request.content.get('artifact'):
                artifact = self._stats['artifact']
                stats = artifact.active_object(request.content['artifact'])
                artifact.reviewed += 1
            else:
                stats = context
                self._stats['context'].reviewed += 1
            stats.reviews += 1
            stats.rating += request.content['rating']

    def commit(self):
        result = _ResourceStats.commit(self)
        self.commented = 0
        return result


class _FeedbackStats(_ResourceStats):

    DOCUMENT = 'feedback'
    OWNERS = ['context']

    solutions = 0
    solved = 0
    rejected = 0

    commented = 0

    def __init__(self, stats, volume):
        _ResourceStats.__init__(self, stats, volume)

        not_solved = volume['feedback'].find(limit=0, solution='')[1]
        self.solutions = self.total - not_solved

    def log(self, request):
        _ResourceStats.log(self, request)

        if request['method'] in ('POST', 'PUT'):
            if 'solution' in request.content:
                if request.content['solution'] is None:
                    self.rejected += 1
                    self.solutions -= 1
                else:
                    self.solved += 1
                    self.solutions += 1

    def commit(self):
        result = _ResourceStats.commit(self)
        self.solved = 0
        self.rejected = 0
        self.commented = 0
        return result


class _SolutionStats(_ResourceStats):

    DOCUMENT = 'solution'
    OWNERS = ['feedback']

    commented = 0

    def commit(self):
        result = _ResourceStats.commit(self)
        self.commented = 0
        return result


class _ArtifactStats(_ResourceStats):

    DOCUMENT = 'artifact'
    OWNERS = ['context']

    downloaded = 0
    reviewed = 0

    def __init__(self, stats, volume):
        _ResourceStats.__init__(self, stats, volume)
        self.active = {}

    def log(self, request):
        _ResourceStats.log(self, request)

        if request['method'] == 'GET':
            if request.get('prop') == 'data':
                self.active_object(request['guid']).downloaded += 1
                self.downloaded += 1

    def commit(self):
        result = _ResourceStats.commit(self)
        self.downloaded = 0
        self.reviewed = 0
        self.active.clear()
        return result


class _CommentStats(_Stats):

    DOCUMENT = 'comment'
    OWNERS = ['solution', 'feedback', 'review']

    def log(self, request):
        _Stats.log(self, request)

        if request['method'] == 'POST':
            for owner in ('solution', 'feedback', 'review'):
                if request.content.get(owner):
                    self._stats[owner].commented += 1
                    break
