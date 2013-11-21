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

from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit import Option


stats_node = Option(
        'collect unpersonalized node statistics',
        default=False, type_cast=Option.bool_cast, action='store_true')

stats_node_step = Option(
        'step interval in seconds for node RRD databases',
        default=60 * 5, type_cast=int)

stats_node_rras = Option(
        'comma separated list of RRAs for node RRD databases',
        default=[
            'RRA:AVERAGE:0.5:1:864',        # 3d with 5min step
            'RRA:AVERAGE:0.5:288:3660',     # 10y with 1d step
            'RRA:AVERAGE:0.5:2880:366',     # 10y with 10d step
            'RRA:AVERAGE:0.5:8640:122',     # 10y with 30d step
            'RRA:AVERAGE:0.5:105408:10',    # 10y with 1y step
            ],
        type_cast=Option.list_cast, type_repr=Option.list_repr)

_logger = logging.getLogger('node.stats_node')


class Sniffer(object):

    def __init__(self, volume, path, reset=False):
        _logger.info('Collect node stats in %r', path)

        self._volume = volume
        self._rrd = Rrd(path, stats_node_step.value, stats_node_rras.value)
        self._stats = {}

        for name, cls in _STATS.items():
            stats = self._stats[name] = cls(self._stats, volume, reset)
            fields = {}
            for attr in dir(stats):
                if attr[0] == '_' or attr[0].isupper() or \
                        type(getattr(stats, attr)) not in (int, long):
                    continue
                if attr == 'total':
                    dst = 'GAUGE'
                    limit = 60 * 60 * 24 * 365
                else:
                    dst = 'ABSOLUTE'
                    limit = stats_node_step.value
                fields[attr] = 'DS:%s:%s:%s:U:U' % (attr, dst, limit)
            if fields:
                self._rrd[name].fields = fields

    def __getitem__(self, name):
        return self._rrd[name]

    def log(self, request):
        if request.cmd or request.resource not in _STATS:
            return
        self._stats[request.resource].log(request)

    def commit(self, timestamp=None, extra_values=None):
        _logger.trace('Commit node stats')

        for resource, stats in self._stats.items():
            if resource not in self._rrd:
                continue
            values = {}
            for field in self._rrd[resource].fields:
                values[field] = getattr(stats, field)
                if field != 'total':
                    setattr(stats, field, 0)
            if extra_values and resource in extra_values:
                values.update(extra_values[resource])
            if values:
                self._rrd[resource].put(values, timestamp=timestamp)

    def commit_objects(self, reset=False):
        _logger.trace('Commit object stats')

        for resource, stats in self._stats.items():
            obj = {
                    'downloads': 0,
                    'reviews': (0, 0),
                    }
            directory = self._volume[resource]
            for guid, obj_stats in stats.active.items():
                if not obj_stats.reviews and not obj_stats.downloads:
                    continue
                if not directory.exists(guid):
                    _logger.warning('Ignore stats for missed %r %s',
                            guid, resource)
                    continue
                if not reset:
                    obj = directory.get(guid)
                patch = {}
                if obj_stats.downloads:
                    patch['downloads'] = obj_stats.downloads + obj['downloads']
                if obj_stats.reviews:
                    reviews, rating = obj['reviews']
                    reviews += obj_stats.reviews
                    rating += obj_stats.rating
                    patch['reviews'] = [reviews, rating]
                    patch['rating'] = int(round(float(rating) / reviews))
                directory.update(guid, patch)
            stats.active.clear()

    def report(self, dbs, start, end, records):
        result = {}

        rdbs = [self._rrd[i] for i in dbs if i in self._rrd]
        if not rdbs:
            return result

        if not start:
            start = min([i.first for i in rdbs]) or 0
        if not end:
            end = max([i.last for i in rdbs]) or 0
        resolution = max(1, (end - start) / records)

        _logger.debug('Report start=%s end=%s resolution=%s dbs=%r',
                start, end, resolution, dbs)

        for rdb in rdbs:
            info = result[rdb.name] = []
            for ts, ds_values in rdb.get(start, end, resolution):
                values = {}
                for name in dbs[rdb.name]:
                    values[name] = ds_values.get(name)
                info.append((ts, values))

        return result


class _ObjectStats(object):

    downloads = 0
    reviews = 0
    rating = 0


class _Stats(object):

    RESOURCE = None
    OWNERS = []

    def __init__(self, stats, volume, reset):
        self.active = {}
        self._stats = stats
        self._volume = volume

    def __getitem__(self, guid):
        result = self.active.get(guid)
        if result is None:
            result = self.active[guid] = _ObjectStats()
        return result

    def log(self, request):
        pass


class _ResourceStats(_Stats):

    total = 0

    def __init__(self, stats, volume, reset):
        _Stats.__init__(self, stats, volume, reset)
        if not reset:
            self.total = volume[self.RESOURCE].find(limit=0)[1]

    def log(self, request):
        if request.method == 'POST':
            self.total += 1
        elif request.method == 'DELETE':
            self.total -= 1

    def parse_context(self, request):
        context = None
        directory = self._volume[self.RESOURCE]

        def parse_context(props):
            for owner in self.OWNERS:
                guid = props.get(owner)
                if not guid:
                    continue
                if owner == 'context':
                    return guid
                else:
                    return self._volume[owner].get(guid)['context']

        if request.method == 'GET':
            if not request.guid:
                context = parse_context(request)
            elif self.RESOURCE == 'context':
                context = request.guid
            elif self.RESOURCE != 'user':
                context = directory.get(request.guid)['context']
        elif request.method == 'PUT':
            if self.RESOURCE == 'context':
                context = request.guid
            else:
                context = request.content.get('context')
                if not context:
                    context = directory.get(request.guid)['context']
        elif request.method == 'POST':
            context = parse_context(request.content)

        return context


class _UserStats(_ResourceStats):

    RESOURCE = 'user'


class _ContextStats(_ResourceStats):

    RESOURCE = 'context'

    released = 0
    failed = 0
    downloaded = 0


class _ImplementationStats(_Stats):

    RESOURCE = 'implementation'
    OWNERS = ['context']

    def log(self, request):
        if request.method == 'GET':
            if request.prop == 'data':
                context = self._volume[self.RESOURCE].get(request.guid)
                self._stats['context'][context.context].downloads += 1
                self._stats['context'].downloaded += 1
        elif request.method == 'POST':
            self._stats['context'].released += 1


class _ReportStats(_Stats):

    RESOURCE = 'report'
    OWNERS = ['context', 'implementation']

    def log(self, request):
        if request.method == 'POST':
            self._stats['context'].failed += 1


class _ReviewStats(_ResourceStats):

    RESOURCE = 'review'
    OWNERS = ['artifact', 'context']

    def log(self, request):
        _ResourceStats.log(self, request)

        if request.method == 'POST':
            if request.content.get('artifact'):
                artifact = self._stats['artifact']
                stats = artifact[request.content['artifact']]
            else:
                stats = self._stats['context'][self.parse_context(request)]
            stats.reviews += 1
            stats.rating += request.content.get('rating') or 0


class _FeedbackStats(_ResourceStats):

    RESOURCE = 'feedback'
    OWNERS = ['context']


class _SolutionStats(_ResourceStats):

    RESOURCE = 'solution'
    OWNERS = ['feedback']


class _ArtifactStats(_ResourceStats):

    RESOURCE = 'artifact'
    OWNERS = ['context']

    downloaded = 0

    def log(self, request):
        _ResourceStats.log(self, request)

        if request.method == 'GET':
            if request.prop == 'data':
                self[request.guid].downloads += 1
                self.downloaded += 1


class _CommentStats(_ResourceStats):

    RESOURCE = 'comment'
    OWNERS = ['solution', 'feedback', 'review']


_STATS = {_UserStats.RESOURCE: _UserStats,
          _ContextStats.RESOURCE: _ContextStats,
          _ImplementationStats.RESOURCE: _ImplementationStats,
          _ReportStats.RESOURCE: _ReportStats,
          _ReviewStats.RESOURCE: _ReviewStats,
          _FeedbackStats.RESOURCE: _FeedbackStats,
          _SolutionStats.RESOURCE: _SolutionStats,
          _ArtifactStats.RESOURCE: _ArtifactStats,
          _CommentStats.RESOURCE: _CommentStats,
          }
