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

import os
import time
import json
import logging
from os.path import exists, join

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

_HEARTBEAT = 60 * 60 * 24 * 365

_logger = logging.getLogger('node.stats_node')


class Sniffer(object):

    def __init__(self, volume, path, reset=False):
        _logger.info('Collect node stats in %r', path)

        self._volume = volume
        self._rrd = Rrd(path, stats_node_step.value, stats_node_rras.value)
        self._stats = {}
        self._suspend_path = join(path, '.suspend')
        self._last = int(time.time())

        for name, cls in _STATS.items():
            stats = self._stats[name] = cls(self._stats, volume)
            fields = {}
            for field in stats:
                fields[field] = 'DS:%s:GAUGE:%s:U:U' % (field, _HEARTBEAT)
            if fields:
                if not reset:
                    stats.update(self._rrd[name].last_ds)
                    stats['total'] = volume[name].find(limit=0)[1]
                self._rrd[name].fields = fields

        if exists(self._suspend_path):
            with file(self._suspend_path) as f:
                suspend = json.load(f)
            for name, stats in self._stats.items():
                if name not in suspend['state']:
                    continue
                total_stats, stats.objects = suspend['state'][name]
                stats.update(total_stats)
            if suspend['timestamp'] < int(time.time()):
                self.commit(suspend['timestamp'])
                self.commit_objects()
            os.unlink(self._suspend_path)

    def __getitem__(self, name):
        return self._rrd[name]

    def suspend(self):
        state = dict([(i, (j, j.objects)) for i, j in self._stats.items()])
        with file(self._suspend_path, 'w') as f:
            json.dump({
                'timestamp': self._last + stats_node_step.value,
                'state': state,
                }, f)

    def log(self, request):
        if request.cmd or request.resource not in _STATS:
            return
        self._stats[request.resource].log(request)

    def commit(self, timestamp=None, extra_values=None):
        _logger.trace('Commit node stats')

        for resource, stats in self._stats.items():
            if resource not in self._rrd:
                continue
            values = stats.copy()
            if extra_values and resource in extra_values:
                values.update(extra_values[resource])
            if values:
                self._rrd[resource].put(values, timestamp=timestamp)

        self._last = timestamp or int(time.time())

    def commit_objects(self, reset=False):
        _logger.trace('Commit object stats')

        for resource, stats in self._stats.items():
            old = {
                    'downloads': 0,
                    'reviews': (0, 0),
                    }
            directory = self._volume[resource]
            for guid, new in stats.objects.items():
                if not directory.exists(guid):
                    _logger.warning('Ignore stats for missed %r %s',
                            guid, resource)
                    continue
                if not reset:
                    old = directory.get(guid)
                patch = {}
                if 'downloads' in new:
                    patch['downloads'] = new['downloads'] + old['downloads']
                if 'reviews' in new:
                    reviews, rating = old['reviews']
                    reviews += new['reviews']
                    rating += new['rating']
                    patch['reviews'] = [reviews, rating]
                    patch['rating'] = int(round(float(rating) / reviews))
                directory.update(guid, patch)
            stats.objects.clear()

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


class _Stats(dict):

    RESOURCE = None
    OWNERS = []

    def __init__(self, stats, volume):
        self.objects = {}
        self._stats = stats
        self._volume = volume

    def inc(self, guid, prop, value=1):
        obj = self.objects.setdefault(guid, {})
        if prop not in obj:
            obj[prop] = value
        else:
            obj[prop] += value

    def log(self, request):
        pass


class _ResourceStats(_Stats):

    def __init__(self, stats, volume):
        _Stats.__init__(self, stats, volume)
        self['total'] = 0

    def log(self, request):
        if request.method == 'POST':
            self['total'] += 1
        elif request.method == 'DELETE':
            self['total'] -= 1

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

    def __init__(self, stats, volume):
        _ResourceStats.__init__(self, stats, volume)
        self['released'] = 0
        self['failed'] = 0
        self['downloaded'] = 0


class _ImplementationStats(_Stats):

    RESOURCE = 'implementation'
    OWNERS = ['context']

    def log(self, request):
        if request.method == 'GET':
            if request.prop == 'data':
                context = self._volume[self.RESOURCE].get(request.guid)
                self._stats['context'].inc(context.context, 'downloads')
                self._stats['context']['downloaded'] += 1
        elif request.method == 'POST':
            self._stats['context']['released'] += 1


class _ReportStats(_Stats):

    RESOURCE = 'report'
    OWNERS = ['context', 'implementation']

    def log(self, request):
        if request.method == 'POST':
            self._stats['context']['failed'] += 1


class _ReviewStats(_ResourceStats):

    RESOURCE = 'review'
    OWNERS = ['artifact', 'context']

    def log(self, request):
        _ResourceStats.log(self, request)

        if request.method == 'POST':
            if request.content.get('artifact'):
                stats = self._stats['artifact']
                guid = request.content['artifact']
            else:
                stats = self._stats['context']
                guid = self.parse_context(request)
            stats.inc(guid, 'reviews')
            stats.inc(guid, 'rating', request.content.get('rating') or 0)


class _FeedbackStats(_ResourceStats):

    RESOURCE = 'feedback'
    OWNERS = ['context']


class _SolutionStats(_ResourceStats):

    RESOURCE = 'solution'
    OWNERS = ['feedback']


class _ArtifactStats(_ResourceStats):

    RESOURCE = 'artifact'
    OWNERS = ['context']

    def __init__(self, stats, volume):
        _ResourceStats.__init__(self, stats, volume)
        self['downloaded'] = 0

    def log(self, request):
        if request.method == 'POST':
            if request.content.get('type') != 'preview':
                self['total'] += 1
        elif request.method == 'DELETE':
            existing = self._volume[self.RESOURCE].get(request.guid)
            if existing['type'] != 'preview':
                self['total'] -= 1
        elif request.method == 'GET' and request.prop == 'data':
            existing = self._volume[self.RESOURCE].get(request.guid)
            if existing['type'] != 'preview':
                self.inc(request.guid, 'downloads')
                self['downloaded'] += 1


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
