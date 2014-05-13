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

import os
import time
import logging

from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit.router import route, postroute, Request
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import Option, coroutine, enforce


stats = Option(
        'collect unpersonalized node statistics',
        default=False, type_cast=Option.bool_cast, action='store_true')

stats_step = Option(
        'step interval in seconds for RRD statistics database',
        default=60 * 5, type_cast=int)

stats_rras = Option(
        'comma separated list of RRAs for RRD statistics database',
        default=[
            'RRA:AVERAGE:0.5:1:864',        # 3d with 5min step
            'RRA:AVERAGE:0.5:288:3660',     # 10y with 1d step
            'RRA:AVERAGE:0.5:2880:366',     # 10y with 10d step
            'RRA:AVERAGE:0.5:8640:122',     # 10y with 30d step
            'RRA:AVERAGE:0.5:105120:10',    # 10y with 1y step
            ],
        type_cast=Option.list_cast, type_repr=Option.list_repr)

_HEARTBEAT_EVER = 60 * 60 * 24 * 365

_DS = {
    'contexts': {
        'type': 'GAUGE',
        'heartbeat': _HEARTBEAT_EVER,
        'resource': 'context',
        'query': {},
        },
    'released': {
        'type': 'ABSOLUTE',
        },
    'solved': {
        'type': 'ABSOLUTE',
        },
    'reported': {
        'type': 'ABSOLUTE',
        },
    'topics': {
        'type': 'GAUGE',
        'heartbeat': _HEARTBEAT_EVER,
        'resource': 'user',
        'query': {'topic': ''},
        },
    'posts': {
        'type': 'GAUGE',
        'heartbeat': _HEARTBEAT_EVER,
        'resource': 'user',
        'query': {'not_topic': ''},
        },
    'users': {
        'type': 'GAUGE',
        'heartbeat': _HEARTBEAT_EVER,
        'resource': 'user',
        'query': {},
        },
    }

_ROUTES = {
    ('POST', 'context', None, None):
        ('contexts', +1),
    ('DELETE', 'context', None, None):
        ('contexts', -1),
    ('POST', 'context', 'releases', None):
        ('released', +1),
    ('GET', 'context', None, 'solve'):
        ('solved', +1),
    ('POST', 'report', None, None):
        ('reported', +1),
    ('POST', 'post', None, None):
        (lambda: 'posts' if this.resource['topic'] else 'topics', +1),
    ('DELETE', 'post', None, None):
        (lambda: 'posts' if this.resource['topic'] else 'topics', -1),
    ('POST', 'user', None, None):
        ('users', +1),
    ('DELETE', 'user', None, None):
        ('users', -1),
    }

_LIMIT = 100

_logger = logging.getLogger('node.stats')


class StatRoutes(object):

    _rrd = None
    _stats = None
    _rating = None
    _stated = False

    def stats_init(self, path, step, rras):
        _logger.info('Collect node stats in %r', path)

        self._rrd = Rrd(path, 'stats', _DS, step, rras)
        self._stats = self._rrd.values()
        self._rating = {'context': {}, 'post': {}}

        if not self._stats:
            for field, traits in _DS.items():
                value = 0
                if traits['type'] == 'GAUGE':
                    directory = this.volume[traits['resource']]
                    __, value = directory.find(limit=0, **traits['query'])
                self._stats[field] = value

    @postroute
    def stat_on_postroute(self, result, exception, stat_rating=True):
        if self._rrd is None or exception is not None:
            return result

        r = this.request
        route_ = _ROUTES.get((r.method, r.resource, r.prop, r.cmd))
        if route_ is None:
            return result
        stat, shift = route_
        self._stated = True

        if not isinstance(stat, basestring):
            stat = stat()
        self._stats[stat] += shift

        if stat_rating:
            rating = None
            if stat == 'topics' and this.resource['type'] == 'review':
                rating = self._rating['context']
                rating = rating.setdefault(this.resource['context'], [0, 0])
            elif stat == 'posts':
                rating = self._rating['post']
                rating = rating.setdefault(this.resource['topic'], [0, 0])
            if rating is not None:
                rating[0] += shift
                rating[1] += shift * this.resource['vote']

        return result

    @route('GET', cmd='stats', arguments={
                'start': int, 'end': int, 'limit': int, 'event': list},
            mime_type='application/json')
    def stats(self, start, end, limit, event):
        enforce(self._rrd is not None, 'Statistics disabled')

        if not start:
            start = self._rrd.first or 0
        if not end:
            end = self._rrd.last or 0
        if limit > _LIMIT:
            _logger.debug('Decrease %d limit by %d', limit, _LIMIT)
            limit = _LIMIT
        elif limit <= 0:
            result = self._rrd.values(end)
            result['timestamp'] = end
            return result
        resolution = max(1, (end - start) / limit)

        result = []
        for ts, values in self._rrd.get(start, end, resolution):
            if event:
                values = dict([(i, values[i]) for i in event])
            values['timestamp'] = ts
            result.append(values)
        return result

    def stats_auto_commit(self):
        while True:
            coroutine.sleep(self._rrd.step)
            self.stats_commit()

    def stats_commit(self, timestamp=None):
        if not self._stated:
            return
        self._stated = False

        _logger.trace('Commit stats')

        self._rrd.put(self._stats, timestamp)
        for field, traits in _DS.items():
            if traits['type'] == 'ABSOLUTE':
                self._stats[field] = 0

        for resource, stats_ in self._rating.items():
            directory = this.volume[resource]
            for guid, (votes, reviews) in stats_.items():
                rating = directory[guid]['rating']
                directory.update(guid, {
                    'rating': [rating[0] + votes, rating[1] + reviews],
                    })
            stats_.clear()

    def stats_regen(self, path, step, rras):
        for i in Rrd(path, 'stats', _DS, step, rras).files:
            os.unlink(i)
        self.stats_init(path, step, rras)
        for field in self._stats:
            self._stats[field] = 0

        def timeline(ts):
            ts = long(ts)
            end = long(time.time())
            step_ = None

            archives = {}
            for rra in rras:
                a_step, a_size = [long(i) for i in rra.split(':')[-2:]]
                a_step *= step
                a_start = end - min(end, a_step * a_size)
                if archives.setdefault(a_start, a_step) > a_step:
                    archives[a_start] = a_step
            archives = list(sorted(archives.items()))

            while ts <= end:
                while not step_ or archives and ts >= archives[0][0]:
                    archive_start, step_ = archives.pop(0)
                    ts = max(ts / step_ * step_, archive_start)
                yield ts, ts + step_ - 1, step_
                ts += step_

        items, __ = this.volume['context'].find(limit=1, order_by='ctime')
        start = next(items)['ctime']
        for left, right, __ in timeline(start):
            for resource in ('user', 'context', 'post', 'report'):
                items, __ = this.volume[resource].find(
                        query='ctime:%s..%s' % (left, right))
                for this.resource in items:
                    this.request = Request(method='POST', path=[resource])
                    self.stat_on_postroute(None, None, False)
            self.stats_commit(left + (right - left) / 2)

    def stats_regen_rating(self, path, step, rras):

        def calc_rating(**kwargs):
            rating = [0, 0]
            alldocs, __ = this.volume['post'].find(**kwargs)
            for post in alldocs:
                rating[0] += 1
                rating[1] += post['vote']
            return rating

        alldocs, __ = this.volume['context'].find()
        for context in alldocs:
            rating = calc_rating(type='review', context=context.guid)
            this.volume['context'].update(context.guid, {'rating': rating})

        alldocs, __ = this.volume['post'].find(topic='')
        for topic in alldocs:
            rating = calc_rating(topic=topic.guid)
            this.volume['post'].update(topic.guid, {'rating': rating})
