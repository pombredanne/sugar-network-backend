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

import time
import logging
from os.path import join

from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit.router import Request
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import Option, coroutine


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


class Monitor(object):

    def __init__(self, volume, step, rras):
        self._volume = volume
        self._rrd = Rrd(join(volume.root, 'var'), 'stats', _DS, step, rras)
        self._stats = self._rrd.values()
        self._stated = False

        if not self._stats:
            for field, traits in _DS.items():
                value = 0
                if traits['type'] == 'GAUGE':
                    directory = volume[traits['resource']]
                    __, value = directory.find(limit=0, **traits['query'])
                self._stats[field] = value

    def count(self, request):
        route_ = _ROUTES.get(
                (request.method, request.resource, request.prop, request.cmd))
        if route_ is None:
            return
        stat, shift = route_
        self._stated = True

        if not isinstance(stat, basestring):
            stat = stat()
        self._stats[stat] += shift

    def get(self, start, end, limit, event):
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

    def auto_commit(self):
        while True:
            coroutine.sleep(self._rrd.step)
            self.commit()

    def commit(self, timestamp=None):
        if not self._stated:
            return
        self._stated = False

        _logger.trace('Commit stats')

        self._rrd.put(self._stats, timestamp)
        for field, traits in _DS.items():
            if traits['type'] == 'ABSOLUTE':
                self._stats[field] = 0

    def regen(self):
        self._rrd.wipe()
        for field in self._stats:
            self._stats[field] = 0

        def timeline(ts):
            ts = long(ts)
            end = long(time.time())
            step_ = None

            archives = {}
            for rra in self._rrd.rras:
                a_step, a_size = [long(i) for i in rra.split(':')[-2:]]
                a_step *= self._rrd.step
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

        items, __ = self._volume['context'].find(limit=1, order_by='ctime')
        start = next(items)['ctime']
        for left, right, __ in timeline(start):
            for resource in ('user', 'context', 'post', 'report'):
                items, __ = self._volume[resource].find(
                        query='ctime:%s..%s' % (left, right))
                for this.resource in items:
                    self.count(Request(method='POST', path=[resource]))
            self.commit(left + (right - left) / 2)
