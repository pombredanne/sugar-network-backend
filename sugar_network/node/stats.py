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

import os
import logging
from os.path import join, exists, isdir

from pylru import lrucache

from active_toolkit.options import Option
from sugar_network.toolkit.rrd import Rrd, ReadOnlyRrd
from sugar_network.toolkit.collection import Sequence, PersistentSequence


stats_root = Option(
        'path to the root directory for placing stats',
        default='/var/lib/sugar-network/stats')

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

stats_user_step = Option(
        'step interval in seconds for users\' RRD databases',
        default=60, type_cast=int)

stats_user_rras = Option(
        'space separated list of RRAs for users\' RRD databases',
        default=[
            'RRA:AVERAGE:0.5:1:4320',   # one day with 60s step
            'RRA:AVERAGE:0.5:5:2016',   # one week with 5min step
            ],
        type_cast=Option.list_cast, type_repr=Option.list_repr)


_RELATED_STATS = {
        # document: [(owner_document, owner_prop)]
        'comment': {
            'props': {
                },
            'posts': [
                ('solution', 'commented'),
                ('feedback', 'commented'),
                ('review', 'commented'),
                ],
            },
        'implementation': {
            'props': {
                'data': ('context', 'downloaded'),
                },
            'posts': [
                ('context', 'released'),
                ],
            },
        'report': {
            'props': {
                },
            'posts': [
                ('context', 'failed'),
                ],
            },
        'review': {
            'props': {
                },
            'posts': [
                ('artifact', 'reviewed'),
                ('context', 'reviewed'),
                ],
            },
        }

_logger = logging.getLogger('node.stats')
_user_cache = lrucache(32)


def get_rrd(user):
    if user in _user_cache:
        return _user_cache[user]
    else:
        rrd = _user_cache[user] = Rrd(_rrd_path(user),
                stats_user_step.value, stats_user_rras.value)
        return rrd


def pull(in_seq, packet):
    for user, rrd in _walk_rrd(join(stats_root.value, 'user')):
        in_seq.setdefault(user, {})

        for db, db_start, db_end in rrd.dbs:
            seq = in_seq[user].get(db)
            if seq is None:
                seq = in_seq[user][db] = PersistentSequence(
                        join(rrd.root, db + '.push'), [1, None])
            elif seq is not dict:
                seq = in_seq[user][db] = Sequence(seq)
            out_seq = Sequence()

            def dump():
                for start, end in seq:
                    for timestamp, values in \
                            rrd.get(db, max(start, db_start), end or db_end):
                        yield {'timestamp': timestamp, 'values': values}
                        seq.exclude(start, timestamp)
                        out_seq.include(start, timestamp)
                        start = timestamp

            packet.push(dump(), arcname=join('stats', user, db),
                    cmd='stats_push', user=user, db=db,
                    sequence=out_seq)


def commit(sequences):
    for user, dbs in sequences.items():
        for db, merged in dbs.items():
            seq = PersistentSequence(_rrd_path(user, db + '.push'), [1, None])
            seq.exclude(merged)
            seq.commit()


class NodeStats(object):

    def __init__(self, volume):
        path = join(stats_root.value, 'node')
        _logger.info('Start collecting node stats in %r', path)

        self._volume = volume
        self._rrd = Rrd(path, stats_node_step.value, stats_node_rras.value)

        self._stats = {
                'user': _UserStats(),
                'context': _ContextStats(),
                'review': _ReviewStats(),
                'feedback': _FeedbackStats(),
                'solution': _SolutionStats(),
                'artifact': _ArtifactStats(),
                }

        for document, stats in self._stats.items():
            type(stats).total = volume[document].find(limit=0)[1]
        _FeedbackStats.solutions = _FeedbackStats.total - \
                volume['feedback'].find(limit=0, solution='')[1]

    def log(self, request):
        document = request.get('document')
        if request.principal is None or not document or \
                request.get('cmd') is not None:
            return

        method = request['method']
        context = None

        stats = self._stats.get(document)
        if stats is not None:
            if method == 'POST':
                stats.total += 1
                stats.created += 1
            elif method == 'PUT':
                stats.updated += 1
                if document == 'context':
                    context = request['guid']
                elif document == 'feedback' and 'solution' in request.content:
                    if request.content['solution'] is None:
                        stats.rejected += 1
                        type(stats).solutions -= 1
                    else:
                        stats.solved += 1
                        type(stats).solutions += 1
            elif method == 'DELETE':
                stats.total -= 1
                stats.deleted += 1
            elif method == 'GET':
                if 'guid' not in request:
                    context = request.get('context')
                    if not context and stats.OWNER and stats.OWNER in request:
                        owner = self._volume[stats.OWNER]
                        context = owner.get(request[stats.OWNER])['context']
                else:
                    guid = request['guid']
                    if document == 'context':
                        context = guid
                    elif document != 'user':
                        context = self._volume[document].get(guid)['context']
                    if 'prop' in request:
                        prop = stats.PROPS.get(request['prop'])
                        if prop:
                            setattr(stats, prop, getattr(stats, prop) + 1)
                    else:
                        stats.viewed += 1

        related = _RELATED_STATS.get(document)
        if related:
            if method == 'POST':
                for owner, prop in related['posts']:
                    if owner not in request.content:
                        continue
                    if not context:
                        guid = request.content[owner]
                        if owner == 'context':
                            context = guid
                        else:
                            context = self._volume[owner].get(guid)['context']
                    stats = self._stats[owner]
                    setattr(stats, prop, getattr(stats, prop) + 1)
                    # It is important to break after the first hit,
                    # eg, `review.context` will be set all time when
                    # `review.artifact` is optional
                    break
            elif method == 'GET' and 'prop' in request:
                related = related['props'].get(request['prop'])
                if related:
                    owner, prop = related
                    stats = self._stats[owner]
                    setattr(stats, prop, getattr(stats, prop) + 1)

        if context:
            self._stats['context'].active.add(context)

        stats = self._stats['user']
        if method in ('POST', 'PUT', 'DELETE'):
            stats.effective.add(request.principal)
        stats.active.add(request.principal)

    def commit(self, timestamp=None):
        _logger.debug('Commit node stats')

        for document, stats in self._stats.items():
            values = {}
            for attr in dir(stats):
                if attr[0] == '_' or attr[0].isupper():
                    continue
                value = getattr(stats, attr)
                if type(value) is set:
                    value = len(value)
                values[attr] = value
            self._rrd.put(document, values, timestamp=timestamp)
            self._stats[document] = type(stats)()


class _Stats(object):

    OWNER = None
    PROPS = {}

    total = 0
    created = 0
    updated = 0
    deleted = 0
    viewed = 0


class _UserStats(_Stats):

    def __init__(self):
        self.active = set()
        self.effective = set()


class _ContextStats(_Stats):

    released = 0
    failed = 0
    downloaded = 0
    reviewed = 0

    def __init__(self):
        self.active = set()


class _ReviewStats(_Stats):

    OWNER = 'artifact'

    commented = 0


class _FeedbackStats(_Stats):

    solutions = 0
    solved = 0
    rejected = 0
    commented = 0


class _SolutionStats(_Stats):

    OWNER = 'feedback'

    commented = 0


class _ArtifactStats(_Stats):

    PROPS = {'data': 'downloaded'}

    downloaded = 0
    reviewed = 0


def _rrd_path(user, *args):
    return join(stats_root.value, 'user', user[:2], user, *args)


def _walk_rrd(root):
    if not exists(root):
        return
    for users_dirname in os.listdir(root):
        users_dir = join(root, users_dirname)
        if not isdir(users_dir):
            continue
        for user in os.listdir(users_dir):
            yield user, ReadOnlyRrd(join(users_dir, user))
