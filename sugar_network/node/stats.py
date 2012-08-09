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


stats = Option(
        'enable stats collecting',
        default=False, type_cast=Option.bool_cast, action='store_true')

stats_root = Option(
        'path to the root directory for placing stats',
        default='/var/lib/sugar-network/stats')

stats_step = Option(
        'step interval in seconds for RRD databases',
        default=60, type_cast=int)

stats_rras = Option(
        'space separated list of RRAs for RRD databases',
        default=['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
        type_cast=Option.list_cast, type_repr=Option.list_repr)


_logger = logging.getLogger('node.stats')
_cache = lrucache(32)


def get_rrd(user):
    if user in _cache:
        return _cache[user]
    else:
        rrd = _cache[user] = Rrd(join(stats_root.value, user[:2], user),
                stats_step.value, stats_rras.value)
        return rrd


def pull(in_seq, packet):
    for user, rrd in _walk_rrd():
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
            seq = PersistentSequence(
                    join(stats_root.value, user[:2], user, db + '.push'),
                    [1, None])
            seq.exclude(merged)
            seq.commit()


def _walk_rrd():
    if not exists(stats_root.value):
        return
    for users_dirname in os.listdir(stats_root.value):
        users_dir = join(stats_root.value, users_dirname)
        if not isdir(users_dir):
            continue
        for user in os.listdir(users_dir):
            yield user, ReadOnlyRrd(join(users_dir, user))
