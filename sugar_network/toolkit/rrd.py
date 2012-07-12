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

"""Convenient access to RRD databases.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/rrd.py$
$Date: 2012-07-12$

"""

import re
import os
import time
import bisect
import logging
from datetime import datetime
from os.path import exists, join

import rrdtool


_DB_FILENAME_RE = re.compile('(.*?)(-[0-9]+){0,1}\.rrd$')
_INFO_RE = re.compile('([^[]+)\[([^]]+)\]\.(.*)$')

_FETCH_PAGE = 256

_logger = logging.getLogger('sugar_stats')


class Rrd(object):

    def __init__(self, root, step, rras):
        self._root = root
        self._step = step
        # rrdtool knows nothing about `unicode`
        self._rras = [i.encode('utf8') for i in rras]
        self._dbsets = {}

        if not exists(self._root):
            os.makedirs(self._root)

        for filename in os.listdir(self._root):
            match = _DB_FILENAME_RE.match(filename)
            if match is not None:
                name, revision = match.groups()
                self._dbset(name).load(filename, int(revision or 0))

    @property
    def step(self):
        return self._step

    @property
    def dbs(self):
        for name, dbset in self._dbsets.items():
            db = dbset.db
            if db is not None:
                yield name, db.first, db.last_update

    def put(self, name, values, timestamp=None):
        self._dbset(name).put(values, timestamp)

    def get(self, name, start=None, end=None):
        return self._dbset(name).get(start, end)

    def _dbset(self, name):
        db = self._dbsets.get(name)
        if db is None:
            db = self._dbsets[name] = \
                    _DbSet(self._root, name, self._step, self._rras)
        return db


class _DbSet(object):

    def __init__(self, root, name, step, rras):
        self._root = root
        self._name = name
        self._step = step
        self._rras = rras
        self._revisions = []
        self._field_names = []
        self.__db = None

    @property
    def db(self):
        if self._revisions:
            return self._revisions[-1]

    def load(self, filename, revision):
        _logger.debug('Load %s database from %s with revision %s',
                filename, self._root, revision)
        db = _Db(join(self._root, filename), revision)
        bisect.insort(self._revisions, db)
        return db

    def put(self, values, timestamp=None):
        if not self._field_names:
            self._field_names = values.keys()
            self._field_names.sort()

        if not timestamp:
            timestamp = int(time.mktime(datetime.utcnow().utctimetuple()))
        timestamp = timestamp / self._step * self._step

        db = self._get_db(timestamp)
        if db is None:
            return

        if timestamp <= db.last_update:
            _logger.warning('Database %s updated at %s, %s in the past',
                    db.path, db.last_update, timestamp)
            return

        value = [str(timestamp)]
        for name in self._field_names:
            value.append(str(values[name]))

        _logger.debug('Put %r to %s', value, db.path)

        db.put(':'.join(value))

    def get(self, start=None, end=None):
        if not self._revisions:
            return

        if start is None:
            start = self._revisions[0].first
        if end is None:
            end = self._revisions[-1].last_update

        revisions = []
        for db in reversed(self._revisions):
            revisions.append(db)
            if db.last_update <= start:
                break

        start = start / self._step * self._step - self._step
        end = end / self._step * self._step - self._step

        for db in reversed(revisions):
            db_end = min(end, db.last_update - self._step)
            while start <= db_end:
                until = max(start,
                        min(start + _FETCH_PAGE, db_end))
                (row_start, start, row_step), __, rows = rrdtool.fetch(
                        str(db.path),
                        'AVERAGE',
                        '--start', str(start),
                        '--end', str(until))
                for raw_row in rows:
                    row_start += row_step
                    row = {}
                    accept = False
                    for i, value in enumerate(raw_row):
                        row[db.field_names[i]] = value
                        accept = accept or value is not None
                    if accept:
                        yield row_start, row
                start = until + 1

    def _get_db(self, timestamp):
        if self.__db is None and self._field_names:
            if self._revisions:
                db = self._revisions[-1]
                if db.last_update >= timestamp:
                    _logger.warning(
                            'Database %s updated at %s, %s in the past',
                            db.path, db.last_update, timestamp)
                    return None
                if db.step != self._step or db.rras != self._rras or \
                        db.field_names != self._field_names:
                    db = self._create_db(self._field_names, db.revision + 1,
                            db.last_update)
            else:
                db = self._create_db(self._field_names, 0, timestamp)
            self.__db = db
        return self.__db

    def _create_db(self, field_names, revision, timestamp):
        filename = self._name
        if revision:
            filename += '-%s' % revision
        filename += '.rrd'

        _logger.debug('Create %s database in %s starting from %s',
                filename, self._root, timestamp)

        fields = []
        for name in field_names:
            fields.append(str('DS:%s:GAUGE:%s:U:U' % (name, self._step * 2)))

        rrdtool.create(
                str(join(self._root, filename)),
                '--start', str(timestamp - self._step),
                '--step', str(self._step),
                *(fields + self._rras))

        return self.load(filename, revision)


class _Db(object):

    def __init__(self, path, revision=0):
        self.path = str(path)
        self.revision = revision
        self.fields = []
        self.field_names = []
        self.rras = []

        info = rrdtool.info(self.path)
        self.step = info['step']
        self.last_update = info['last_update']

        fields = {}
        rras = {}

        for key, value in info.items():
            match = _INFO_RE.match(key)
            if match is None:
                continue
            prefix, key, prop = match.groups()
            if prefix == 'ds':
                fields.setdefault(key, {})
                fields[key][prop] = value
            if prefix == 'rra':
                rras.setdefault(key, {})
                rras[key][prop] = value

        for index in sorted([int(i) for i in rras.keys()]):
            rra = rras[str(index)]
            self.rras.append(
                    'RRA:%(cf)s:%(xff)s:%(pdp_per_row)s:%(rows)s' % rra)

        for name in sorted(fields.keys()):
            props = fields[name]
            props['name'] = name
            self.fields.append(props)
            self.field_names.append(name)

    def put(self, value):
        rrdtool.update(self.path, str(value))
        self.last_update = rrdtool.info(self.path)['last_update']

    @property
    def first(self):
        return rrdtool.first(self.path)

    def __cmp__(self, other):
        return cmp(self.revision, other.revision)
