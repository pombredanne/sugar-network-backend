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

"""Convenient access to RRD databases."""

import re
import os
import time
import bisect
import logging
from datetime import datetime
from os.path import exists, join


_DB_FILENAME_RE = re.compile('(.*?)(-[0-9]+){0,1}\\.rrd$')
_INFO_RE = re.compile('([^[]+)\\[([^]]+)\\]\\.(.*)$')

_FETCH_PAGE = 256

_logger = logging.getLogger('sugar_stats')
_rrdtool = None


class Rrd(object):

    def __init__(self, root, step, rras=None):
        global _rrdtool

        import rrdtool
        _rrdtool = rrdtool

        self._root = root
        self._step = step
        # rrdtool knows nothing about `unicode`
        self._rras = [i.encode('utf8') for i in rras or []]
        self._dbsets = {}

        if not exists(self._root):
            os.makedirs(self._root)

        for filename in os.listdir(self._root):
            match = _DB_FILENAME_RE.match(filename)
            if match is not None:
                name, revision = match.groups()
                self.get(name).load(filename, int(revision or 0))

    def __iter__(self):
        for i in self._dbsets.values():
            yield i

    def __getitem__(self, name):
        return self.get(name)

    @property
    def root(self):
        return self._root

    @property
    def step(self):
        return self._step

    def get(self, name):
        db = self._dbsets.get(name)
        if db is None:
            db = self._dbsets[name] = \
                    _DbSet(self._root, name, self._step, self._rras)
        return db


class _DbSet(object):

    def __init__(self, root, name, step, rras):
        self._root = root
        self.name = name
        self._step = step
        self._rras = rras
        self._revisions = []
        self._field_names = []
        self.__db = None

    @property
    def first(self):
        if self._revisions:
            return self._revisions[0].first

    @property
    def last(self):
        if self._revisions:
            return self._revisions[-1].last

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

        if timestamp <= db.last:
            _logger.warning('Database %s updated at %s, %s in the past',
                    db.path, db.last, timestamp)
            return

        value = [str(timestamp)]
        for name in self._field_names:
            value.append(str(values[name]))

        _logger.debug('Put %r to %s', value, db.path)

        db.put(':'.join(value))

    def get(self, start=None, end=None, resolution=None):
        if not self._revisions:
            return

        if not resolution:
            resolution = self._step

        if start is None:
            start = self._revisions[0].first
        if end is None:
            end = self._revisions[-1].last

        revisions = []
        for db in reversed(self._revisions):
            revisions.append(db)
            if db.last <= start:
                break

        start = start - start % self._step - self._step
        end = min(end, start + _FETCH_PAGE * resolution)
        end -= end % self._step + self._step

        for db in reversed(revisions):
            db_end = min(end, db.last - self._step)
            if start > db_end:
                break
            (row_start, start, row_step), __, rows = _rrdtool.fetch(
                    str(db.path),
                    'AVERAGE',
                    '--start', str(start),
                    '--end', str(db_end),
                    '--resolution', str(resolution))
            for raw_row in rows:
                row_start += row_step
                row = {}
                accept = False
                for i, value in enumerate(raw_row):
                    row[db.field_names[i]] = value
                    accept = accept or value is not None
                if accept:
                    yield row_start, row
            start = db_end + 1

    def _get_db(self, timestamp):
        if self.__db is None and self._field_names:
            if self._revisions:
                db = self._revisions[-1]
                if db.last >= timestamp:
                    _logger.warning(
                            'Database %s updated at %s, %s in the past',
                            db.path, db.last, timestamp)
                    return None
                if db.step != self._step or db.rras != self._rras or \
                        db.field_names != self._field_names:
                    db = self._create_db(self._field_names, db.revision + 1,
                            db.last)
            else:
                db = self._create_db(self._field_names, 0, timestamp)
            self.__db = db
        return self.__db

    def _create_db(self, field_names, revision, timestamp):
        filename = self.name
        if revision:
            filename += '-%s' % revision
        filename += '.rrd'

        _logger.debug('Create %s database in %s starting from %s',
                filename, self._root, timestamp)

        fields = []
        for name in field_names:
            fields.append(str('DS:%s:GAUGE:%s:U:U' % (name, self._step * 2)))

        _rrdtool.create(
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

        info = _rrdtool.info(self.path)
        self.step = info['step']
        self.last = info['last_update']

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
        _rrdtool.update(self.path, str(value))
        self.last = _rrdtool.info(self.path)['last_update']

    @property
    def first(self):
        return _rrdtool.first(self.path)

    def __cmp__(self, other):
        return cmp(self.revision, other.revision)
