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
import json
import bisect
import logging
from os.path import exists, join, splitext


_DB_FILENAME_RE = re.compile('(.*?)(-[0-9]+){0,1}\\.rrd$')
_INFO_RE = re.compile('([^[]+)\\[([^]]+)\\]\\.(.*)$')

_FETCH_PAGE = 256

_logger = logging.getLogger('rrd')
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

    def __contains__(self, name):
        return name in self._dbsets

    @property
    def root(self):
        return self._root

    @property
    def step(self):
        return self._step

    def get(self, name):
        db = self._dbsets.get(name)
        if db is None:
            db = _DbSet(self._root, name, self._step, self._rras)
            self._dbsets[name] = db
        return db


class _DbSet(object):

    def __init__(self, root, name, step, rras):
        self._root = root
        self.name = name
        self._step = step
        self._rras = rras
        self._revisions = []
        self._fields = None
        self._field_names = None
        self.__db = None

    @property
    def fields(self):
        return self._field_names

    @fields.setter
    def fields(self, fields):
        self._field_names = fields.keys()
        self._field_names.sort()
        self._fields = [str(fields[i]) for i in self._field_names]
        _logger.debug('Set %r fields for %r', self._fields, self.name)

    @property
    def first(self):
        if not self._revisions:
            return
        return self._revisions[0].first

    @property
    def last(self):
        if not self._revisions:
            return
        return self._revisions[-1].last

    @property
    def last_ds(self):
        if not self._revisions or not self._field_names:
            return {}
        info = _rrdtool.info(self._revisions[-1].path)
        result = {}
        for field in self._field_names:
            result[field] = float(info.get('ds[%s].last_ds' % field) or 0)
        return result

    def load(self, filename, revision):
        _logger.debug('Load %s database from %s with revision %s',
                filename, self._root, revision)
        db = _Db(join(self._root, filename), revision)
        bisect.insort(self._revisions, db)
        return db

    def put(self, values, timestamp=None):
        if not self.fields:
            _logger.debug('Parse fields from the first put')
            self.fields = dict([
                (i, 'DS:%s:GAUGE:%s:U:U' % (i, self._step * 2))
                for i in values])

        if not timestamp:
            timestamp = int(time.time())
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

        db.put(':'.join(value), timestamp)

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
        last = min(end, start + _FETCH_PAGE * resolution)
        last -= last % self._step + self._step

        for db in reversed(revisions):
            db_end = min(last, db.last - self._step)
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
                if row_start > end:
                    break
                row = {}
                for i, value in enumerate(raw_row):
                    row[db.field_names[i]] = value or .0
                yield row_start, row
            start = db_end + 1

    def _get_db(self, timestamp):
        if self.__db is None and self._fields:
            if self._revisions:
                db = self._revisions[-1]
                if db.last >= timestamp:
                    _logger.warning(
                            'Database %s updated at %s, %s in the past',
                            db.path, db.last, timestamp)
                    return None
                if db.step != self._step or db.rras != self._rras or \
                        db.field_names != self._field_names:
                    db = self._create_db(db.revision + 1, db.last)
            else:
                db = self._create_db(0, timestamp)
            self.__db = db
        return self.__db

    def _create_db(self, revision, timestamp):
        filename = self.name
        if revision:
            filename += '-%s' % revision
        filename += '.rrd'

        _logger.debug('Create %s database in %s start=%s step=%s',
                filename, self._root, timestamp, self._step)

        _rrdtool.create(
                str(join(self._root, filename)),
                '--start', str(timestamp - self._step),
                '--step', str(self._step),
                *(self._fields + self._rras))

        return self.load(filename, revision)


class _Db(object):

    def __init__(self, path, revision=0):
        self.path = str(path)
        self._meta_path = splitext(path)[0] + '.meta'
        self.revision = revision
        self.fields = []
        self.field_names = []
        self.rras = []

        info = _rrdtool.info(self.path)
        self.step = info['step']
        self.first = 0
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

        if exists(self._meta_path):
            with file(self._meta_path) as f:
                self.first = json.load(f).get('first')

    def put(self, value, timestamp):
        if not self.first:
            with file(self._meta_path, 'w') as f:
                json.dump({'first': timestamp}, f)
            self.first = timestamp
        _rrdtool.update(self.path, str(value))
        self.last = _rrdtool.info(self.path)['last_update']

    def __cmp__(self, other):
        return cmp(self.revision, other.revision)
