# Copyright (C) 2012-2014 Aleksey Lim
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
from os.path import exists, join, splitext, basename

import rrdtool

from . import Bin


_DB_FILENAME_RE = re.compile('(.*?)(-[0-9]+){0,1}\\.rrd$')
_INFO_RE = re.compile('([^[]+)\\[([^]]+)\\]\\.(.*)$')

_FETCH_PAGE = 256

_logger = logging.getLogger('rrd')


class Rrd(object):

    def __init__(self, root, name, fields, step, rras):
        self._root = root
        self.name = name
        self.step = step
        self._fields = fields or {}
        # rrdtool knows nothing about `unicode`
        self._rras = [i.encode('utf8') for i in rras or []]
        self._revisions = []
        self._db = None

        _logger.debug('[%s] open rrd at %r', self.name, root)

        if not exists(self._root):
            os.makedirs(self._root)

        for filename in os.listdir(self._root):
            match = _DB_FILENAME_RE.match(filename)
            if match is None:
                continue
            name_, revision = match.groups()
            if name_ == name:
                self._load(filename, int(revision or 0))

    @property
    def rras(self):
        return self._rras

    @property
    def first(self):
        return self._revisions[0].first if self._revisions else None

    @property
    def last(self):
        return self._revisions[-1].last if self._revisions else None

    def wipe(self):
        for rev in self._revisions:
            os.unlink(rev.path)
        del self._revisions[:]

    def values(self, timestamp=None):
        return self._revisions[-1].values(timestamp) if self._revisions else {}

    def put(self, values, timestamp=None):
        if not timestamp:
            timestamp = int(time.time())
        timestamp = timestamp / self.step * self.step
        _logger.trace('[%s] put %r', self.name, values)
        self._get_db(timestamp, values).put(values, timestamp)

    def get(self, start=None, end=None, resolution=None):
        if not self._revisions:
            return

        if not resolution:
            resolution = self.step

        if start is None:
            start = self._revisions[0].first
        if end is None:
            end = self._revisions[-1].last

        revisions = []
        for db in reversed(self._revisions):
            revisions.append(db)
            if db.last <= start:
                break

        start = start - start % self.step - self.step
        last = min(end, start + _FETCH_PAGE * resolution)
        last -= last % self.step + self.step

        for db in reversed(revisions):
            db_end = min(last, db.last - self.step)
            if start > db_end:
                break
            for ts, row in db.get(start, db_end, resolution):
                if ts > end:
                    break
                yield ts, row
            start = db_end + 1

    def _get_db(self, timestamp, values):
        if self._db is not None:
            return self._db

        fields = []
        for field in sorted(values.keys()):
            ds = self._fields.get(field) or {}
            ds_type = ds.get('type') or 'GAUGE'
            ds_heartbeat = ds.get('heartbeat') or self.step * 2
            fields.append('DS:%s:%s:%s:U:U' % (field, ds_type, ds_heartbeat))
        _logger.debug('[%s] fields from jut values: %r', self.name, fields)

        if not self._revisions:
            self._db = self._create_db(0, fields, timestamp)
        else:
            db = self._revisions[-1]
            if db.fields == fields and db.rras == self._rras:
                self._db = db
            else:
                self._db = self._create_db(db.revision + 1, fields, db.last)

        return self._db

    def _create_db(self, revision, fields, timestamp):
        filename = self.name
        if revision:
            filename += '-%s' % revision
        filename += '.rrd'
        _logger.debug('[%s] create database filename=%s start=%s step=%s',
                self.name, filename, timestamp, self.step)
        rrdtool.create(
                str(join(self._root, filename)),
                '--start', str(timestamp - self.step),
                '--step', str(self.step),
                *(fields + self._rras))
        return self._load(filename, revision)

    def _load(self, filename, revision):
        _logger.debug('[%s] load database filename=%s revision=%s',
                self.name, filename, revision)
        db = _Db(join(self._root, filename), revision)
        bisect.insort(self._revisions, db)
        return db


class _Db(object):

    def __init__(self, path, revision=0):
        self.path = str(path)
        basepath = splitext(path)[0]
        self.name = basename(basepath)
        self._meta = Bin(basepath + '.meta', {})
        self.revision = revision
        self.fields = []
        self.field_names = []
        self.rras = []

        info = rrdtool.info(self.path)
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
            ds = fields[name]
            self.fields.append('DS:%s:%s:%s:U:U' %
                    (name, ds['type'], ds['minimal_heartbeat']))
            self.field_names.append(name)

    @property
    def first(self):
        return self._meta['first'] or 0

    def values(self, timestamp=None):
        result = None
        if timestamp and timestamp - self.last <= self.step and \
                'pending' in self._meta:
            result = self._meta['pending']
        elif not timestamp or timestamp == self.last:
            info = rrdtool.info(self.path)
            result = {}
            for field in self.field_names:
                result[field] = float(info.get('ds[%s].last_ds' % field) or 0)
        else:
            timestamp -= self.step
            for __, result in self.get(timestamp, timestamp, self.step):
                pass
        return result

    def put(self, values, timestamp):
        if timestamp - self.last < self.step:
            self._meta['pending'] = values
            self._meta.commit()
            return
        if 'pending' in self._meta:
            pending = self._meta.pop('pending')
            if timestamp - self.last >= self.step * 2:
                self.put(pending, self.last + self.step)
            self._meta.commit()
        if not self.first:
            self._meta['first'] = timestamp
            self._meta.commit()
        value = [str(timestamp)]
        for name in self.field_names:
            value.append(str(values[name]))
        rrdtool.update(self.path, str(':'.join(value)))
        self.last = timestamp

    def get(self, start, end, resolution):
        (row_start, start, row_step), __, rows = rrdtool.fetch(
                str(self.path),
                'AVERAGE',
                '--start', str(start),
                '--end', str(end),
                '--resolution', str(resolution))
        for raw_row in rows:
            row_start += row_step
            row = {}
            for i, value in enumerate(raw_row):
                row[self.field_names[i]] = value or .0
            yield row_start, row

    def __cmp__(self, other):
        return cmp(self.revision, other.revision)
