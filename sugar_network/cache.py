# Copyright (C) 2012, Aleksey Lim
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
import json
import shutil
import logging
import tempfile
from os.path import isdir, exists, lexists, basename, dirname, join
from gettext import gettext as _

from sweets_recipe import Bundle

from sugar_network import sugar, env, http
from sugar_network.util import enforce


_CHUNK_SIZE = 1024 * 10

_logger = logging.getLogger('client')


def resolve_context(name):
    enforce(os.sep not in name,
            _('Not allowed symbols in context name "%s"'), name)

    path = _resolve_path(name)
    if lexists(path):
        return basename(os.readlink(path))

    reply = http.request('GET', ['context'],
            params={'implement': name, 'reply': ['guid']})
    enforce(reply['total'], _('Cannot resolve "%s" context name'), name)
    enforce(reply['total'] == 1,
            _('Name "%s" is associated with more than one context'), name)
    guid = reply['result'][0]['guid']

    _store_resolve_result(guid, [name])

    return guid


def get_properties(resource, guid):
    path = _path(resource, guid, 'properties')
    if not exists(path):
        return
    with file(path) as f:
        return json.load(f)


def set_properties(props, resource, guid):
    path = _path(resource, guid, 'properties')
    if not exists(dirname(path)):
        os.makedirs(dirname(path))
    with file(path, 'w') as f:
        json.dump(props, f)


def get_blob(resource, guid, prop):
    path = _path(resource, guid, prop)
    mime_path = path + '.mime'

    if exists(path) and exists(mime_path):
        return path, mime_path

    if isdir(path):
        shutil.rmtree(path)

    response = http.raw_request('GET', [resource, guid, prop],
            allow_redirects=True)

    if not exists(dirname(path)):
        os.makedirs(dirname(path))

    with file(mime_path, 'w') as f:
        f.write(response.headers.get('Content-Type') or \
                'application/octet-stream')

    def download(f):
        _logger.debug('Download "%s" BLOB', path)

        length = int(response.headers.get('Content-Length', _CHUNK_SIZE))
        chunk_size = min(length, _CHUNK_SIZE)

        for chunk in response.iter_content(chunk_size=chunk_size):
            f.write(chunk)

    if resource == 'implementation' and prop == 'bundle':
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            download(tmp_file)
            tmp_file.close()
            with Bundle(tmp_file.name, 'application/zip') as bundle:
                bundle.extractall(path)
        finally:
            os.unlink(tmp_file.name)
    else:
        with file(path, 'w') as f:
            download(f)

    return path, mime_path


def _path(resource, guid, *args):
    cachedir = env.cachedir.value
    if not cachedir:
        cachedir = sugar.profile_path('cache')
    return join(cachedir, resource, guid[:2], guid, *args)


def _resolve_path(*args):
    return sugar.profile_path('cache', 'context', 'implement', *args)


def _store_resolve_result(guid, names):
    for name in names:
        path = _resolve_path(name)
        if not exists(dirname(path)):
            os.makedirs(dirname(path))
        if lexists(path):
            os.unlink(path)
        os.symlink(join('..', guid[:2], guid), path)
