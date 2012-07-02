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

import logging
import tempfile
from gettext import gettext as _

import requests
from requests.sessions import Session

# Let http work in concurrence
# TODO Is it safe for the rest of code?
from gevent.monkey import patch_socket
patch_socket(dns=False)

from active_toolkit.sockets import BUFFER_SIZE


_logger = logging.getLogger('http')
_session = None


def reset():
    global _session
    _session = None


def download(url):
    _logger.debug('Download %r', url)

    response = request('GET', url, allow_redirects=True)
    content_length = response.headers.get('Content-Length')
    content_length = int(content_length) if content_length else 0

    ostream = tempfile.NamedTemporaryFile()
    try:
        chunk_size = min(content_length, BUFFER_SIZE)
        for chunk in response.iter_content(chunk_size=chunk_size):
            ostream.write(chunk)
    except Exception:
        ostream.close()
        raise

    ostream.seek(0)
    return ostream


def request(method, path, data=None, headers=None, allowed_response=None,
        **kwargs):
    global _session

    if _session is None:
        _session = Session()

    try:
        response = requests.request(method, path, data=data,
                headers=headers, session=_session, **kwargs)
    except requests.exceptions.SSLError:
        _logger.warning(_('Pass --no-check-certificate ' \
                'to avoid SSL checks'))
        raise

    if response.status_code != 200:
        _logger.debug('Got %s HTTP error for %r request',
                response.status_code, path)
        response.raise_for_status()

    return response
