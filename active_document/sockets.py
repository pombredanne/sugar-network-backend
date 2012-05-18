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

"""Utilities to work with sockets.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/socket.py$
$Data: 2012-05-15$

"""

import os
import sys
import json
import errno
import struct
import random
import tempfile
from os.path import join, isfile, relpath
from gettext import gettext as _

from . import util
enforce = util.enforce


BUFFER_SIZE = 1024 * 10


class SocketFile(object):

    def __init__(self, socket):
        self._socket = socket
        self._message_buffer = bytearray('\0' * BUFFER_SIZE)
        self._read_size = None

    @property
    def socket(self):
        return self._socket

    def write_message(self, message):
        try:
            message_str = json.dumps(message)
        except Exception, error:
            raise RuntimeError(_('Cannot encode %r message: %s') % \
                    (message, error))
        self.write(message_str)

    def read_message(self):
        message_str = self.read()
        if not message_str:
            return None
        try:
            message = json.loads(message_str)
        except Exception, error:
            raise RuntimeError(_('Cannot decode "%s" message: %s') % \
                    (message_str, error))
        return message

    def write(self, data, size=None):
        if data is None:
            data = ''

        if hasattr(data, 'read'):
            enforce(size)
        else:
            size = len(data)

        size_str = struct.pack('i', size)
        self._socket.send(size_str)

        if hasattr(data, 'read'):
            while size:
                chunk_size = min(size, BUFFER_SIZE)
                # pylint: disable-msg=E1103
                self._socket.send(data.read(chunk_size))
                size -= chunk_size
        else:
            self._socket.send(data)

    def read(self, size=None):

        def read_size():
            size_str = self._recv(struct.calcsize('i'))
            if not size_str:
                return 0
            return struct.unpack('i', size_str)[0]

        if size is None:
            chunks = []
            size = read_size()
            while size:
                chunk = self._recv(min(size, BUFFER_SIZE))
                if not chunk:
                    break
                chunks.append(chunk)
                size -= len(chunk)
            return ''.join(chunks)
        else:
            if self._read_size is None:
                self._read_size = read_size()
            if self._read_size:
                chunk = self._recv(min(self._read_size, BUFFER_SIZE, size))
            else:
                chunk = ''
            if not chunk:
                self._read_size = None
            else:
                self._read_size -= len(chunk)
            return chunk

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def _recv(self, size):
        while True:
            try:
                chunk = self._socket.recv(size)
            except OSError, error:
                if error.errno == errno.EINTR:
                    continue
                raise
            return chunk

    def __repr__(self):
        return repr(self._socket)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self):
        while True:
            chunk = self.read(BUFFER_SIZE)
            if not chunk:
                break
            yield chunk

    def __getattr__(self, name):
        return getattr(self._socket, name)


def encode_multipart(data, boundary=None):
    """Encode data into a multipart encoded stream.

    :param data:
        a `dict` with filename and data object (stream or buffer), or,
        an iterator that returns the same tuples
    :param boundary:
        boundary string to separate multipart portions; if not specified,
        will be set default one
    :returns:
        an iterator that returns encoded data by chunks

    """
    if boundary is None:
        boundary = _MULTIPART_BOUNDARY % random.randrange(sys.maxint)

    if type(data) is dict:
        data = data.items()

    for filename, content in data:
        yield _multipart_header(boundary, filename)
        if hasattr(content, 'read'):
            while True:
                chunk = content.read(BUFFER_SIZE)
                if not chunk:
                    break
                yield chunk
        else:
            if isinstance(content, unicode):
                content = content.encode('utf-8')
            yield content
        yield '\r\n'

    yield _multipart_tail(boundary)


def encode_directory(path, boundary=None):
    """Encode directory content as a multipart stream.

    Function calculate number of files to encode, returns stat object, then,
    encode data.

    :param path:
        path to directory to encode
    :param boundary:
        boundary string to separate multipart portions; if not specified,
        will be set default one
    :returns:
        a tuple of `MultipartInfo` object and an iterator that returns
        encoded data by chunks

    """
    if boundary is None:
        boundary = _MULTIPART_BOUNDARY % random.randrange(sys.maxint)

    info = MultipartInfo()
    info.boundary = boundary
    info.content_type = 'multipart/mixed; boundary="%s"' % boundary

    to_encode = []

    for root, __, files in os.walk(path):
        for filename in files:
            filename_path = join(root, filename)
            if not isfile(filename_path):
                continue
            name = relpath(filename_path, path)
            info.content_length += len(_multipart_header(boundary, name)) + \
                    os.stat(filename_path).st_size + 2
            info.files_number += 1
            to_encode.append((filename_path, name))
    info.content_length += len(_multipart_tail(boundary))

    def feeder():
        for path, filename in to_encode:
            with file(path, 'rb') as f:
                yield filename, f

    return info, encode_multipart(feeder(), boundary)


def decode_multipart(stream, size, boundary):
    """Decode multipart data.

    :param stream:
        object that has `read()` to restore data from
    :param size:
        number of bytes to read from `stream`
    :param boundary:
        boundary string that separates multipart portions
    :returns:
        iterator object that returns `(filename, stream)` for each
        multipart portion

    """
    from werkzeug.formparser import FormDataParser

    if hasattr(stream, 'recv'):
        stream = SocketFile(stream)

    parser = FormDataParser(silent=False,
            stream_factory=lambda * args: tempfile.NamedTemporaryFile())
    __, __, files = parser.parse(stream, 'multipart/form-data', size,
            {'boundary': boundary})

    for __, chunk in files.iteritems(multi=True):
        yield chunk.filename, chunk.stream
        chunk.close()


class MultipartInfo(object):

    content_length = 0
    files_number = 0
    boundary = None
    content_type = None


def _multipart_header(boundary, filename):
    return """\
--%s\r
Content-Disposition: attachment; filename="%s"\r
Content-Type: application/octet-stream\r
\r
""" % (boundary, filename)


def _multipart_tail(boundary):
    return '--%s--\r\n' % boundary


_MULTIPART_BOUNDARY = ('=' * 15) + '%%0%dd' % len(repr(sys.maxint-1)) + '=='
