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

from sugar_network import toolkit
from sugar_network.toolkit import http, enforce


class Digest(str):
    pass


def post(content, meta=None):
    # if fileobj is tmp then move files
    pass


def update(digest, meta):
    pass


def get(digest):
    pass


def delete(digest):
    pass


def path(digest):
    pass






"""

def diff(volume, in_seq, out_seq=None, exclude_seq=None, layer=None,
        fetch_blobs=False, ignore_documents=None, **kwargs):

                    if 'blob' in meta:
                        blob_path = meta.pop('blob')
                        yield {'guid': guid,
                               'diff': {prop: meta},
                               'blob_size': meta['blob_size'],
                               'blob': toolkit.iter_file(blob_path),
                               }
                    elif fetch_blobs and 'url' in meta:
                        url = meta.pop('url')
                        try:
                            blob = connection.request('GET', url,
                                    allow_redirects=True,
                                    # We need uncompressed size
                                    headers={'Accept-Encoding': ''})
                        except Exception:
                            _logger.exception('Cannot fetch %r for %s:%s:%s',
                                    url, resource, guid, prop)
                            is_the_only_seq = False
                            continue
                        yield {'guid': guid,
                               'diff': {prop: meta},
                               'blob_size':
                                    int(blob.headers['Content-Length']),
                               'blob': blob.iter_content(toolkit.BUFFER_SIZE),
                               }
                    else:

















                    'digest': hashlib.sha1(png.getvalue()).hexdigest(),




                if value is None:
                    value = {'blob': None}
                elif isinstance(value, basestring) or hasattr(value, 'read'):
                    value = _read_blob(request, prop, value)
                    blobs.append(value['blob'])
                elif isinstance(value, dict):
                    enforce('url' in value or 'blob' in value, 'No bundle')
                else:
                    raise RuntimeError('Incorrect BLOB value')

def _read_blob(request, prop, value):
    digest = hashlib.sha1()
    dst = toolkit.NamedTemporaryFile(delete=False)

    try:
        if isinstance(value, basestring):
            digest.update(value)
            dst.write(value)
        else:
            size = request.content_length or sys.maxint
            while size > 0:
                chunk = value.read(min(size, toolkit.BUFFER_SIZE))
                if not chunk:
                    break
                dst.write(chunk)
                size -= len(chunk)
                digest.update(chunk)
    except Exception:
        os.unlink(dst.name)
        raise
    finally:
        dst.close()

    if request.prop and request.content_type:
        mime_type = request.content_type
    else:
        mime_type = prop.mime_type

    return {'blob': dst.name,
            'digest': digest.hexdigest(),
            'mime_type': mime_type,
            }

)
"""
