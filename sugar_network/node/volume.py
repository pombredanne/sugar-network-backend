# Copyright (C) 2012-2013 Aleksey Lim
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

from sugar_network import toolkit
from sugar_network.toolkit.router import Request
from sugar_network.toolkit import http, coroutine, enforce


# Apply node level layer for these documents
_LIMITED_RESOURCES = ('context', 'release')

_logger = logging.getLogger('node.volume')


def diff(volume, in_seq, out_seq=None, exclude_seq=None, layer=None,
        fetch_blobs=False, ignore_documents=None, **kwargs):
    connection = http.Connection()
    if out_seq is None:
        out_seq = toolkit.Sequence([])
    is_the_only_seq = not out_seq
    if layer:
        if isinstance(layer, basestring):
            layer = [layer]
        layer.append('common')
    try:
        for resource, directory in volume.items():
            if ignore_documents and resource in ignore_documents:
                continue
            coroutine.dispatch()
            directory.commit()
            yield {'resource': resource}
            for guid, patch in directory.diff(in_seq, exclude_seq,
                    layer=layer if resource in _LIMITED_RESOURCES else None):
                adiff = {}
                adiff_seq = toolkit.Sequence()
                for prop, meta, seqno in patch:
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
                        adiff[prop] = meta
                    adiff_seq.include(seqno, seqno)
                if adiff:
                    yield {'guid': guid, 'diff': adiff}
                out_seq.include(adiff_seq)
        if is_the_only_seq:
            # There is only one diff, so, we can stretch it to remove all holes
            out_seq.stretch()
    except StopIteration:
        pass

    yield {'commit': out_seq}


def merge(volume, records, shift_seqno=True, stats=None):
    resource = None
    directory = None
    commit_seq = toolkit.Sequence()
    merged_seq = toolkit.Sequence()
    synced = False

    for record in records:
        resource_ = record.get('resource')
        if resource_:
            resource = resource_
            directory = volume[resource_]
            continue

        if 'guid' in record:
            guid = record['guid']
            layer = []
            existed = directory.exists(guid)
            if existed:
                layer = directory.get(guid)['layer']

            def update_stats(upd):
                method = 'PUT' if existed else 'POST'
                if ('deleted' in layer) != ('deleted' in upd.get('layer', [])):
                    if 'deleted' in layer:
                        # TODO
                        enforce(not 'supported yet')
                    else:
                        method = 'DELETE'
                stats.log(Request(
                        method=method,
                        path=[resource, guid],
                        content=upd,
                        ))

            if stats is not None:
                record['op'] = update_stats
            seqno, merged = directory.merge(shift_seqno=shift_seqno, **record)
            synced = synced or merged
            if seqno is not None:
                merged_seq.include(seqno, seqno)
            continue

        commit = record.get('commit')
        if commit is not None:
            commit_seq.include(commit)
            continue

    if synced:
        volume.broadcast({'event': 'sync'})

    return commit_seq, merged_seq
