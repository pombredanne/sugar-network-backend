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

import os
import logging

from sugar_network.toolkit import BUFFER_SIZE, http, util, coroutine, enforce


# Apply node level layer for these documents
_LIMITED_RESOURCES = ('context', 'implementation')

_logger = logging.getLogger('node.volume')


def diff(volume, in_seq, out_seq=None, exclude_seq=None, layer=None,
        fetch_blobs=False, **kwargs):
    connection = http.Client()
    if out_seq is None:
        out_seq = util.Sequence([])
    is_initial_diff = not out_seq
    if layer:
        if isinstance(layer, basestring):
            layer = [layer]
        layer.append('common')
    try:
        for document, directory in volume.items():
            coroutine.dispatch()
            directory.commit()
            yield {'document': document}
            for guid, patch in directory.diff(in_seq, exclude_seq,
                    layer=layer if document in _LIMITED_RESOURCES else None):
                adiff = {}
                adiff_seq = util.Sequence()
                for prop, meta, seqno in patch:
                    if 'blob' in meta:
                        blob_path = meta.pop('blob')
                        yield {'guid': guid,
                               'diff': {prop: meta},
                               'blob_size': os.stat(blob_path).st_size,
                               'blob': util.iter_file(blob_path),
                               }
                    elif fetch_blobs and 'url' in meta:
                        blob = connection.request('GET',
                                meta.pop('url'), allow_redirects=True,
                                # We need uncompressed size
                                headers={'Accept-Encoding': ''})
                        yield {'guid': guid,
                               'diff': {prop: meta},
                               'blob_size':
                                    int(blob.headers['Content-Length']),
                               'blob': blob.iter_content(BUFFER_SIZE),
                               }
                    else:
                        adiff[prop] = meta
                    adiff_seq.include(seqno, seqno)
                if adiff:
                    yield {'guid': guid, 'diff': adiff}
                out_seq.include(adiff_seq)
        if is_initial_diff:
            # There is only one diff, so, we can stretch it to remove all holes
            out_seq.stretch()
    except StopIteration:
        pass

    yield {'commit': out_seq}


def merge(volume, records, shift_seqno=True, node_stats=None):
    document = None
    directory = None
    commit_seq = util.Sequence()
    merged_seq = util.Sequence()
    synced = False

    for record in records:
        document_ = record.get('document')
        if document_:
            document = document_
            directory = volume[document_]
            continue

        if 'guid' in record:
            enforce(document, 'Invalid merge, no document')

            seqno, merged = directory.merge(shift_seqno=shift_seqno, **record)
            synced = synced or merged
            if seqno is not None:
                merged_seq.include(seqno, seqno)

            if node_stats is not None and document == 'review':
                request = _Request()
                request['document'] = document
                request['method'] = 'POST'
                patch = record['diff']
                request.content = {
                        'context': patch['context']['value'],
                        'rating': patch['rating']['value'],
                        }
                if 'artifact' in patch:
                    request.content['artifact'] = patch['artifact']['value']
                node_stats.log(request)
            continue

        commit = record.get('commit')
        if commit is not None:
            commit_seq.include(commit)
            continue

    if synced:
        volume.notify({'event': 'sync'})

    return commit_seq, merged_seq


class _Request(dict):

    principal = None
    content = None
