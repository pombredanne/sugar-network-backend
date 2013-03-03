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

from sugar_network.toolkit import util, coroutine, enforce


_logger = logging.getLogger('node.sync')


def diff(volume, in_seq, out_seq=None, exclude_seq=None, layer=None, **kwargs):
    if out_seq is None:
        out_seq = util.Sequence([])
    is_initial_diff = not out_seq

    try:
        for document, directory in volume.items():
            coroutine.dispatch()
            directory.commit()
            yield {'document': document}
            for guid, patch in directory.diff(in_seq, out_seq, exclude_seq,
                    layer=layer
                            if document in ('context', 'implementation')
                            else None,
                    **kwargs):
                coroutine.dispatch()
                yield {'diff': patch, 'guid': guid}

        if is_initial_diff:
            # There is only one diff, so, we can stretch it to remove all holes
            out_seq.stretch()
    except StopIteration:
        pass

    yield {'commit': out_seq}


def merge(volume, records, shift_seqno=True):
    directory = None
    commit_seq = util.Sequence()
    merged_seq = util.Sequence()
    synced = False

    for record in records:
        document = record.get('document')
        if document is not None:
            directory = volume[document]
            continue

        patch = record.get('diff')
        if patch is not None:
            enforce(directory is not None,
                    'Invalid merge, no document')
            seqno, merged = directory.merge(record['guid'], patch, shift_seqno)
            synced = synced or merged
            if seqno is not None:
                merged_seq.include(seqno, seqno)
            continue

        commit = record.get('commit')
        if commit is not None:
            commit_seq.include(commit)
            continue

    if synced:
        volume.notify({'event': 'sync'})

    return commit_seq, merged_seq
