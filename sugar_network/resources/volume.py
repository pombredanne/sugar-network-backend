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

import json
import logging
from os.path import join

import active_document as ad
from active_document import directory as ad_directory
from sugar_network import local, node, toolkit
from sugar_network.toolkit.sneakernet import DiskFull
from sugar_network.toolkit.collection import Sequence
from sugar_network.toolkit import http
from active_toolkit.sockets import BUFFER_SIZE
from active_toolkit import coroutine, enforce


ad_directory._LAYOUT_VERSION = 2
_DIFF_CHUNK = 1024

_logger = logging.getLogger('resources.volume')


class Request(ad.Request):

    principal = None
    mountpoint = None


class Resource(ad.Document):

    @ad.active_property(prefix='RU', typecast=[], default=[],
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def user(self, value):
        return value

    @ad.active_property(prefix='RL', typecast=[], default=['public'])
    def layer(self, value):
        return value

    @ad.active_property(prefix='RA', full_text=True, default=[], typecast=[],
            permissions=ad.ACCESS_READ)
    def author(self, value):
        return value

    @ad.active_property(prefix='RT', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value


class Volume(ad.SingleVolume):

    RESOURCES = (
            'sugar_network.resources.artifact',
            'sugar_network.resources.comment',
            'sugar_network.resources.context',
            'sugar_network.resources.implementation',
            'sugar_network.resources.notification',
            'sugar_network.resources.feedback',
            'sugar_network.resources.report',
            'sugar_network.resources.solution',
            'sugar_network.resources.user',
            )

    def __init__(self, root, document_classes=None, lazy_open=False):
        if document_classes is None:
            document_classes = Volume.RESOURCES
        self._downloader = None
        self._populators = coroutine.Pool()
        ad.SingleVolume.__init__(self, root, document_classes, lazy_open)

    def close(self):
        if self._downloader is not None:
            self._downloader.close()
            self._downloader = None
        self._populators.kill()
        ad.SingleVolume.close(self)

    def notify(self, event):
        if event['event'] == 'update' and 'props' in event and \
                'deleted' in event['props'].get('layer', []):
            event['event'] = 'delete'
            del event['props']

        ad.SingleVolume.notify(self, event)

    def merge(self, record, increment_seqno=True):
        coroutine.dispatch()
        if record.get('content_type') == 'blob':
            diff = record['blob']
        else:
            diff = record['diff']
        return self[record['document']].merge(record['guid'], diff,
                increment_seqno=increment_seqno)

    def diff(self, in_seq, out_packet):
        # Since `in_seq` will be changed in `patch()`, original sequence
        # should be passed as-is to every document's `diff()` because
        # seqno handling is common for all documents
        orig_seq = Sequence(in_seq)
        push_seq = Sequence()

        for document, directory in self.items():
            coroutine.dispatch()
            directory.commit()

            def patch():
                for guid, seqno, diff in \
                        directory.diff(orig_seq, limit=_DIFF_CHUNK):
                    coroutine.dispatch()

                    for prop, value in diff.items():
                        if 'path' in value:
                            data = file(value.pop('path'), 'rb')
                        elif 'url' in value:
                            data = self._download_blob(value.pop('url'))
                        else:
                            continue
                        del diff[prop]
                        arcname = join(document, 'blobs', guid, prop)
                        out_packet.push(data, arcname=arcname, cmd='sn_push',
                                document=document, guid=guid, **value)

                    if not diff:
                        continue

                    yield {'guid': guid, 'diff': diff}

                    # Update `in_seq`, it might be reused by caller
                    in_seq.exclude(seqno, seqno)
                    push_seq.include(seqno, seqno)

            try:
                out_packet.push(patch(), arcname=join(document, 'diff'),
                        cmd='sn_push', document=document)
            except DiskFull:
                if push_seq:
                    out_packet.push(force=True, cmd='sn_commit',
                            sequence=push_seq)
                raise

        if push_seq:
            # Only here we can collapse `push_seq` since seqno handling
            # is common for all documents; if there was an exception before
            # this place, `push_seq` should contain not-collapsed sequence
            orig_seq.floor(push_seq.last)
            out_packet.push(force=True, cmd='sn_commit', sequence=orig_seq)

    def _open(self, name, document):
        directory = ad.SingleVolume._open(self, name, document)
        self._populators.spawn(self._populate, directory)
        return directory

    def _populate(self, directory):
        for __ in directory.populate():
            coroutine.dispatch()

    def _download_blob(self, url):
        _logger.debug('Download %r blob', url)

        if self._downloader is None:
            self._downloader = http.Client()

        response = self._downloader.request('GET', url, allow_redirects=True)
        content_length = response.headers.get('Content-Length')
        content_length = int(content_length) if content_length else 0

        ostream = toolkit.NamedTemporaryFile()
        try:
            chunk_size = min(content_length, BUFFER_SIZE)
            # pylint: disable-msg=E1103
            for chunk in response.iter_content(chunk_size=chunk_size):
                ostream.write(chunk)
        except Exception:
            ostream.close()
            raise

        ostream.seek(0)
        return ostream


class Commands(object):

    volume = None

    def __init__(self):
        self._notifier = coroutine.AsyncResult()
        self.connect(lambda event: self._notify(event))

    def connect(self, callback, condition=None, **kwargs):
        raise NotImplementedError()

    @ad.volume_command(method='GET', cmd='subscribe',
            mime_type='application/json')
    def subscribe(self, request=None, response=None, only_commits=False):
        """Subscribe to Server-Sent Events.

        :param only_commits:
            subscribers can be notified only with "commit" events;
            that is useful to minimize interactions between server and clients

        """
        if response is not None:
            response.content_type = 'text/event-stream'
            response['Cache-Control'] = 'no-cache'
        peer = 'anonymous'
        if hasattr(request, 'environ'):
            peer = request.environ.get('HTTP_SUGAR_USER') or peer
        return self._pull_events(peer, only_commits)

    @ad.directory_command_post(method='GET')
    def _Commands_find_post(self, request, response, result):
        self._mixin_blobs(request, result['result'])
        return result

    @ad.document_command_post(method='GET')
    def _Commands_get_post(self, request, response, result):
        self._mixin_blobs(request, [result])
        return result

    def _mixin_blobs(self, request, result):
        requested_guid = request.get('guid')
        if node.static_url.value:
            prefix = node.static_url.value
        elif hasattr(request, 'environ'):
            prefix = 'http://' + request.environ['HTTP_HOST']
        else:
            prefix = 'http://localhost:%s' % local.ipc_port.value
        if request.mountpoint in (None, '/'):
            postfix = ''
        else:
            postfix = '?mountpoint=' + request.mountpoint
        document = request['document']

        for props in result:
            guid = props.get('guid') or requested_guid
            for name, value in props.items():
                if not isinstance(value, ad.PropertyMeta):
                    continue
                enforce(guid, 'No way to get BLOB urls if %r was not '
                        'in %r parameter', 'guid', 'reply')
                props[name] = value.url(
                        default='/'.join(['', document, guid, name]) + postfix,
                        prefix=prefix)

    def _pull_events(self, peer, only_commits):
        _logger.debug('Start pulling events to %s user', peer)
        # Otherwise, gevent's WSGI server doesn't sent HTTP status
        yield '\n'

        try:
            while True:
                event = self._notifier.get()
                if only_commits:
                    if event['event'] != 'commit':
                        continue
                else:
                    if event['event'] == 'commit':
                        # Subscribers already got update notifications enough
                        continue
                yield 'data: %s\n\n' % json.dumps(event)
        finally:
            _logger.debug('Stop pulling events to %s user', peer)

    def _notify(self, event):
        _logger.debug('Publish event: %r', event)
        self._notifier.set(event)
        self._notifier = coroutine.AsyncResult()
        coroutine.dispatch()


class VolumeCommands(ad.VolumeCommands):

    @ad.document_command(method='GET', cmd='deplist',
            mime_type='application/json')
    def deplist(self, document, guid, repo):
        """List of native packages context is dependening on.

        Command return only GNU/Linux package names and ignores
        Sugar Network dependencies.

        :param repo:
            OBS repository name to get package names for, e.g.,
            Fedora-14
        :returns:
            list of package names

        """
        enforce(document == 'context')
        enforce(repo, 'Argument %r should be set', 'repo')
        context = self.volume['context'].get(guid)

        result = []

        for package in context['dependencies']:
            dep = self.volume['context'].get(package)
            enforce(repo in dep['packages'],
                    'No packages for %r on %r', package, repo)
            result.extend(dep['packages'][repo].get('binary') or [])

        return result
