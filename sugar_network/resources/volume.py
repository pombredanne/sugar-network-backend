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
from sugar_network import client, node, toolkit, static
from sugar_network.toolkit.sneakernet import DiskFull
from sugar_network.toolkit.collection import Sequence
from sugar_network.toolkit import http, router
from active_toolkit.sockets import BUFFER_SIZE
from active_toolkit import coroutine, enforce


AUTHORITY_AUTHOR = 1

_DIFF_CHUNK = 1024

_logger = logging.getLogger('resources.volume')


def _reprcast_authors(value):
    if not value:
        return []
    if isinstance(value, basestring):
        return [value]
    result = []
    for i in value:
        if isinstance(i, basestring):
            result.append(i)
        else:
            result.append(i[0])
    return result


class Resource(ad.Document):

    @ad.active_property(prefix='RU', typecast=dict, default={},
            reprcast=lambda x: x.keys() if isinstance(x, dict) else x,
            permissions=ad.ACCESS_READ)
    def authority(self, value):
        return value

    @authority.setter
    def authority(self, value):
        if not self.request.principal:
            return {}
        value = {self.request.principal: AUTHORITY_AUTHOR}
        self['author'] = self._populate_authors(value, None)
        return value

    @ad.document_command(method='PUT', cmd='useradd',
            arguments={'role': ad.to_int},
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def useradd(self, user, role=0):
        authority = self['authority']
        authority[user] = role
        self.directory.update(self.guid, authority=authority,
                author=self._populate_authors(authority, None))

    @ad.document_command(method='PUT', cmd='userdel',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def userdel(self, user):
        enforce(user != self.request.principal, 'Cannot remove yourself')
        authority = self['authority']
        if user not in authority:
            return
        del authority[user]
        self.directory.update(self.guid, authority=authority,
                author=self._populate_authors(authority, None))

    @ad.active_property(prefix='RA', full_text=True, default=[],
            reprcast=_reprcast_authors)
    def author(self, value):
        return _reprcast_authors(value)

    @author.setter
    def author(self, value):
        if not value:
            return []
        if isinstance(value, basestring):
            value = [(value, True)]
        else:
            value = [(i, True) for i in value]
        return self._populate_authors(self['authority'], value)

    @ad.active_property(prefix='RL', typecast=[], default=['public'])
    def layer(self, value):
        return value

    @ad.active_property(prefix='RT', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value

    def _populate_authors(self, authority, authors):
        if authors is None:
            authors = []
            for user, orig in self['author'] or []:
                if orig:
                    authors.append((user, orig))

        users = self.volume['user']
        for user_guid in sorted(authority.keys()):
            if not users.exists(user_guid):
                _logger.warning('No %r user to set author property', user_guid)
                continue
            user = users.get(user_guid)
            if user['name']:
                authors.append((user['name'], False))

        return authors


class Volume(ad.SingleVolume):

    RESOURCES = (
            'sugar_network.resources.artifact',
            'sugar_network.resources.comment',
            'sugar_network.resources.context',
            'sugar_network.resources.implementation',
            'sugar_network.resources.notification',
            'sugar_network.resources.feedback',
            'sugar_network.resources.report',
            'sugar_network.resources.review',
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

    def __init__(self):
        self._notifier = coroutine.AsyncResult()
        self.connect(lambda event: self._notify(event))

    def connect(self, callback, condition=None, **kwargs):
        raise NotImplementedError()

    @router.route('GET', '/robots.txt')
    def robots(self, request, response):
        response.content_type = 'text/plain'
        return _ROBOTS_TXT

    @router.route('GET', '/favicon.ico')
    def favicon(self, request, response):
        return ad.PropertyMeta(
                path=join(static.PATH, 'favicon.ico'),
                mime_type='image/x-icon')

    @ad.volume_command(method='GET', mime_type='text/html')
    def hello(self):
        return _HELLO_HTML

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

    @ad.directory_command_post(method='GET')
    def _VolumeCommands_find_post(self, request, response, result):
        self._mixin_blobs(request, result['result'])
        return result

    @ad.document_command_pre(method='GET', arguments={'reply': ad.to_list})
    def _VolumeCommands_get_pre(self, request):
        if 'reply' not in request:
            reply = request['reply'] = []
            for prop in self.volume[request['document']].metadata.values():
                if prop.permissions & ad.ACCESS_READ and \
                        not (prop.permissions & ad.ACCESS_LOCAL):
                    reply.append(prop.name)

    @ad.document_command_post(method='GET')
    def _VolumeCommands_get_post(self, request, response, result):
        self._mixin_blobs(request, [result])
        return result

    def _mixin_blobs(self, request, result):
        blobs = []
        metadata = self.volume[request['document']].metadata
        for prop in request['reply']:
            if isinstance(metadata[prop], ad.BlobProperty):
                blobs.append(prop)
        if not blobs:
            return

        requested_guid = request.get('guid')
        enforce(requested_guid or 'guid' in request['reply'],
                'No way to get BLOB urls if GUID was not specified')

        if node.static_url.value:
            prefix = node.static_url.value
        elif hasattr(request, 'environ'):
            prefix = 'http://' + request.environ['HTTP_HOST']
        else:
            prefix = 'http://localhost:%s' % client.ipc_port.value
        if request.mountpoint in (None, '/'):
            postfix = ''
        else:
            postfix = '?mountpoint=' + request.mountpoint

        for props in result:
            for name in blobs:
                url = props[name].get('url')
                if url is None:
                    url = '/'.join([
                        '',
                        request['document'],
                        props.get('guid') or requested_guid,
                        name,
                        ]) + postfix
                if url.startswith('/'):
                    url = prefix + url
                props[name] = url


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Consult <a href="http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""

_ROBOTS_TXT = """\
User-agent: *
Disallow: /
"""
