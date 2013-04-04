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

import json
import logging

from sugar_network import db
from sugar_network.toolkit import coroutine, enforce


AUTHOR_INSYSTEM = 1
AUTHOR_ORIGINAL = 2
AUTHOR_ALL = (AUTHOR_INSYSTEM | AUTHOR_ORIGINAL)


_logger = logging.getLogger('resources.volume')


def _reprcast_authors(value):
    for guid, props in value.items():
        if 'name' in props:
            yield props['name']
        yield guid


class Resource(db.Document):

    @db.indexed_property(prefix='RA', typecast=dict, full_text=True,
            default={}, reprcast=_reprcast_authors, permissions=db.ACCESS_READ)
    def author(self, value):
        result = []
        for guid, props in sorted(value.items(),
                cmp=lambda x, y: cmp(x[1]['order'], y[1]['order'])):
            if 'name' in props:
                result.append({
                    'guid': guid,
                    'name': props['name'],
                    'role': props['role'],
                    })
            else:
                result.append({
                    'name': guid,
                    'role': props['role'],
                    })
        return result

    @author.setter
    def author(self, value):
        if not self.request.principal:
            return {}
        return self._useradd(self.request.principal, AUTHOR_ORIGINAL)

    @db.document_command(method='PUT', cmd='useradd',
            arguments={'role': db.to_int},
            permissions=db.ACCESS_AUTH | db.ACCESS_AUTHOR)
    def useradd(self, user, role):
        enforce(user, "Argument 'user' is not specified")
        self.directory.update(self.guid, author=self._useradd(user, role))

    @db.document_command(method='PUT', cmd='userdel',
            permissions=db.ACCESS_AUTH | db.ACCESS_AUTHOR)
    def userdel(self, user):
        enforce(user, "Argument 'user' is not specified")
        enforce(user != self.request.principal, 'Cannot remove yourself')
        author = self['author']
        enforce(user in author, 'No such user')
        del author[user]
        self.directory.update(self.guid, author=author)

    @db.indexed_property(prefix='RL', typecast=[], default=['public'])
    def layer(self, value):
        return value

    @db.indexed_property(prefix='RT', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value

    def _useradd(self, user, role):
        props = {}

        if role is None:
            role = 0
        users = self.volume['user']
        if users.exists(user):
            props['name'] = users.get(user)['name']
            role |= AUTHOR_INSYSTEM
        else:
            role &= ~AUTHOR_INSYSTEM
        props['role'] = role & AUTHOR_ALL

        author = self['author'] or {}
        if user in author:
            author[user].update(props)
        else:
            if author:
                order = max(author.values(), key=lambda x: x['order'])['order']
                props['order'] = order + 1
            else:
                props['order'] = 0
            author[user] = props

        return author


class Volume(db.Volume):

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
        self._populators = coroutine.Pool()
        db.Volume.__init__(self, root, document_classes, lazy_open=lazy_open)

    def close(self):
        self._populators.kill()
        db.Volume.close(self)

    def notify(self, event):
        if event['event'] == 'update' and 'props' in event and \
                'deleted' in event['props'].get('layer', []):
            event['event'] = 'delete'
            del event['props']

        db.Volume.notify(self, event)

    def _open(self, name, document):
        directory = db.Volume._open(self, name, document)
        self._populators.spawn(self._populate, directory)
        return directory

    def _populate(self, directory):
        for __ in directory.populate():
            coroutine.dispatch()


class Commands(object):

    def __init__(self):
        self._pooler = _Pooler()

    @db.volume_command(method='GET', cmd='subscribe',
            mime_type='text/event-stream')
    def subscribe(self, request=None, response=None, ping=False, **condition):
        """Subscribe to Server-Sent Events."""
        if request is not None and not condition:
            condition = request.query
        if response is not None:
            response.content_type = 'text/event-stream'
            response['Cache-Control'] = 'no-cache'
        peer = 'anonymous'
        if hasattr(request, 'environ'):
            peer = request.environ.get('HTTP_SUGAR_USER') or peer
        return self._pull_events(peer, ping, condition)

    @db.volume_command(method='POST', cmd='broadcast',
            mime_type='application/json', permissions=db.ACCESS_LOCAL)
    def broadcast(self, event=None, request=None):
        if request is not None:
            event = request.content
        _logger.debug('Publish event: %r', event)
        self._pooler.notify_all(event)
        coroutine.dispatch()

    def _pull_events(self, peer, ping, condition):
        _logger.debug('Start pulling events to %s user', peer)

        if ping:
            # XXX The whole commands' kwargs handling should be redesigned
            if 'ping' in condition:
                condition.pop('ping')
            # If non-greenlet application needs only to initiate
            # a subscription and do not stuck in waiting for the first event,
            # it should pass `ping` argument to return fake event to unblock
            # `GET /?cmd=subscribe` call.
            yield 'data: %s\n\n' % json.dumps({'event': 'pong'})

        try:
            while True:
                event = self._pooler.wait()
                for key, value in condition.items():
                    if value.startswith('!'):
                        if event.get(key) == value[1:]:
                            break
                    elif event.get(key) != value:
                        break
                else:
                    yield 'data: %s\n\n' % json.dumps(event)
        finally:
            _logger.debug('Stop pulling events to %s user', peer)


class _Pooler(object):
    """One-producer-to-many-consumers events delivery."""

    def __init__(self):
        self._value = None
        self._waiters = 0
        self._ready = coroutine.Event()
        self._open = coroutine.Event()
        self._open.set()

    def wait(self):
        self._open.wait()
        self._waiters += 1
        try:
            self._ready.wait()
        finally:
            self._waiters -= 1
            if self._waiters == 0:
                self._ready.clear()
                self._open.set()
        return self._value

    def notify_all(self, value=None):
        self._open.wait()
        if not self._waiters:
            return
        self._open.clear()
        self._value = value
        self._ready.set()
