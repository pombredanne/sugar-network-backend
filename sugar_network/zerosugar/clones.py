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

import os
import shutil
import hashlib
import logging
from os.path import join, exists, lexists, relpath, dirname, basename, isdir
from os.path import abspath, islink

from sugar_network.zerosugar.spec import Spec
from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_CLOSE_WRITE, \
        IN_MOVED_TO, IN_MOVED_FROM
from active_document import DEFAULT_LANG
from sugar_network.toolkit import sugar
from sugar_network import toolkit, local
from active_toolkit import coroutine, util


_logger = logging.getLogger('zerosugar.clones')


def walk(context):
    root = _context_path(context, '')
    if not exists(root):
        return

    for filename in os.listdir(root):
        path = join(root, filename)
        if exists(path):
            yield os.readlink(path)


def ensure_clones(context):
    root = _context_path(context, '')
    if not exists(root):
        return False

    found = False

    for filename in os.listdir(root):
        path = join(root, filename)
        if lexists(path):
            if not exists(path):
                os.unlink(path)
            else:
                found = True

    return found


def wipeout(context):
    for path in walk(context):
        _logger.info('Wipe out %r implementation from %r', context, path)
        if isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)


def monitor(contexts, paths):
    inotify = _Inotify(contexts)
    inotify.setup(paths)
    inotify.serve_forever()


def populate(contexts, paths):
    inotify = _Inotify(contexts)
    inotify.add_watch = lambda *args: None
    inotify.setup(paths)


class _Inotify(Inotify):

    def __init__(self, contexts):
        Inotify.__init__(self)

        self._contexts = contexts
        self._roots = []
        self._jobs = coroutine.Pool()

        xdg_data_home = os.environ.get('XDG_DATA_HOME') or \
                join(os.environ['HOME'], '.local', 'share')
        self._icons_dir = join(xdg_data_home,
                'icons', 'sugar', 'scalable', 'mimetypes')
        self._mime_dir = join(xdg_data_home, 'mime')

    def setup(self, paths):
        for path in paths:
            path = abspath(path)
            if not exists(path):
                if not os.access(dirname(path), os.W_OK):
                    _logger.warning('No permissions to create %s '
                            'directory, do not monitor it', path)
                    continue
                os.makedirs(path)
            self._roots.append(_Root(self, path))

    def serve_forever(self):
        while True:
            coroutine.select([self.fileno()], [], [])
            if self.closed:
                break
            for filename, event, cb in self.read():
                try:
                    cb(filename, event)
                except Exception:
                    util.exception('Cannot dispatch 0x%X event for %r',
                            event, filename)
                coroutine.dispatch()

    def found(self, clone_path):
        hashed_path, checkin_path = _checkin_path(clone_path)
        if exists(checkin_path):
            return

        _logger.debug('Checking in activity from %r', clone_path)

        try:
            spec = Spec(root=clone_path)
        except Exception:
            util.exception(_logger, 'Cannot read %r spec', clone_path)
            return

        context = spec['Activity', 'bundle_id']

        context_path = _ensure_context_path(context, hashed_path)
        if lexists(context_path):
            os.unlink(context_path)
        os.symlink(clone_path, context_path)

        if lexists(checkin_path):
            os.unlink(checkin_path)
        local.ensure_path(checkin_path)
        os.symlink(relpath(context_path, dirname(checkin_path)), checkin_path)

        if self._contexts.exists(context):
            self._contexts.update(context, {'keep_impl': 2})
        else:
            _logger.debug('Register unknown local activity, %r', context)

            mtime = os.stat(spec.root).st_mtime
            self._contexts.create(guid=context, type='activity',
                    title={DEFAULT_LANG: spec['name']},
                    summary={DEFAULT_LANG: spec['summary']},
                    description={DEFAULT_LANG: spec['description']},
                    keep_impl=2, user=[sugar.uid()], author=[sugar.nickname()],
                    ctime=mtime, mtime=mtime)

            icon_path = join(spec.root, spec['icon'])
            if exists(icon_path):
                self._contexts.set_blob(context, 'artifact_icon', icon_path)
                with toolkit.NamedTemporaryFile() as f:
                    toolkit.svg_to_png(icon_path, f.name, 32, 32)
                    self._contexts.set_blob(context, 'icon', f.name)

        self._checkin_activity(spec)

    def found_mimetypes(self, impl_path):
        hashed_path, __ = _checkin_path(impl_path)
        src_path = join(impl_path, 'activity', 'mimetypes.xml')
        dst_path = join(self._mime_dir, 'packages', hashed_path + '.xml')

        if exists(dst_path):
            return

        _logger.debug('Update MIME database to process found %r', src_path)

        toolkit.symlink(src_path, dst_path)
        toolkit.spawn('update-mime-database', self._mime_dir)

    def lost(self, clone_path):
        __, checkin_path = _checkin_path(clone_path)
        if not lexists(checkin_path):
            return

        _logger.debug('Checking out activity from %r', clone_path)

        context_path = _read_checkin_path(checkin_path)
        context_dir = dirname(context_path)
        impls = set(os.listdir(context_dir)) - set([basename(context_path)])

        if not impls:
            context = basename(context_dir)
            if self._contexts.exists(context):
                self._contexts.update(context, {'keep_impl': 0})

        if lexists(context_path):
            os.unlink(context_path)
        os.unlink(checkin_path)

        self._checkout_activity(clone_path)

    def lost_mimetypes(self, impl_path):
        hashed_path, __ = _checkin_path(impl_path)
        dst_path = join(self._mime_dir, 'packages', hashed_path + '.xml')

        if not lexists(dst_path):
            return

        _logger.debug('Update MIME database to process lost %r', impl_path)

        os.unlink(dst_path)
        toolkit.spawn('update-mime-database', self._mime_dir)

    def _checkin_activity(self, spec):
        icon_path = join(spec.root, spec['icon'])
        if spec['mime_types'] and exists(icon_path):
            _logger.debug('Register %r icons for %r',
                    spec['mime_types'], spec)
            if not exists(self._icons_dir):
                os.makedirs(self._icons_dir)
            for mime_type in spec['mime_types']:
                toolkit.symlink(icon_path,
                        join(self._icons_dir,
                            mime_type.replace('/', '-') + '.svg'))

    def _checkout_activity(self, clone_path):
        if exists(self._icons_dir):
            for filename in os.listdir(self._icons_dir):
                path = join(self._icons_dir, filename)
                if islink(path) and \
                        os.readlink(path).startswith(clone_path + os.sep):
                    os.unlink(path)


class _Root(object):

    def __init__(self, monitor_, path):
        self.path = path
        self._monitor = monitor_
        self._nodes = {}

        _logger.info('Start monitoring %r implementations root', self.path)

        self._monitor.add_watch(self.path,
                IN_DELETE_SELF | IN_CREATE | IN_DELETE |
                        IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        for filename in os.listdir(self.path):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _Node(self._monitor, path)

    def __watch_cb(self, filename, event):
        if event & IN_DELETE_SELF:
            _logger.warning('Lost ourselves, cannot monitor anymore')
            self._nodes.clear()
            return

        if event & (IN_CREATE | IN_MOVED_TO):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _Node(self._monitor, path)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            node = self._nodes.get(filename)
            if node is not None:
                node.unlink()
                del self._nodes[filename]


class _Node(object):

    def __init__(self, monitor_, path):
        self._path = path
        self._monitor = monitor_
        self._activity_path = join(path, 'activity')
        self._activity_dir = None

        _logger.debug('Start monitoring %r root activity directory', path)

        self._wd = self._monitor.add_watch(path,
                IN_CREATE | IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        if exists(self._activity_path):
            self._activity_dir = \
                    _ActivityDir(self._monitor, self._activity_path)

    def unlink(self):
        if self._activity_dir is not None:
            self._activity_dir.unlink()
            self._activity_dir = None
        _logger.debug('Stop monitoring %r root activity directory', self._path)
        self._monitor.rm_watch(self._wd)

    def __watch_cb(self, filename, event):
        if filename != 'activity':
            return
        if event & (IN_CREATE | IN_MOVED_TO):
            self._activity_dir = \
                    _ActivityDir(self._monitor, self._activity_path)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            self._activity_dir.unlink()
            self._activity_dir = None


class _ActivityDir(object):

    def __init__(self, monitor_, path):
        self._path = path
        self._monitor = monitor_
        self._found = False
        self._node_path = dirname(path)

        _logger.debug('Start monitoring %r activity directory', path)

        self._wd = self._monitor.add_watch(path,
                IN_CREATE | IN_CLOSE_WRITE | IN_DELETE | IN_MOVED_TO |
                        IN_MOVED_FROM,
                self.__watch_cb)

        for filename in ('activity.info', 'mimetypes.xml'):
            if exists(join(path, filename)):
                self.found(filename)

    def unlink(self):
        self.lost('activity.info')
        _logger.debug('Stop monitoring %r activity directory', self._path)
        self._monitor.rm_watch(self._wd)

    def found(self, filename):
        if filename == 'mimetypes.xml':
            self._monitor.found_mimetypes(self._node_path)
            return
        if self._found:
            return
        _logger.debug('Found %r', self._node_path)
        self._found = True
        self._monitor.found(self._node_path)
        if exists(join(self._path, 'mimetypes.xml')):
            self._monitor.found_mimetypes(self._node_path)

    def lost(self, filename):
        if filename == 'mimetypes.xml':
            self._monitor.lost_mimetypes(self._node_path)
            return
        if not self._found:
            return
        _logger.debug('Lost %r', self._node_path)
        self._found = False
        self._monitor.lost(self._node_path)

    def __watch_cb(self, filename, event):
        if filename not in ('activity.info', 'mimetypes.xml'):
            return
        if event & IN_CREATE:
            # There is only one case when newly created file can be read,
            # if number of hardlinks is bigger than one, i.e., its content
            # already populated
            if os.stat(join(self._path, filename)).st_nlink > 1:
                self.found(filename)
        elif event & (IN_CLOSE_WRITE | IN_MOVED_TO):
            self.found(filename)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            self.lost(filename)


def _checkin_path(clone_path):
    hashed_path = hashlib.sha1(clone_path).hexdigest()
    return hashed_path, local.path('clones', 'checkin', hashed_path)


def _read_checkin_path(checkin_path):
    return join(dirname(checkin_path), os.readlink(checkin_path))


def _context_path(context, hashed_path):
    return local.path('clones', 'context', context, hashed_path)


def _ensure_context_path(context, hashed_path):
    return local.ensure_path('clones', 'context', context, hashed_path)
