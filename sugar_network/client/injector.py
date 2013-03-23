# Copyright (C) 2010-2012 Aleksey Lim
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
import json
import shutil
import logging
from os.path import join, exists, basename, dirname

from sugar_network import client
from sugar_network.client import journal, cache
from sugar_network.toolkit import pipe, lsb_release, util


_PMS_PATHS = {
        'Debian': '/var/lib/dpkg/status',
        'Fedora': '/var/lib/rpm/Packages',
        'Ubuntu': '/var/lib/dpkg/status',
        }

_logger = logging.getLogger('client.injector')
_pms_path = _PMS_PATHS.get(lsb_release.distributor_id())
_mtime = None


def make(guid):
    return pipe.fork(_make, log_path=client.profile_path('logs', guid),
            context=guid, session={'context': guid})


def launch(guid, args=None, activity_id=None, object_id=None, uri=None,
        color=None):
    if object_id:
        if not activity_id:
            activity_id = journal.get(object_id, 'activity_id')
        if not color:
            color = journal.get(object_id, 'icon-color')

    if not activity_id:
        activity_id = journal.create_activity_id()

    if args is None:
        args = []
    args.extend([
        '-b', guid,
        '-a', activity_id,
        ])
    if object_id:
        args.extend(['-o', object_id])
    if uri:
        args.extend(['-u', uri])

    return pipe.fork(_launch, log_path=client.profile_path('logs', guid),
            context=guid, args=args, session={
                'context': guid,
                'activity_id': activity_id,
                'color': color,
                })


def clone(guid):
    return pipe.fork(_clone, log_path=client.profile_path('logs', guid),
            context=guid, session={'context': guid})


def clone_impl(context, guid, spec):
    return pipe.fork(_clone_impl,
            log_path=client.profile_path('logs', context), guid=guid,
            spec=spec, session={'context': context})


def invalidate_solutions(mtime):
    global _mtime
    _mtime = mtime


def _make(context):
    pipe.feedback('analyze')
    solution = _solve(context)
    pipe.feedback('solved', environ={'solution': solution})

    to_install = []
    for impl in solution:
        if 'install' in impl:
            to_install.extend(impl['install'])
    if to_install:
        pipe.trace('Install %s package(s)',
                ', '.join([i['name'] for i in to_install]))
        from sugar_network.client import packagekit
        packagekit.install(to_install)

    for impl in solution:
        if 'path' in impl or impl['stability'] == 'packaged':
            continue

        pipe.trace('Download %s implementation', impl['id'])
        # TODO Process different mountpoints
        impl_path = cache.get(impl['id'])
        if 'prefix' in impl:
            impl_path = join(impl_path, impl['prefix'])
        impl['path'] = impl_path

    pipe.feedback('ready')
    return solution


def _launch(context, args):
    solution = _make(context)

    args = solution[0]['command'] + (args or [])
    _logger.info('Executing %r feed: %s', context, args)
    pipe.feedback('exec')

    _activity_env(solution[0], os.environ)
    os.execvpe(args[0], args, os.environ)


def _clone(context):
    solution = _make(context)

    cloned = []
    try:
        for impl in solution:
            path = impl.get('path')
            if not path or \
                    path == '/':  # Fake path set by "sugar" dependency
                continue
            dst_path = util.unique_filename(
                    client.activity_dirs.value[0], basename(path))
            cloned.append(dst_path)
            _logger.info('Clone implementation to %r', dst_path)
            util.cptree(path, dst_path)
    except Exception:
        while cloned:
            shutil.rmtree(cloned.pop(), ignore_errors=True)
        raise


def _clone_impl(guid, spec):
    spec = spec['*-*']

    src_path = cache.get(guid)
    if 'extract' in spec:
        src_path = join(src_path, spec['extract'])
    dst_path = util.unique_filename(
            client.activity_dirs.value[0], basename(src_path))

    _logger.info('Clone implementation to %r', dst_path)
    util.cptree(src_path, dst_path)


def _solve(context):
    print '>', context
    pipe.trace('Start solving %s feed', context)

    cached_path, solution, stale = _get_cached_solution(context)
    if stale is False:
        pipe.trace('Reuse cached solution')
        return solution

    conn = client.IPCClient()
    print solution, conn.get(cmd='inline')
    if solution is not None and not conn.get(cmd='inline'):
        pipe.trace('Reuse stale cached solution in offline mode')
        return solution

    from sugar_network.client import solver

    solution = solver.solve(conn, context)
    _set_cached_solution(cached_path, solution)

    return solution


def _activity_env(impl, environ):
    root = client.profile_path('data', impl['context'])
    impl_path = impl['path']

    for path in ['instance', 'data', 'tmp']:
        path = join(root, path)
        if not exists(path):
            os.makedirs(path)

    environ['PATH'] = ':'.join([
        join(impl_path, 'activity'),
        join(impl_path, 'bin'),
        environ['PATH'],
        ])
    environ['SUGAR_BUNDLE_PATH'] = impl_path
    environ['SUGAR_BUNDLE_ID'] = impl['context']
    environ['SUGAR_BUNDLE_NAME'] = impl['name']
    environ['SUGAR_BUNDLE_VERSION'] = impl['version']
    environ['SUGAR_ACTIVITY_ROOT'] = root
    environ['PYTHONPATH'] = impl_path + ':' + environ.get('PYTHONPATH', '')
    environ['SUGAR_LOCALEDIR'] = join(impl_path, 'locale')

    os.chdir(impl_path)


def _get_cached_solution(guid):
    path = client.path('cache', 'solutions', guid[:2], guid)

    solution = None
    if exists(path):
        try:
            with file(path) as f:
                api_url, solution = json.load(f)
        except Exception, error:
            _logger.debug('Cannot open %r solution: %s', path, error)
    if solution is None:
        return path, None, None

    stale = (api_url != client.api_url.value)
    if not stale and _mtime is not None:
        stale = (_mtime > os.stat(path).st_mtime)
    if not stale and _pms_path is not None:
        stale = (os.stat(_pms_path).st_mtime > os.stat(path).st_mtime)
    if not stale:
        for impl in solution:
            spec = impl.get('spec')
            if spec and exists(spec):
                stale = (os.stat(spec).st_mtime > os.stat(path).st_mtime)
                if stale:
                    break

    return path, solution, stale


def _set_cached_solution(path, solution):
    if not exists(dirname(path)):
        os.makedirs(dirname(path))
    with file(path, 'w') as f:
        json.dump([client.api_url.value, solution], f)
