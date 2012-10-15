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

from sugar_network import local, sugar
from sugar_network.zerosugar import pipe, cache, lsb_release
from active_toolkit import util


_PMS_PATHS = {
        'Debian': '/var/lib/dpkg/status',
        'Fedora': '/var/lib/rpm/Packages',
        'Ubuntu': '/var/lib/dpkg/status',
        }

_logger = logging.getLogger('zerosugar.injector')
_pms_path = _PMS_PATHS.get(lsb_release.distributor_id())
_mtime = None


def launch(mountpoint, context, args=None):
    return pipe.fork(_launch, mountpoint, context, args)


def clone(mountpoint, context):
    return pipe.fork(_clone, mountpoint, context)


def invalidate_solutions(mtime):
    global _mtime
    _mtime = mtime


def _launch(mountpoint, context, args):
    if args is None:
        args = []

    solution = _solve(mountpoint, context)
    _make(solution)

    args = solution[0]['command'] + args
    _logger.info('Executing %r from %r: %s', context, mountpoint, args)
    pipe.progress('exec')

    _activity_env(solution[0], os.environ)
    os.execvpe(args[0], args, os.environ)


def _clone(mountpoint, context):
    solution = _solve(mountpoint, context)
    _make(solution)

    cloned = []
    try:
        for impl in solution:
            dst_path = util.unique_filename(
                    local.activity_dirs.value[0], basename(impl['path']))
            cloned.append(dst_path)
            _logger.info('Clone implementation to %r', dst_path)
            util.cptree(impl['path'], dst_path)
    except Exception:
        while cloned:
            shutil.rmtree(cloned.pop(), ignore_errors=True)
        raise


def _make(solution):
    to_install = []
    for impl in solution:
        if 'install' in impl:
            to_install.extend(impl.pop('install'))
    if to_install:
        from sugar_network.zerosugar import packagekit
        packagekit.install(to_install)

    for impl in solution:
        if 'mountpoint' not in impl or 'path' in impl:
            continue
        # TODO Process different mountpoints
        impl_path = cache.get(impl['id'])
        if 'prefix' in impl:
            impl_path = join(impl_path, impl['prefix'])
        impl['path'] = impl_path

    pipe.progress('ready', session={'implementation': solution[0]['id']})


def _solve(mountpoint, context):
    pipe.progress('analyze')

    cached_path, solution, stale = _get_cached_solution(mountpoint, context)
    if stale is False:
        return solution

    from sugar_network import zeroinstall

    try:
        solution = zeroinstall.solve(mountpoint, context)
    except Exception:
        if solution is None:
            raise
        util.exception(_logger, 'Fallback to stale %r solution', context)
    else:
        _set_cached_solution(cached_path, solution)

    return solution


def _activity_env(impl, environ):
    root = sugar.profile_path('data', impl['context'])
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


def _get_cached_solution(mountpoint, guid):
    path = local.path('cache', 'solutions', mountpoint.replace('/', '#'),
            guid[:2], guid)

    solution = None
    if exists(path):
        try:
            with file(path) as f:
                api_url, solution = json.load(f)
        except Exception, error:
            _logger.debug('Cannot open %r solution: %s', path, error)
    if solution is None:
        return path, None, None

    stale = (api_url != local.api_url.value)
    if _mtime is not None:
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
        json.dump([local.api_url.value, solution], f)
