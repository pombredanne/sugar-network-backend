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
from os.path import join, exists, basename

from sugar_network import local, sugar
from sugar_network.zerosugar import pipe, cache
from active_toolkit import util


_logger = logging.getLogger('zerosugar.injector')


def launch(mountpoint, context, args=None):
    return pipe.fork(_launch, mountpoint, context, args)


def clone(mountpoint, context):
    return pipe.fork(_clone, mountpoint, context)


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

    cache_dir = local.path('cache', 'solutions', mountpoint.replace('/', '\\'))
    cache_path = join(cache_dir, context)

    if exists(cache_path) and \
            os.stat(cache_dir).st_mtime <= os.stat(cache_path).st_mtime:
        try:
            with file(cache_path) as f:
                return json.load(f)
        except Exception, error:
            _logger.debug('Cannot open %r solution: %s', cache_path, error)

    from sugar_network import zeroinstall

    try:
        solution = zeroinstall.solve(mountpoint, context)
    except Exception:
        if exists(cache_path):
            print cache_path
            try:
                with file(cache_path) as f:
                    solution = json.load(f)
                    util.exception(_logger,
                            'Cannot solve %r, fallback to stale solution',
                            context)
                    return solution
            except Exception, error:
                _logger.debug('Cannot open %r solution: %s', cache_path, error)
        raise
    else:
        if not exists(cache_dir):
            os.makedirs(cache_dir)
        with file(cache_path, 'w') as f:
            json.dump(solution, f)

    return solution


def _activity_env(impl, environ):
    root = sugar.profile_path('data', impl['context'])
    impl_path = impl['path']

    for path in ['instance', 'data', 'tmp']:
        path = join(root, path)
        if not exists(path):
            os.makedirs(path)

    environ['SUGAR_BUNDLE_PATH'] = impl_path
    environ['SUGAR_BUNDLE_ID'] = impl['context']
    environ['SUGAR_BUNDLE_NAME'] = impl['name']
    environ['SUGAR_BUNDLE_VERSION'] = impl['version']
    environ['SUGAR_ACTIVITY_ROOT'] = root
    environ['PYTHONPATH'] = impl_path + ':' + environ.get('PYTHONPATH', '')
    environ['SUGAR_LOCALEDIR'] = join(impl_path, 'locale')

    # TODO Do it only once on unzip
    # Activities might call bin/* files but python zipfile module
    # doesn't set exec permissions while extracting
    for exec_dir in ('bin', 'activity'):
        bin_path = join(impl_path, exec_dir)
        if exists(bin_path):
            environ['PATH'] = bin_path + ':' + environ['PATH']
            for filename in os.listdir(bin_path):
                os.chmod(join(bin_path, filename), 0755)

    os.chdir(impl_path)
