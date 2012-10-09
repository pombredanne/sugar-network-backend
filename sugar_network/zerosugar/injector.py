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
import shutil
import logging
from os.path import join, exists, basename

from sugar_network import local, sugar, IPCClient
from sugar_network.zerosugar import pipe
from active_toolkit import util, enforce


_logger = logging.getLogger('zerosugar.injector')


def launch(mountpoint, context, args=None):
    return pipe.fork(_launch, mountpoint, context, args)


def checkin(mountpoint, context):
    return pipe.fork(_checkin, mountpoint, context)


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


def _checkin(mountpoint, context):
    solution = _solve(mountpoint, context)
    _make(solution)

    checkedin = []
    try:
        for impl in solution:
            dst_path = util.unique_filename(
                    local.activity_dirs.value[0], basename(impl['path']))
            checkedin.append(dst_path)
            _logger.info('Checkin implementation to %r', dst_path)
            util.cptree(impl['path'], dst_path)
    except Exception:
        while checkedin:
            shutil.rmtree(checkedin.pop(), ignore_errors=True)
        raise


def _make(solution):
    to_install = []
    for impl in solution:
        if 'install' in impl:
            to_install.extend(impl.pop('install'))
    if to_install:
        from sugar_network.zerosugar import packagekit
        packagekit.install(to_install)

    client = IPCClient()
    for impl in solution:
        if 'mountpoint' not in impl or 'path' in impl:
            continue
        # TODO Per download progress
        pipe.progress('download')
        bundle = client.get(
                ['implementation', impl['id'], 'data'],
                cmd='get_blob', mountpoint=impl['mountpoint'])
        enforce(bundle and 'path' in bundle, 'Cannot download implementation')
        impl_path = bundle['path']
        if 'prefix' in impl:
            impl_path = join(impl_path, impl['prefix'])
        impl['path'] = impl_path

    pipe.progress('ready', session={'implementation': solution[0]['id']})


def _solve(mountpoint, context):
    solution = _get_cached_solution(mountpoint, context)
    if solution is not None and not solution.stale:
        return solution

    from sugar_network import zeroinstall

    pipe.progress('analyze')
    try:
        solution = zeroinstall.solve(mountpoint, context)
    except Exception:
        if solution is None:
            raise
        util.exception(_logger, 'Cannot solve remote solution %r, '
                'fallback to stale cached version', context)
    else:
        _set_cached_solution(mountpoint, context, solution)

    return solution


def _get_cached_solution(mountpoint, context):
    pass


def _set_cached_solution(mountpoint, context, solution):
    pass


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
    environ['PYTHONPATH'] = '%s:%s' % (impl_path, environ['PYTHONPATH'])
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
