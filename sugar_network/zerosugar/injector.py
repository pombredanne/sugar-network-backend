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

from zeroinstall.injector import model
from zeroinstall.injector.requirements import Requirements

from sugar_network.zerosugar import pipe, packagekit, Spec
from sugar_network.zerosugar.solution import solve
from sugar_network import local
from sugar_network.toolkit import sugar
from active_toolkit import util, enforce


_logger = logging.getLogger('zerosugar.injector')


def launch(mountpoint, context, command='activity', args=None):
    return pipe.fork(_launch, mountpoint, context, command, args)


def checkin(mountpoint, context, command='activity'):
    return pipe.fork(_checkin, mountpoint, context, command)


def _launch(mountpoint, context, command, args):
    if args is None:
        args = []

    solution = _make(context, command)
    cmd = solution.commands[0]
    args = cmd.path.split() + args

    _logger.info('Executing %s: %s', solution.interface, args)
    pipe.progress('exec')

    if command == 'activity':
        _activity_env(solution.top, os.environ)
    os.execvpe(args[0], args, os.environ)


def _checkin(mountpoint, context, command):
    solution = _make(context, command)

    checkedin = []
    try:
        for sel, __, __ in solution.walk():
            dst_path = util.unique_filename(
                    local.activity_dirs.value[0], basename(sel.local_path))
            checkedin.append(dst_path)
            _logger.info('Checkin implementation to %r', dst_path)
            util.cptree(sel.local_path, dst_path)
    except Exception:
        while checkedin:
            shutil.rmtree(checkedin.pop(), ignore_errors=True)
        raise


def _make(context, command):
    requirement = Requirements(context)
    requirement.command = command

    pipe.progress('analyze')
    solution = solve(requirement)

    to_install = []
    for sel, __, __ in solution.walk():
        to_install.extend(sel.to_install or [])
    if to_install:
        packagekit.install(to_install)

    for sel, __, __ in solution.walk():
        if sel.is_available():
            continue

        enforce(sel.download_sources,
                'No sources to download implementation for %r context',
                sel.interface)

        # TODO Per download progress
        pipe.progress('download')

        impl = sel.client.get(['implementation', sel.id, 'data'],
                cmd='get_blob')
        enforce(impl and 'path' in impl, 'Cannot download implementation')
        impl_path = impl['path']

        dl = sel.download_sources[0]
        if dl.extract is not None:
            impl_path = join(impl_path, dl.extract)
        sel.local_path = impl_path

    pipe.progress('ready', session={'implementation': solution.top.id})

    return solution


def _activity_env(selection, environ):
    root = sugar.profile_path('data', selection.interface)

    for path in ['instance', 'data', 'tmp']:
        path = join(root, path)
        if not exists(path):
            os.makedirs(path)

    # TODO Any way to avoid loading spec file?
    spec = Spec(root=selection.local_path)

    environ['SUGAR_BUNDLE_PATH'] = selection.local_path
    environ['SUGAR_BUNDLE_ID'] = selection.feed.context
    environ['SUGAR_BUNDLE_NAME'] = spec['Activity', 'name']
    environ['SUGAR_BUNDLE_VERSION'] = model.format_version(selection.version)
    environ['SUGAR_ACTIVITY_ROOT'] = root
    environ['PATH'] = '%s:%s' % \
            (join(selection.local_path, 'bin'), environ['PATH'])
    environ['PYTHONPATH'] = '%s:%s' % \
            (selection.local_path, environ['PYTHONPATH'])
    environ['SUGAR_LOCALEDIR'] = join(selection.local_path, 'locale')

    os.chdir(selection.local_path)
