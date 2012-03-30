# Copyright (C) 2012, Aleksey Lim
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
import sys
from os.path import exists, join, dirname
from gettext import gettext as _

from sugar_network import util, sugar


api_url = util.Option(
        _('url to connect to Sugar Network server API'),
        default='https://api.network.sugarlabs.org', short_option='-a')

certfile = util.Option(
        _('path to SSL certificate file to connect to server via HTTPS'))

no_check_certificate = util.Option(
        _('do not check the server certificate against the available ' \
                'certificate authorities'),
        default=False, type_cast=util.Option.bool_cast, action='store_true')

cache_dir = util.Option(
        _('path to directory to keep persistent cache; ' \
                'if omited, ~/sugar/*/cache directory will be used'))

debug = util.Option(
        _('debug logging level; multiple argument'),
        default=0, type_cast=int, short_option='-D', action='count')


def config(parser=None, stop_args=None):
    """Load sugar-network configure settings.

    :param parser:
        `OptionParser` object to apply configuration, parsed from command line
        arguments, on top of configuration from configure files
    :param stop_args:
        if `parser` was specified, `stop_args` might be a list of arguments
        that should stop further command-line arguments parsing
    :returns:
        if `parser` was specified, return (`options`, `args`) tuple
        with data parsed from command line arguments

    """
    util.Option.seek('main')

    config_files = [
            '/etc/sugar-network.conf',
            '~/.config/sugar-network/config',
            sugar.profile_path('sugar-network.conf'),
            ]

    if parser is None:
        util.Option.load(config_files)
    else:
        return util.Option.parse_args(parser, config_files, stop_args)


def launch(context, command='activity', args=None):
    """Launch context implementation.

    Function will call fork at the beginning. In forked process, it will try
    to choose proper implementation to execute and launch it.

    Execution log will be stored in `~/.sugar/PROFILE/logs` directory.

    :param context:
        context GUID or name to look for implementations to launch
    :param command:
        command that selected implementation should support
    :param args:
        optional list of arguments to pass to launching implementation
    :returns:
        child process pid

    """
    pid = os.fork()
    if pid:
        return pid

    cmd = ['sugar-network', '-C', command, 'launch', context] + (args or [])

    cmd_path = join(dirname(__file__), '..', 'sugar-network')
    if exists(cmd_path):
        os.execv(cmd_path, cmd)
    else:
        os.execvp(cmd[0], cmd)

    sys.exit(1)
