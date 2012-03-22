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

from sugar_network.resources import User, Context, Question, Idea, Problem, \
        Review, Solution, Artifact, Implementation, Report, Notification, \
        Comment

from sugar_network.env import api_url, certfile, no_check_certificate, debug

from sugar_network.sugar import guid, profile_path, pubkey, nickname, color, \
        machine_sn, machine_uuid


def config(parser=None):
    """Load sugar-network configure settings.

    :param parser:
        `OptionParser` object to apply configuration, parsed from command line
        arguments, on top of configuration from configure files
    :returns:
        if `parser` was specified, return (`options`, `args`) tuple
        with data parsed from command line arguments

    """
    from sugar_network import util, env

    util.Option.seek('main', env)

    config_files = [
            '/etc/sugar-network.conf',
            '~/.config/sugar-network/config',
            profile_path('sugar-network.conf'),
            ]

    if parser is None:
        util.Option.merge(None, config_files)
    else:
        util.Option.bind(parser, config_files)
        options, args = parser.parse_args()
        util.Option.merge(options)
        return options, args


def launch(context, command='activity', args=None):
    """Launch context implementation.

    Function will call fork at the beginning. In forked process, it will try
    to choose proper implementation to execute.

    Execution log will be stored in `~/.sugar/PROFILE/logs` directory.

    :param context:
        context GUID to look for implementations
    :param command:
        command that selected implementation should support
    :param args:
        optional list of arguments to pass to launching implementation
    :returns:
        child process pid

    """
    import os
    import sys
    from os.path import exists, join, dirname

    pid = os.fork()
    if pid:
        return pid

    cmd = ['sugar-network']
    if command:
        cmd.extend(['--command', command])
    cmd.extend(['launch', context])
    if args:
        cmd.extend(args)

    cmd_path = join(dirname(__file__), '..', 'sugar-network')
    if exists(cmd_path):
        os.execv(cmd_path, cmd)
    else:
        os.execvp(cmd[0], cmd)
    sys.exit(1)
