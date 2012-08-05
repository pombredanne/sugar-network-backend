# Copyright (C) 2011 Aleksey Lim
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

"""Console output routines.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/printf.py$
$Date: 2012-08-05$

"""

import sys
import logging


#: Disable/enable non-status output.
VERBOSE = True
#: Disable/enable any output.
QUIET = False

RESET = '\033[0m'
BOLD = '\033[1m'
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = \
        ['\033[1;%dm' % (30 + i_) for i_ in range(8)]

_hints = []
_last_line_len = 0
_last_progress = []
_screen_width = None


def dump(message, *args):
    """Print verbatim text.

    :param message:
        text to print
    :param \*args:
        `%` arguments to expand `message` value

    """
    _dump(False, sys.stdout, '', [message, args], '\n')


def info(message, *args):
    """Print information text.

    :param message:
        text to print
    :param \*args:
        `%` arguments to expand `message` value

    """
    _dump(True, sys.stdout, None, [message, args], '\n')
    _dump_progress()


def exception(message=None, *args):
    """Print exception text.

    Call this function in `try..except` block after getting exceptions.

    :param message:
        text to print
    :param \*args:
        `%` arguments to expand `message` value

    """
    import traceback

    klass, error, tb = sys.exc_info()

    tb_list = []
    for line in traceback.format_exception(klass, error, tb):
        tb_list.extend([i.rstrip() for i in line.strip().split('\n')])

    if type(error).__name__ == 'dbus.exceptions.DBusException':
        dbus_tb = str(error).split('\n')
        if len(dbus_tb) == 1:
            error = dbus_tb[0]
        else:
            # Strip the last empty line
            del dbus_tb[-1]
            error = '%s:%s' % \
                    (dbus_tb[0].split(':')[0], dbus_tb[-1].split(':', 1)[-1])

    if message and args:
        message = message % args

    error = str(error) or 'Something weird happened'
    if message:
        message += ': %s' % error
    else:
        message = str(error)
    _dump(True, sys.stdout, None, message, '\n')

    if logging.getLogger().level > logging.INFO:
        hint('Use -D argument for debug info, '
                '-DD for full debuging output and tracebacks')
    elif logging.getLogger().level > logging.DEBUG:
        hint('Use -DD argument for full debuging output and tracebacks')
    else:
        for i in tb_list:
            _dump(True, sys.stdout, '   ', i, '\n')

    _dump_progress()


def scan_yn(message, *args):
    """Request for Y/N input.

    :param message:
        prefix text to print
    :param \*args:
        `%` arguments to expand `message` value
    :returns:
        `True` if user's input was `Y`

    """
    _dump(True, sys.stderr, None, [message, args], ' [Y/N] ')
    answer = raw_input()
    _dump_progress()
    return answer and answer in 'Yy'


def progress(message, *args):
    """Print status line text.

    Status line will be shown as the last line all time and will be cleared
    on program exit.

    :param message:
        prefix text to print
    :param \*args:
        `%` arguments to expand `message` value

    """
    _last_progress[:] = [message, args]
    _dump_progress()


def clear_progress():
    """Clear status line on program exit."""
    if _last_line_len:
        sys.stderr.write(chr(13) + ' ' * _last_line_len + chr(13))


def hint(message, *args):
    """Add new hint.

    All hint will be queued to print them at once in `flush_hints()` function
    on program exit.

    :param message:
        prefix text to print
    :param \*args:
        `%` arguments to expand `message` value

    """
    if args:
        message = message % args
    _hints.append(message)


def flush_hints():
    """Print all queued hints."""
    clear_progress()
    if _hints:
        dump('')
    while _hints:
        _dump(True, sys.stderr, '-- Hint: ', _hints.pop(0), '\n')


def _dump(is_status, stream, prefix, *args):
    if not VERBOSE or QUIET:
        return

    global _last_line_len
    global _screen_width

    if _screen_width is None:
        try:
            import curses
            curses.setupterm()
            _screen_width = curses.tigetnum('cols') or 80
        except Exception, error:
            logging.info('Cannot get screen width: %s', error)
            _screen_width = 80

    if prefix is None:
        prefix = '-- '

    clear_progress()
    _last_line_len = 0

    for i in [prefix] + list(args):
        if isinstance(i, list):
            if i:
                message, message_args = i
                if message_args:
                    message = message % message_args
        else:
            message = i

        stream.write(message)

        if is_status:
            _last_line_len += len(message)

    _last_line_len = min(_last_line_len, _screen_width)


def _dump_progress():
    _dump(True, sys.stderr, '   ', _last_progress, chr(13))
    sys.stderr.flush()
