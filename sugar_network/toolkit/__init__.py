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

import sys
import logging
from os.path import join

from sugar_network.toolkit.options import Option


BUFFER_SIZE = 1024 * 10


tmpdir = Option(
        'if specified, use this directory for temporary files; such files '
        'might take considerable number of bytes while downloading of '
        'synchronizing Sugar Network content',
        name='tmpdir')


def enforce(condition, error=None, *args):
    """Make an assertion in runtime.

    In comparing with `assert`, it will all time present in the code.
    Just a bit of syntax sugar.

    :param condition:
        the condition to assert; if not False then return,
        otherse raise an RuntimeError exception
    :param error:
        error message to pass to RuntimeError object
        or Exception class to raise
    :param args:
        optional '%' arguments for the `error`

    """
    if condition:
        return

    if isinstance(error, type):
        exception_class = error
        if args:
            error = args[0]
            args = args[1:]
        else:
            error = None
    else:
        exception_class = RuntimeError

    if args:
        error = error % args
    elif not error:
        # pylint: disable-msg=W0212
        frame = sys._getframe(1)
        error = 'Runtime assertion failed at %s:%s' % \
                (frame.f_globals['__file__'], frame.f_lineno - 1)

    raise exception_class(error)


def exception(*args):
    """Log about exception on low log level.

    That might be useful for non-critial exception. Input arguments are the
    same as for `logging.exception` function.

    :param args:
        optional arguments to pass to logging function;
        the first argument might be a `logging.Logger` to use instead of
        using direct `logging` calls

    """
    if args and isinstance(args[0], logging.Logger):
        logger = args[0]
        args = args[1:]
    else:
        logger = logging

    klass, error, tb = sys.exc_info()

    import traceback
    tb = [i.rstrip() for i in traceback.format_exception(klass, error, tb)]

    error_message = str(error) or '%s exception' % type(error).__name__
    if args:
        if len(args) == 1:
            message = args[0]
        else:
            message = args[0] % args[1:]
        error_message = '%s: %s' % (message, error_message)

    logger.error(error_message)
    logger.debug('\n'.join(tb))
