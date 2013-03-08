# Copyright (C) 2011-2013 Aleksey Lim
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

from sugar_network.toolkit import Option


ACCESS_CREATE = 1
ACCESS_WRITE = 2
ACCESS_READ = 4
ACCESS_DELETE = 8
ACCESS_PUBLIC = ACCESS_CREATE | ACCESS_WRITE | ACCESS_READ | ACCESS_DELETE

ACCESS_AUTH = 16
ACCESS_AUTHOR = 32

ACCESS_SYSTEM = 64
ACCESS_LOCAL = 128
ACCESS_REMOTE = 256
ACCESS_LEVELS = ACCESS_SYSTEM | ACCESS_LOCAL | ACCESS_REMOTE

ACCESS_CALC = 512

ACCESS_NAMES = {
        ACCESS_CREATE: 'Create',
        ACCESS_WRITE: 'Write',
        ACCESS_READ: 'Read',
        ACCESS_DELETE: 'Delete',
        }

MAX_LIMIT = 2147483648


index_flush_timeout = Option(
        'flush index index after specified seconds since the last change',
        default=5, type_cast=int)

index_flush_threshold = Option(
        'flush index every specified changes',
        default=32, type_cast=int)

index_write_queue = Option(
        'if active-document is being used for the scheme with one writer '
            'process and multiple reader processes, this option specifies '
            'the writer\'s queue size',
        default=256, type_cast=int)


def uuid():
    """Generate GUID value.

    Function will tranform `uuid.uuid1()` result to leave only alnum symbols.
    The reason is reusing the same resulting GUID in different cases, e.g.,
    for Telepathy names where `-` symbols, from `uuid.uuid1()`, are not
    permitted.

    :returns:
        GUID string value

    """
    from uuid import uuid1
    return ''.join(str(uuid1()).split('-'))


def default_lang():
    """Default language to fallback for localized strings.

    :returns:
        string in format of HTTP's Accept-Language, e.g., `en-gb`.

    """
    global _default_lang

    if _default_lang is None:
        import locale
        lang = locale.getdefaultlocale()[0]
        if lang:
            _default_lang = lang.replace('_', '-').lower()
        else:
            _default_lang = 'en'

    return _default_lang


def gettext(value, accept_language=None):
    if not value:
        return ''
    if not isinstance(value, dict):
        return value

    if accept_language is None:
        accept_language = [default_lang()]
    elif isinstance(accept_language, basestring):
        accept_language = [accept_language]
    stripped_value = None

    for lang in accept_language:
        result = value.get(lang)
        if result is not None:
            return result

        prime_lang = lang.split('-')[0]
        if prime_lang != lang:
            result = value.get(prime_lang)
            if result is not None:
                return result

        if stripped_value is None:
            stripped_value = {}
            for k, v in value.items():
                if '-' in k:
                    stripped_value[k.split('-', 1)[0]] = v
        result = stripped_value.get(prime_lang)
        if result is not None:
            return result

    return value[min(value.keys())]


class BadRequest(Exception):
    """Bad requested resource."""
    pass


class NotFound(Exception):
    """Resource was not found."""
    pass


class Forbidden(Exception):
    """Caller does not have permissions to get access."""
    pass


class CommandNotFound(Exception):
    pass


_default_lang = None
