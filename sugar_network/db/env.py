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

from sugar_network import toolkit
from sugar_network.toolkit import Option, http


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


def gettext(value, accept_language=None):
    if not value:
        return ''
    if not isinstance(value, dict):
        return value

    if accept_language is None:
        accept_language = [toolkit.default_lang()]
    elif isinstance(accept_language, basestring):
        accept_language = [accept_language]
    accept_language.append('en')

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


class CommandNotFound(http.BadRequest):
    pass
