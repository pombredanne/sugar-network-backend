# Copyright (C) 2014 Aleksey Lim
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
import logging
import gettext
import locale


# To let `encode()` working properly, avoid msgids gettext'izing
# but still populate .po files parsing the source code
_ = lambda x: x

_logger = logging.getLogger('i18n')
_i18n = None
_domain = None


def init(domain):
    global _domain, _i18n
    _domain = domain
    _i18n = None
    gettext.textdomain(domain)
    locale.setlocale(locale.LC_ALL, '')


def default_lang():
    """Default language to fallback for localized strings.

    :returns:
        string in format of HTTP's Accept-Language

    """
    return default_langs()[0]


def default_langs():
    """Default languages list, i.e., including all secondory languages.

    :returns:
        list of strings in format of HTTP's Accept-Language

    """
    global _default_langs

    if _default_langs is None:
        locales = os.environ.get('LANGUAGE')
        if locales:
            locales = [i for i in locales.split(':') if i.strip()]
        else:
            locales = [locale.getdefaultlocale()[0]]
        if not locales:
            _default_langs = ['en']
        else:
            _default_langs = []
            for loc in locales:
                lang = loc.strip().split('.')[0].lower()
                if lang == 'c':
                    lang = 'en'
                elif '_' in lang:
                    lang, region = lang.split('_')
                    if lang != region:
                        lang = '-'.join([lang, region])
                _default_langs.append(lang)
        _logger.info('Default languages are %r', _default_langs)

    return _default_langs


def decode(value, accept_language=None):
    if not value:
        return ''
    if not isinstance(value, dict):
        return value

    if accept_language is None:
        accept_language = default_langs()
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


def encode(msgid, *args, **kwargs):
    global _i18n

    if _i18n is None:
        from sugar_network.toolkit.languages import LANGUAGES

        _i18n = {}
        if _domain is not None:
            for lang in LANGUAGES:
                try:
                    _i18n[lang] = \
                            gettext.translation(_domain, languages=[lang])
                except IOError, error:
                    _logger.error('Failed to open %r locale: %s', lang, error)
        if not _i18n:
            _i18n = {default_lang(): None}

    result = {}

    for lang, trans in _i18n.items():
        msgstr = trans.gettext(msgid) if trans is not None else msgid
        if args:
            msgargs = []
            for arg in args:
                msgargs.append(decode(arg, lang))
            msgstr = msgstr % tuple(msgargs)
        elif kwargs:
            msgargs = {}
            for key, value in kwargs.items():
                msgargs[key] = decode(value, lang)
            msgstr = msgstr % msgargs
        result[lang] = msgstr

    return result


_default_lang = None
_default_langs = None
