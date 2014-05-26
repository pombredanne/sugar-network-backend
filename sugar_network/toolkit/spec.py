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

import re
import os
import logging
from os.path import join, exists, dirname
from ConfigParser import ConfigParser

from sugar_network.toolkit.licenses import GOOD_LICENSES
from sugar_network.toolkit import enforce


EMPTY_LICENSE = 'License is not specified'

_FIELDS = {
        # name: (required, typecast)
        'context': (True, None),
        'name': (True, None),
        'summary': (True, None),
        'description': (False, None),
        'license': (True, lambda x: _parse_list(x)),
        'homepage': (True, None),
        'icon': (False, None),
        'version': (True, None),
        'stability': (True, None),
        'tags': (False, lambda x: _parse_list(x)),
        'mime_types': (False, lambda x: _parse_list(x)),
        }
_ARCHES = ['all', 'any']
_STABILITIES = ('insecure', 'buggy', 'developer', 'testing', 'stable')
_POLICY_URL = 'http://wiki.sugarlabs.org/go/Sugar_Network/Policy'
_LIST_SEPARATOR = ','

_RESTRICTION_RE = re.compile('(<|<=|=|==|>|>=)\\s*([0-9.]+)')

_VERSION_RE = re.compile('-([a-z]*)')
_VERSION_MOD_TO_VALUE = {
        'pre': -2,
        'rc': -1,
        '': 0,
        'r': 1,
        'post': 2,
        }
_VERSION_VALUE_TO_MOD = {}

_logger = logging.getLogger('sweets-recipe')


def parse_version(version_string, ignore_errors=False):
    """Convert a version string to an internal representation.

    The parsed format can be compared quickly using the standard Python
    functions. Adapted Zero Install version.

    :param version_string:
        version in format supported by 0install
    :returns:
        array of arrays of integers

    """
    if version_string is None:
        return None

    parts = _VERSION_RE.split(version_string.lower())
    if parts[-1] == '':
        del parts[-1]  # Ends with a modifier
    else:
        parts.append('')
    enforce(parts, ValueError, 'Empty version string')

    def to_int(x):
        pos = 0
        for i in x:
            if not i.isdigit():
                enforce(ignore_errors, ValueError,
                        'Only numbers are allowed in version category')
                break
            pos += 1
        if pos:
            return int(x[:pos])
        else:
            enforce(ignore_errors, ValueError, 'Empty version category')
            return 0

    length = len(parts)
    try:
        for x in range(0, length, 2):
            part = parts[x]
            if part:
                parts[x] = [to_int(i) for i in part.split('.')]
            else:
                parts[x] = []  # (because ''.split('.') == [''], not [])
    except ValueError as error:
        raise ValueError('Invalid version format in "%s": %s' %
                (version_string, error))
    except KeyError as error:
        raise ValueError('Invalid version modifier in "%s": %s' %
                (version_string, error))

    for x in range(1, length, 2):
        parts[x] = _VERSION_MOD_TO_VALUE[parts[x]]
    return parts


def format_version(version):
    """Convert version to string representation.

    If string value is passed, it will be parsed to procuduce
    canonicalized string representation.

    """
    if version is None:
        return None
    if isinstance(version, basestring):
        version = parse_version(version)

    if not _VERSION_VALUE_TO_MOD:
        for mod, value in _VERSION_MOD_TO_VALUE.items():
            _VERSION_VALUE_TO_MOD[value] = mod

    length = len(version) - (0 if version[-1] else 1)
    result = [None] * length

    for x in range(0, length, 2):
        result[x] = '.'.join([str(i) for i in version[x]])
        if x + 1 < length:
            result[x + 1] = '-' + _VERSION_VALUE_TO_MOD[version[x + 1]]

    return ''.join(result)


def parse_requires(requires):
    result = {}

    for dep_str in _parse_list(requires):
        parts = _RESTRICTION_RE.split(dep_str)
        enforce(parts[0], 'Cannot parse %r dependency', dep_str)
        dep_name = parts.pop(0).strip()
        dep = result.setdefault(dep_name, [])

        while len(parts) >= 3:
            rel = parts[0]
            if rel in ('=', '=='):
                rel = [0]
            elif rel == '<':
                rel = [-1]
            elif rel == '>':
                rel = [1]
            elif rel == '<=':
                rel = [-1, 0]
            elif rel == '>=':
                rel = [1, 0]
            dep.append((rel, parse_version(parts[1])))
            del parts[:3]

        enforce(not parts or not parts[0].strip(),
                'Cannot parse %r dependency', dep_str)

    return result


def ensure_version(version, cond):
    if cond:
        for op, cond_version in cond:
            if op == [0]:
                # Make `version` the same length as `cond_version`
                if len(version) > len(cond_version):
                    version = version[:len(cond_version) - 1] + [0]
                if len(version[0]) > len(cond_version[0]):
                    version = [version[0][:len(cond_version[0])], 0]
            if cmp(version, cond_version) not in op:
                return False
    return True


class Spec(object):

    def __init__(self, spec=None, root=None):
        self.path = None
        self.commands = {}
        self.bindings = set()
        self.requires = {}
        self.build_requires = []
        self.source_requires = []
        self.archives = []
        self.applications = []
        self.activity = None
        self.library = None
        self._fields = {}
        self._noarch = True
        self._config = ConfigParser()

        if hasattr(spec, 'readline'):
            self._config.readfp(spec)
        else:
            if spec is not None:
                enforce(exists(spec), 'Recipe file %s does not exist', spec)
                self.path = spec
            elif root is not None:
                # TODO Handle sweets.recipe specs
                self.path = join(root, 'activity', 'activity.info')
            self._config.read(self.path)

        self._read()

    @property
    def root(self):
        if self.path is not None:
            return dirname(dirname(self.path))

    @property
    def types(self):
        result = []
        if self.activity is not None:
            result.append('activity')
        if self.library is not None:
            result.append('library')
        if self.applications:
            result.append('application')
        return result

    @property
    def noarch(self):
        return self._noarch

    def lint(self, is_sweet=None):
        for i in self['license']:
            enforce(i in GOOD_LICENSES,
                    'Not supported "%s" license, see %s for details',
                    i, _POLICY_URL)

    def __getitem__(self, key):
        section = None
        if isinstance(key, tuple):
            if len(key) == 2:
                section, key = key
            else:
                enforce(len(key) == 1)
                key = key[0]
        if not section:
            if key in _FIELDS:
                return self._fields.get(key)
            section = 'DEFAULT'
        return self._get(section, key)

    def __repr__(self):
        return '<Spec %s>' % self['context']

    def _get(self, section, key):
        if self._config.has_option(section, key):
            return self._config.get(section, key)

    def _read(self):
        for section in sorted(self._config.sections()):
            bindings = _parse_bindings(self._get(section, 'binding'))
            self.bindings.update(bindings)
            requires = parse_requires(self._get(section, 'requires'))

            section_type = section.split(':')[0]
            if section_type == 'Activity':
                self._new_activity(section, requires)
            elif section_type == 'Application':
                self._new_command(section, requires)
                self.applications.append(_Application(self._config, section))
            elif section_type == 'Library':
                enforce(':' not in section, '[Library] should be singular')
                enforce(self._get(section, 'binding'),
                        'Option "binding" should exist')
                self.library = _Library(self._config, section)
                self.requires.update(requires)
            elif section_type == 'Package':
                self.requires.update(requires)
            elif section_type == 'Build':
                for i in requires:
                    i.for_build = True
                self.build_requires.extend(requires)
                continue
            elif section_type == 'Source':
                self.source_requires.extend(requires)
                continue
            else:
                if section_type == 'Archive':
                    self._new_archive(section)
                # The further code only for usecase sections
                continue

            for key, (__, typecast) in _FIELDS.items():
                value = self._get(section, key)
                if value is None:
                    continue
                enforce(self._fields.get(key) is None or
                        self._fields[key] == value,
                        'Option %s should be the same for all sections', key)
                if typecast is not None:
                    value = typecast(value)
                self._fields[key] = value

        if self.activity is not None:
            # TODO Switch to `context` tag at the end
            self._fields['context'] = self.activity['bundle_id']
            # Do some backwards compatibility expansions for activities
            if not self['summary'] and self['name']:
                self._fields['summary'] = self['name']
            if not self['version'] and self.activity['activity_version']:
                self._fields['version'] = self.activity['activity_version']
            if not self['stability']:
                self._fields['stability'] = 'stable'
            if not self['homepage']:
                self._fields['homepage'] = \
                        'http://wiki.sugarlabs.org/go/Activities/%s' % \
                        self['name']
            if not self['icon'].lower().endswith('.svg'):
                self._fields['icon'] = join('activity', self['icon'] + '.svg')
            if not self['license']:
                self._fields['license'] = EMPTY_LICENSE

        for key, (required, __) in _FIELDS.items():
            enforce(not required or key in self._fields,
                    'Option "%s" is required', key)
        if 'Build' in self._config.sections():
            enforce(self._get('Build', 'install'),
                    'At least "install" should exists in [Build]')

        if not self['description']:
            self._fields['description'] = self['summary']
        self._fields['version'] = format_version(self['version'])

        if not self.archives:
            self.archives.append(_Archive(self._config, 'DEFAULT'))
        for i in self.applications:
            i.name = '-'.join(i.section.split(':')[1:])

    def _new_command(self, section, requires):
        cmdline = self._get(section, 'exec')
        enforce(cmdline,
                'Option "exec" should exist for [%s] section', section)
        name = section.split(':')[-1] if ':' in section else section.lower()
        command = self.commands[name] = {'exec': cmdline}
        if ':' in section:
            command['requires'] = requires
        else:
            self.requires.update(requires)

    def _new_activity(self, section, requires):
        enforce(':' not in section, '[Activity] should be singular')

        # Support deprecated activity.info options
        for new_key, old_key, fmt in [
                ('exec', 'class', 'sugar-activity %s'),
                ('bundle_id', 'service_name', '%s')]:
            if not self._get(section, new_key) and self._get(section, old_key):
                _logger.warning('Option "%s" is deprecated, use "%s" instead',
                        old_key, new_key)
                self._config.set(section, new_key,
                        fmt % self._get(section, old_key))

        for key in ['icon', 'exec']:
            enforce(self._get(section, key),
                    'Option "%s" should exist for activities', key)

        self._new_command(section, requires)
        self.activity = _Activity(self._config, section)

    def _new_archive(self, section):
        arch = self._get(section, 'arch') or 'all'
        enforce(arch in _ARCHES,
                'Unknown arch %s in [%s], it should be %r',
                arch, section, _ARCHES)
        self._noarch = self._noarch and (arch == 'all')
        if self._get(section, 'lang'):
            assert False, 'Not yet implemented'
            """
            for lang in get_list('langs', section):
                result.add(SubPackage(section, lang))
            """
        else:
            self.archives.append(_Archive(self._config, section))


class _Section(object):

    def __init__(self, config, section):
        self._config = config
        self.section = section
        self.name = None

    def __getitem__(self, key):
        return self._config.get(self.section, key)


class _Archive(_Section):

    @property
    def include(self):
        # TODO per lang includes
        return _parse_list(self['include'])

    @property
    def exclude(self):
        # TODO per lang excludes
        return _parse_list(self['exclude'])

    @property
    def noarch(self):
        return (self['arch'] or 'all') == 'all'


class _Application(_Section):
    pass


class _Activity(_Section):
    pass


class _Library(_Section):
    pass


def _parse_bindings(text):
    result = set()

    def parse_str(bind_str):
        parts = bind_str.split()
        if not parts:
            return
        if parts[0].lower() in ['prepend', 'append', 'replace']:
            mode = parts.pop(0).lower()
        else:
            mode = 'prepend'
        if len(parts) > 1:
            insert = parts.pop().strip(os.sep)
        else:
            insert = ''
        result.add((parts[0], insert, mode))

    for i in _parse_list(text):
        parse_str(i)

    return sorted(result)


def _parse_list(str_list):
    if not str_list:
        return []

    parts = []
    brackets = {('(', ')'): 0,
                ('[', ']'): 0,
                ('"', '"'): 0}
    str_list = str_list.replace("\n", _LIST_SEPARATOR).strip()
    i = 0

    while i < len(str_list):
        if not max(brackets.values()) and \
                str_list[i] in (_LIST_SEPARATOR, ';'):
            parts.append(str_list[:i])
            str_list = str_list[i + 1:]
            i = 0
        else:
            for key in brackets.keys():
                left, right = key
                if str_list[i] == left:
                    brackets[key] += 1
                    break
                elif str_list[i] == right:
                    brackets[key] -= 1
                    break
            i += 1

    parts.append(str_list)

    return [i.strip() for i in parts if i.strip()]
