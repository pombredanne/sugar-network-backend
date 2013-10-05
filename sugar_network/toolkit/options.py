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

"""Command-line options parsing utilities."""

import os
import sys
from os.path import exists, expanduser, isdir, join


class Option(object):
    """Configuration option.

    `Option` object will be used as command-line argument and
    configuration file option. All these objects will be automatically
    collected from `sugar_server.env` module and from `etc` module from
    all services.

    """
    #: Collected by `Option.seek()` options by name.
    items = {}
    #: Configure files used to form current configuration
    config_files = []
    #: `Option` value for --config setting
    config = None

    _parser = None
    _config_to_save = None

    def __init__(self, description=None, default=None, short_option=None,
            type_cast=None, type_repr=None, action=None, name=None):
        """
        :param description:
            description string
        :param default:
            default value for the option
        :param short_option:
            value in for of `-<char>` to use as a short option for command-line
            parser
        :param type_cast:
            function that will be uses to type cast to option type
            while setting option value
        :param type_repr:
            function that will be uses to type cast from option type
            while converting option value to string
        :param action:
            value for `action` argument of `OptionParser.add_option()`
        :param name:
            specify option name instead of reusing variable name

        """
        if default is not None and type_cast is not None:
            default = type_cast(default)
        self.default = default
        self._value = default
        self.description = description
        self.type_cast = type_cast
        self.type_repr = type_repr
        self.short_option = short_option or ''
        self.action = action
        self.section = None
        self.name = name
        self.attr_name = None

    @property
    def long_option(self):
        """Long command-line argument name."""
        return '--%s' % self.name

    # pylint: disable-msg=E0202
    @property
    def value(self):
        """Get option raw value."""
        return self._value

    # pylint: disable-msg=E1101, E0102, E0202
    @value.setter
    def value(self, x):
        """Set option value.

        The `Option.type_cast` function will be used for type casting specified
        value to option.

        """
        if x is None:
            self._value = None
        elif self.type_cast is not None:
            self._value = self.type_cast(x)
        else:
            self._value = str(x) or None

    @staticmethod
    def get(section, key):
        if Option._parser is None or \
                not Option._parser.has_option(section, key):
            return None
        return Option._parser.get(section, key)

    @staticmethod
    def seek(section, mod=None):
        """Collect `Option` objects.

        Call this function before any usage of `Option` objects to scan
        module(s) for option objects.

        :param section:
            arbitrary name to group options per section
        :param mod:
            mdoule object to search for `Option` objects;
            if omited, use caller's module

        """
        if mod is None:
            mod_name = _get_frame(1).f_globals['__name__']
            mod = sys.modules[mod_name]

        if type(mod) in (list, tuple):
            options = dict([(i.name.replace('-', '_'), i) for i in mod])
        else:
            options = dict([(i, getattr(mod, i)) for i in dir(mod)])

        for name in sorted(options):
            attr = options[name]
            # Options might be from different `options` modules
            if not (type(attr).__name__ == 'Option' and
                    type(attr).__module__.split('.')[-1] == 'options'):
                continue
            attr.attr_name = name
            attr.name = name.replace('_', '-')
            attr.module = mod
            attr.section = section
            Option.items[attr.name] = attr

    @staticmethod
    def load(config_files):
        """Load option settings from configure files.

        If application accepts command-line arguments,
        use `Option.parse_args()` function instead.

        :param config_files:
            list of paths to files that will be used to read default
            option values; this value will initiate `Option.config` variable

        """
        Option._load(None, config_files)

    @staticmethod
    def parse_args(parser, config_files=None, stop_args=None, notice=None):
        """Load configure files and combine them with command-line arguments.

        :param parser:
            `OptionParser` object to parse for command-line arguments
        :param config_files:
            list of paths to files that will be used to read default
            option values; this value will initiate `Option.config` variable
        :param stop_args:
            optional list of arguments that should stop further command-line
            arguments parsing
        :param notice:
            optional notice to use only in command-line related cases
        :returns:
            (`options`, `args`) tuple with data parsed from
            command-line arguments

        """
        Option._bind(parser, config_files, notice)

        if stop_args:
            parser.disable_interspersed_args()
        options, args = parser.parse_args()
        if stop_args and args and args[0] not in stop_args:
            parser.enable_interspersed_args()
            options, args = parser.parse_args(args, options)

        Option._load(options, None)

        # Update default values accoriding to current values
        # to expose them while processing --help
        for prop in [Option.config] + Option.items.values():
            if prop is None:
                continue
            parser.set_default(prop.name.replace('-', '_'), prop)

        return options, args

    @staticmethod
    def help():
        """Current configuration in human readable form.

        :returns:
            list of lines

        """
        from textwrap import wrap

        sections = {}
        for prop in sorted(Option.items):
            prop = Option.items[prop]
            section = sections.setdefault(prop.section, [])
            section.append(prop)

        lines = []
        for section, props in sections.items():
            lines.append('[%s]' % section)
            for prop in props:
                lines.append('\n'.join(
                        ['# %s' % i for i in wrap(prop.description, 78)]))
                value = '\n\t'.join(str(prop).split('\n'))
                lines.append('%s = %s' % (prop.name, value))
            lines.append('')

        return '\n'.join(lines)

    @staticmethod
    def save(path=None):
        from cStringIO import StringIO
        from sugar_network.toolkit import new_file

        if Option._parser is None:
            raise RuntimeError('No configure files to save')
        if not path:
            if not Option._config_to_save:
                raise RuntimeError('No configure files to save')
            path = Option._config_to_save

        for prop in Option.items.values():
            if not Option._parser.has_section(prop.section):
                Option._parser.add_section(prop.section)
            Option._parser.set(prop.section, prop.name, prop)
        result = StringIO()
        Option._parser.write(result)

        with new_file(path) as f:
            f.write(result.getvalue())

    @staticmethod
    def bool_cast(x):
        if not x or str(x).strip().lower() in ['', 'false', 'none']:
            return False
        else:
            return bool(x)

    @staticmethod
    def list_cast(x):
        if isinstance(x, basestring):
            return [i for i in x.strip().split() if i]
        else:
            return x

    @staticmethod
    def list_repr(x):
        return ' '.join(x)

    @staticmethod
    def paths_cast(x):
        if isinstance(x, basestring):
            return [i for i in x.strip().split(':') if i]
        else:
            return x

    @staticmethod
    def paths_repr(x):
        return ':'.join(x)

    def __str__(self):
        if self.value is None:
            return ''
        else:
            if self.type_repr is None:
                return str(self.value)
            else:
                return self.type_repr(self.value)

    def __unicode__(self):
        return self.__str__()

    @staticmethod
    def _bind(parser, config_files, notice):
        import re

        if config_files:
            Option.config = Option()
            Option.config.name = 'config'
            Option.config.attr_name = 'config'
            Option.config.description = \
                    'colon separated list of paths to configuration file(s)'
            Option.config.short_option = '-c'
            Option.config.type_cast = \
                    lambda x: [i for i in re.split('[\\s:;,]+', x) if i]
            Option.config.type_repr = \
                    lambda x: ':'.join(x)
            Option.config.value = ':'.join(config_files)

        for prop in [Option.config] + Option.items.values():
            if prop is None:
                continue
            desc = prop.description
            if prop.value is not None:
                desc += ' [%default]'
            if notice:
                desc += '; ' + notice
            if parser is not None:
                parser.add_option(prop.short_option, prop.long_option,
                        action=prop.action, help=desc)

    @staticmethod
    def _load(options, config_files):
        from ConfigParser import ConfigParser
        Option._parser = ConfigParser()

        def load_config(path):
            if Option._config_to_save is None:
                Option._config_to_save = path
            Option.config_files.append(path)
            Option._parser.read(path)

        if not config_files and Option.config is not None:
            config_files = Option.config.value

        for config_path in config_files or []:
            config_path = expanduser(config_path)
            if isdir(config_path):
                for path in sorted(os.listdir(config_path)):
                    load_config(join(config_path, path))
            elif exists(config_path):
                load_config(config_path)

        for prop in Option.items.values():
            if hasattr(options, prop.attr_name) and \
                    getattr(options, prop.attr_name) is not None:
                prop.value = getattr(options, prop.attr_name)
            elif Option._parser.has_option(prop.section, prop.name):
                prop.value = Option._parser.get(prop.section, prop.name)


def _get_frame(frame_no):
    """Return Python call stack frame.

    The reason to have this wrapper is that this stack information is a private
    data and might depend on Python implementaion.

    :param frame_no:
        number of stack frame starting from caller's stack position
    :returns:
        frame object

    """
    # +1 since the calling `get_frame` adds one more frame
    # pylint: disable-msg=W0212
    return sys._getframe(frame_no + 1)
