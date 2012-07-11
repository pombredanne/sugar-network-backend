# Copyright (C) 2011-2012 Aleksey Lim
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

"""Command-line options parsing utilities.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/options.py$
$Date: 2012-07-11$

"""

import sys
from os.path import exists, expanduser
from gettext import gettext as _


class Option(object):
    """Configuration option.

    `Option` object will be used as command-line argument and
    configuration file option. All these objects will be automatically
    collected from `sugar_server.env` module and from `etc` module from
    all services.

    """
    #: Collected by `Option.seek()` options in original order.
    unsorted_items = []
    #: Collected by `Option.seek()` options by name.
    items = {}
    #: Collected by `Option.seek()` options by section.
    sections = {}
    #: Configure files used to form current configuration
    config_files = []

    _config = None
    _config_files_to_save = []

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
    def seek(section, mod=None):
        """Collect `Option` objects.

        Function will populate `Option.unsorted_items`, `Option.items` and
        `Option.sections` values. Call this function before any usage
        of `Option` objects.

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
            options = dict([(i.name, i) for i in mod])
        else:
            options = dict([(i, getattr(mod, i)) for i in dir(mod)])

        for name in sorted(options):
            attr = options[name]
            # Options might be from different `options` modules
            if not (type(attr).__name__ == 'Option' and \
                    type(attr).__module__.split('.')[-1] == 'options'):
                continue

            attr.attr_name = name
            attr.name = name.replace('_', '-')
            attr.module = mod
            attr.section = section

            Option.unsorted_items.append(attr)
            Option.items[attr.name] = attr
            if section not in Option.sections:
                Option.sections[section] = {}
            Option.sections[section][attr.name] = attr

    @staticmethod
    def load(config_files):
        """Load option settings from configure files.

        If application accepts command-line arguments,
        use `Option.parse_args()` function instead.

        :param config_files:
            list of paths to files that will be used to read default
            option values; this value will initiate `Option.config` variable

        """
        Option._merge(None, config_files)

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

        Option._merge(options, None)

        # Update default values accoriding to current values
        # to expose them while processing --help
        for prop in [Option._config] + Option.items.values():
            parser.set_default(prop.name.replace('-', '_'), prop)

        return options, args

    @staticmethod
    def bind(parser, config_files=None, notice=None):
        # DEPRECATED
        Option._bind(parser, config_files, notice)

    @staticmethod
    def merge(options, config_files=None):
        # DEPRECATED
        Option._merge(options, config_files)

    @staticmethod
    def export():
        """Current configuration in human readable form.

        :returns:
            list of lines

        """
        import textwrap

        lines = []
        sections = set()

        for prop in Option.unsorted_items:
            if prop.section not in sections:
                if sections:
                    lines.append('')
                lines.append('[%s]' % prop.section)
                sections.add(prop.section)
            lines.append('\n'.join(
                    ['# %s' % i for i in textwrap.wrap(prop.description, 78)]))
            value = '\n\t'.join(str(prop).split('\n'))
            lines.append('%s = %s' % (prop.name, value))

        return lines

    @staticmethod
    def save(path=None):
        if not path:
            if not Option._config_files_to_save:
                raise RuntimeError(_('No configure files to save'))
            path = Option._config_files_to_save[-1]
        with file(path, 'w') as f:
            f.write('\n'.join(Option.export()))

    @staticmethod
    def bool_cast(x):
        if not x or str(x).strip().lower() in ['', 'false', 'none']:
            return False
        else:
            return bool(x)

    @staticmethod
    def list_cast(x):
        if isinstance(x, str) or isinstance(x, unicode):
            return [i for i in x.strip().split(':') if i]
        else:
            return x

    @staticmethod
    def list_repr(x):
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
            Option._config = Option()
            Option._config.name = 'config'
            Option._config.attr_name = 'config'
            Option._config.description = \
                    _('colon separated list of paths to alternative ' \
                    'configuration file(s)')
            Option._config.short_option = '-c'
            Option._config.type_cast = \
                    lambda x: [i for i in re.split('[\s:;,]+', x) if i]
            Option._config.type_repr = \
                    lambda x: ':'.join(x)
            Option._config.value = ':'.join(config_files)

        for prop in [Option._config] + Option.items.values():
            desc = prop.description
            if prop.value is not None:
                desc += ' [%default]'
            if notice:
                desc += '; ' + notice
            if parser is not None:
                parser.add_option(prop.short_option, prop.long_option,
                        action=prop.action, help=desc)

    @staticmethod
    def _merge(options, config_files):
        from ConfigParser import ConfigParser

        if config_files is None:
            if Option._config is None:
                raise RuntimeError(_('Method Option.bind was not called or ' \
                        'its config_files argument was None'))
            config_files = Option._config.value

        configs = [ConfigParser()]
        for config in config_files:
            if isinstance(config, ConfigParser):
                configs.append(config)
            else:
                config = expanduser(config)
                if exists(config):
                    Option.config_files.append(config)
                    configs[0].read(config)
                Option._config_files_to_save.append(config)

        for prop in Option.items.values():
            if hasattr(options, prop.attr_name) and \
                    getattr(options, prop.attr_name) is not None:
                prop.value = getattr(options, prop.attr_name)
            else:
                for config in configs:
                    if config.has_option(prop.section, prop.name):
                        prop.value = config.get(prop.section, prop.name)


class Command(object):
    """Service command.

    `Command` is a way to have custom sub-commands in services. All these
    objects will be automatically collected from `etc` module
    from all services.

    """
    #: Collected by `Command.seek()` commands by name.
    items = {}
    #: Collected by `Command.seek()` commands by section.
    sections = {}

    def __init__(self, description=None, cmd_format=None):
        """
        :param description:
            command description
        :param cmd_format:
            part of description to explain additional command arguments

        """
        self.description = description or ''
        self.cmd_format = cmd_format or ''
        self.name = None
        self.attr_name = None

    @staticmethod
    def seek(section, mod=None):
        """Collect `Command` objects.

        Function will populate `Command.items` and `Command.sections` values.
        Call this function before any usage of `Command` objects.

        :param section:
            arbitrary name to group options per section
        :param mod:
            mdoule object to search for `Option` objects;
            if omited, use caller's module

        """
        if mod is None:
            mod_name = _get_frame(1).f_globals['__name__']
            mod = sys.modules[mod_name]

        for name in sorted(dir(mod)):
            attr = getattr(mod, name)
            # Commands might be from different `options` modules
            if not (type(attr).__name__ == 'Command' and \
                    type(attr).__module__.split('.')[-1] == 'options'):
                continue

            attr.name = name.replace('_', '-')
            attr.attr_name = name
            attr.module = mod
            attr.section = section

            Command.items[attr.name] = attr
            if section not in Command.sections:
                Command.sections[section] = {}
            Command.sections[section][attr.name] = attr

    @staticmethod
    def call(mod, name, *args, **kwargs):
        """Call the command.

        Specfied module should contain a function with a name
        `CMD_<command-name>()`. All additional `Command.call()` arguments
        will be passed as-is to command implementaion function.

        :param mod:
            module to search for command implementaion
        :param name:
            command name
        :returns:
            what command implementaion returns

        """
        cmd = Command.items.get(name)
        if cmd is None:
            raise RuntimeError(_('No such command, %s') % name)

        func_name = 'CMD_%s' % cmd.attr_name
        if not hasattr(mod, func_name):
            raise RuntimeError(_('No such command, %s, in module %s') % \
                    (name, mod.__name__))
        getattr(mod, func_name)(*args, **kwargs)

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.__str__()


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
