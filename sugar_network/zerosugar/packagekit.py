# Copyright (C) 2010-2012 Aleksey Lim
# Copyright (C) 2010 Thomas Leonard
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
import locale
import logging
from ConfigParser import ConfigParser
from os.path import exists
from gettext import gettext as _

import dbus
import gobject
from dbus.mainloop.glib import threads_init, DBusGMainLoop

from sugar_network.toolkit import pipe
from active_toolkit import enforce


_PK_CONFILE = '/etc/PackageKit/PackageKit.conf'

_logger = logging.getLogger('zerosugar.packagekit')

_pk = None
_pk_max_resolve = 100
_pk_max_install = 2500


def resolve(names):
    enforce(_get_pk() is not None, 'Cannot connect to PackageKit')

    pipe.feedback('resolve',
            message=_('Resolving %s package name(s)') % len(names))
    _logger.debug('Resolve names %r', names)
    result = {}

    mainloop = gobject.MainLoop()
    while names:
        chunk = names[:min(len(names), _pk_max_resolve)]
        del names[:len(chunk)]

        transaction = _Transaction(mainloop.quit)
        transaction.resolve(chunk)
        mainloop.run()

        missed = set(chunk) - set([i['name'] for i in transaction.packages])
        enforce(not missed,
                'Failed to resolve %s package(s)', ', '.join(missed))
        for pkg in transaction.packages:
            result[pkg['name']] = pkg

    return result


def install(packages):
    ids = [i['pk_id'] for i in packages]

    pipe.feedback('install',
            message=_('Installing %s package(s)') % len(packages))
    _logger.debug('Ask PackageKit to install %r packages', ids)

    mainloop = gobject.MainLoop()
    while ids:
        chunk = ids[:min(len(ids), _pk_max_install)]
        del ids[:len(chunk)]

        transaction = _Transaction(mainloop.quit)
        transaction.install(chunk)
        mainloop.run()

        enforce(transaction.error_code is None or
                transaction.error_code in ('package-already-installed',
                    'all-packages-already-installed'),
                'PackageKit install failed: %s (%s)',
                transaction.error_details, transaction.error_code)


class _Transaction(object):

    def __init__(self, finished_cb):
        self._finished_cb = finished_cb
        self.error_code = None
        self.error_details = None
        self.packages = []

        self._object = dbus.SystemBus().get_object(
                # pylint: disable-msg=E1103
                'org.freedesktop.PackageKit', _get_pk().GetTid(), False)
        self._proxy = dbus.Interface(self._object,
                'org.freedesktop.PackageKit.Transaction')
        self._props = dbus.Interface(self._object, dbus.PROPERTIES_IFACE)

        self._signals = []
        for signal, cb in [
                ('Finished', self.__finished_cb),
                ('ErrorCode', self.__error_code_cb),
                ('Package', self.__package_cb),
                ]:
            self._signals.append(self._proxy.connect_to_signal(signal, cb))

        defaultlocale = locale.getdefaultlocale()[0]
        if defaultlocale is not None:
            self._compat_call([
                    ('SetLocale', defaultlocale),
                    ('SetHints', ['locale=%s' % defaultlocale]),
                    ])

    def resolve(self, names):
        self._proxy.Resolve('none', names)

    def install(self, names):
        _auth_wrapper('org.freedesktop.packagekit.package-install',
                self._compat_call, [
                    ('InstallPackages', names),
                    ('InstallPackages', True, names),
                    ])

    def get_percentage(self):
        if self._object is None:
            return None
        try:
            return self._props.Get('org.freedesktop.PackageKit.Transaction',
                    'Percentage')
        except Exception:
            result, __, __, __ = self._proxy.GetProgress()
            return result

    def _compat_call(self, calls):
        for call in calls:
            method = call[0]
            args = call[1:]
            try:
                dbus_method = self._proxy.get_dbus_method(method)
                return dbus_method(*args)
            except dbus.exceptions.DBusException, e:
                if e.get_dbus_name() not in [
                        'org.freedesktop.DBus.Error.UnknownMethod',
                        'org.freedesktop.DBus.Error.InvalidArgs']:
                    raise
        raise Exception('Cannot call %r DBus method' % calls)

    def __finished_cb(self, status, runtime):
        _logger.debug('Transaction finished: %s', status)
        for i in self._signals:
            i.remove()
        self._finished_cb()
        self._props = None
        self._proxy = None
        self._object = None

    def __error_code_cb(self, code, details):
        self.error_code = code
        self.error_details = details

    def __package_cb(self, status, pk_id, summary):
        from sugar_network import zeroinstall

        package_name, version, arch, __ = pk_id.split(';')
        clean_version = zeroinstall.try_cleanup_distro_version(version)
        if not clean_version:
            _logger.warn('Cannot parse distribution version "%s" '
                    'for package "%s"', version, package_name)
        package = {
                'pk_id': str(pk_id),
                'version': clean_version,
                'name': package_name,
                'arch': zeroinstall.canonical_machine(arch),
                'installed': (status == 'installed'),
                }
        _logger.debug('Resolved PackageKit name: %r', package)
        self.packages.append(package)


def _get_pk():
    global _pk, _pk_max_resolve, _pk_max_install

    if _pk is not None:
        if _pk is False:
            return None
        else:
            return _pk

    gobject.threads_init()
    threads_init()
    DBusGMainLoop(set_as_default=True)
    try:
        bus = dbus.SystemBus()
        pk_object = bus.get_object('org.freedesktop.PackageKit',
                '/org/freedesktop/PackageKit', False)
        _pk = dbus.Interface(pk_object, 'org.freedesktop.PackageKit')
        _logger.info('PackageKit dbus service found')
    except Exception, error:
        _pk = False
        _logger.info('PackageKit dbus service not found: %s', error)
        return None

    if exists(_PK_CONFILE):
        conf = ConfigParser()
        conf.read(_PK_CONFILE)
        if conf.has_option('Daemon', 'MaximumItemsToResolve'):
            _pk_max_resolve = \
                    int(conf.get('Daemon', 'MaximumItemsToResolve'))
        if conf.has_option('Daemon', 'MaximumPackagesToProcess'):
            _pk_max_install = \
                    int(conf.get('Daemon', 'MaximumPackagesToProcess'))

    return _pk


def _auth_wrapper(iface, method, *args):
    _logger.info('Obtain authentication for %s', iface)

    def obtain():
        pk_auth = dbus.SessionBus().get_object(
                'org.freedesktop.PolicyKit.AuthenticationAgent', '/',
                'org.freedesktop.PolicyKit.AuthenticationAgent')
        pk_auth.ObtainAuthorization(iface, dbus.UInt32(0),
                dbus.UInt32(os.getpid()), timeout=300)

    try:
        # PK on f11 needs to obtain authentication at first
        obtain()
        return method(*args)
    except Exception:
        # It seems doesn't work for recent PK
        try:
            return method(*args)
        except dbus.exceptions.DBusException, e:
            if e.get_dbus_name() != \
                    'org.freedesktop.PackageKit.Transaction.RefusedByPolicy':
                raise
            iface, auth = e.get_dbus_message().split()
            if not auth.startswith('auth_'):
                raise
            obtain()
            return method(*args)


if __name__ == '__main__':
    import sys
    from pprint import pprint

    if len(sys.argv) == 1:
        exit()

    logging.basicConfig(level=logging.DEBUG)

    if sys.argv[1] == 'install':
        install(resolve(sys.argv[2:]).values())
    else:
        pprint(resolve(sys.argv[1:]))
