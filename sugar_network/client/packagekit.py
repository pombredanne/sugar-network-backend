# Copyright (C) 2010-2013 Aleksey Lim
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

from sugar_network.toolkit import lsb_release, gbus, enforce


_PK_MAX_RESOLVE = 100
_PK_MAX_INSTALL = 2500

_PMS_PATHS = {
        'Debian': '/var/lib/dpkg/status',
        'Fedora': '/var/lib/rpm/Packages',
        'Ubuntu': '/var/lib/dpkg/status',
        }

_logger = logging.getLogger('packagekit')
_pms_path = _PMS_PATHS.get(lsb_release.distributor_id())


def mtime():
    if _pms_path:
        return os.stat(_pms_path).st_mtime


def resolve(names):
    result = {}

    while names:
        chunk = names[:min(len(names), _PK_MAX_RESOLVE)]
        del names[:len(chunk)]

        _logger.debug('Resolve %r', chunk)

        resp = gbus.call(_pk, 'Resolve', 'none', chunk)
        missed = set(chunk) - set(resp.packages.keys())
        enforce(not missed, 'Failed to resolve %s', ', '.join(missed))
        result.update(resp.packages)

    return result


def install(packages):
    ids = [i['pk_id'] for i in packages]
    while ids:
        chunk = ids[:min(len(ids), _PK_MAX_INSTALL)]
        del ids[:len(chunk)]

        _logger.debug('Install %r', chunk)

        resp = gbus.call(_pk, 'InstallPackages', True, chunk)
        enforce(resp.error_code in (
                    'package-already-installed',
                    'all-packages-already-installed', None),
                'Installation failed: %s (%s)',
                resp.error_details, resp.error_code)


class _Response(object):

    def __init__(self):
        self.error_code = None
        self.error_details = None
        self.packages = {}


def _pk(result, op, *args):
    import dbus

    bus = dbus.SystemBus()
    pk = dbus.Interface(
            bus.get_object(
                'org.freedesktop.PackageKit', '/org/freedesktop/PackageKit',
                False),
            'org.freedesktop.PackageKit')
    txn = dbus.Interface(
            bus.get_object('org.freedesktop.PackageKit', pk.GetTid(), False),
            'org.freedesktop.PackageKit.Transaction')
    resp = _Response()
    signals = []

    def Finished_cb(status, runtime):
        _logger.debug('Transaction finished: %s', status)
        for i in signals:
            i.remove()
        result.set(resp)

    def ErrorCode_cb(code, details):
        resp.error_code = code
        resp.error_details = details

    def Package_cb(status, pk_id, summary):
        from sugar_network.client import solver

        package_name, version, arch, __ = pk_id.split(';')
        clean_version = solver.try_cleanup_distro_version(version)
        if not clean_version:
            _logger.warn('Cannot parse distribution version "%s" '
                    'for package "%s"', version, package_name)
        if package_name in resp.packages:
            return
        package = {
                'pk_id': str(pk_id),
                'version': clean_version,
                'name': package_name,
                'arch': solver.canonicalize_machine(arch),
                'installed': (status == 'installed'),
                }
        _logger.debug('Found: %r', package)
        resp.packages[package_name] = package

    for signal, cb in [
            ('Finished', Finished_cb),
            ('ErrorCode', ErrorCode_cb),
            ('Package', Package_cb),
            ]:
        signals.append(txn.connect_to_signal(signal, cb))

    op = txn.get_dbus_method(op)
    try:
        op(*args)
    except dbus.exceptions.DBusException, error:
        if error.get_dbus_name() != \
                'org.freedesktop.PackageKit.Transaction.RefusedByPolicy':
            raise
        iface, auth = error.get_dbus_message().split()
        if not auth.startswith('auth_'):
            raise
        auth = dbus.SessionBus().get_object(
                'org.freedesktop.PolicyKit.AuthenticationAgent', '/',
                'org.freedesktop.PolicyKit.AuthenticationAgent')
        auth.ObtainAuthorization(iface, dbus.UInt32(0),
                dbus.UInt32(os.getpid()), timeout=300)
        op(*args)


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
