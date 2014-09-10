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

"""Get LSB (Linux Standard Base) Distribution information."""

import re
import math
import subprocess
from os.path import exists


_distributor_id = None
_release = None

_DERIVATES = {
        'Trisquel': (
            'Ubuntu', [
                lambda x: '%02d.%02d' %
                    (int(float(x)) + 6, 4
                        if float(x) - int(float(x)) < 0.5 else 10),
                ],
            ),
        'LinuxMint': (
            'Ubuntu', [
                lambda x: '%02d.%02d' %
                    (math.ceil(int(x) / 2.) + 5, [4, 10][(int(x) - 1) % 2]),
                ],
            ),
        'Tuquito': (
            'Ubuntu', [
                lambda x: '%02d.%02d' %
                    (int(float(x)) + 6, 4 if float(x) == int(float(x)) else 10)
                ],
            ),
        'Debian': (
            'Debian', [
                lambda x: '%d.0' % int(float(x)),
                ],
            ),
        }


def distributor_id():
    """Current distribution LSB `Distributor ID`.

    :returns:
        string value

    """
    if _distributor_id is None:
        _init()
    return _distributor_id


def release():
    """Current distribution LSB `Release`.

    :returns:
        string value

    """
    if _release is None:
        _init()
    return _release


def name():
    return '%s-%s' % (distributor_id(), release())


def _init():
    global _distributor_id, _release

    def check_derivates():
        global _distributor_id, _release

        if _distributor_id not in _DERIVATES:
            return
        _distributor_id, releases = _DERIVATES[_distributor_id]
        for i in releases:
            release_value = i(_release)
            if release_value:
                break
        else:
            release_value = ''
        _release = release_value

    try:
        _distributor_id, _release = _lsb_release()
        check_derivates()
    except OSError:
        if exists('/etc/lsb-release'):
            _distributor_id, _release = _parse_lsb_release()
            check_derivates()
        if not _release:
            _distributor_id, _release = _find_lsb_release()

    return _distributor_id, _release


def _lsb_release():
    lsb_id, lsb_release = '', ''

    process = subprocess.Popen(['lsb_release', '--all'],
            stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, __ = process.communicate()

    if process.returncode == 0:
        for line in str(stdout).split('\n'):
            if ':' not in line:
                continue
            key, value = line.split(':', 1)
            if key.strip() == 'Distributor ID':
                lsb_id = value.strip()
            elif key.strip() == 'Release':
                lsb_release = value.strip()

    return lsb_id, lsb_release


def _parse_lsb_release():
    lsb_id, lsb_release = '', ''

    for line in file('/etc/lsb-release').readlines():
        key, value = line.split('=')
        value = value.strip().strip('\'"')
        if key == 'DISTRIB_ID':
            lsb_id = value
        elif key == 'DISTRIB_RELEASE':
            lsb_release = value

    return lsb_id, lsb_release


def _find_lsb_release():
    if exists('/etc/debian_version'):
        return 'Debian', file('/etc/debian_version').read().strip()

    elif exists('/etc/redhat-release'):
        line = file('/etc/redhat-release').read().strip()

        match = re.search('Fedora.*?\\W([0-9.]+)', line)
        if match is not None:
            return 'Fedora', match.group(1)

        match = re.search('CentOS.*?\\W([0-9.]+)', line)
        if match is not None:
            return 'CentOS', match.group(1)

        match = re.search('\\W([0-9.]+)', line)
        if match is not None:
            return 'RHEL', match.group(1)

    else:
        # TODO http://linuxmafia.com/faq/Admin/release-files.html
        return '', ''


if __name__ == '__main__':
    print distributor_id(), release()
