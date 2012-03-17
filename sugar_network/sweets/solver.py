# Copyright (C) 2011-2012, Aleksey Lim
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

import logging
from gettext import gettext as _

from zeroinstall.injector import model
from zeroinstall.injector.driver import Driver

from sugar_network.sweets.config import config
from sugar_network.sweets.solution import Solution


_logger = logging.getLogger('sweets')


def solve(req, record_details=False):
    driver = Driver(config=config, requirements=req)
    driver.solver.record_details = record_details

    driver.solver.solve(req.interface_uri,
                driver.target_arch, command_name=req.command)

    result = Solution(driver.solver.selections, req)
    result.details = dict((k.uri, v) \
            for k, v in (driver.solver.details or {}).items())
    result.ready = driver.solver.ready

    if not result.ready:
        # pylint: disable-msg=W0212
        failure_reason = driver.solver._failure_reason
        if not failure_reason:
            missed_ifaces = [iface.uri for iface, impl in \
                    driver.solver.selections.items() if impl is None]
            failure_reason = _('Cannot find requireed implementations ' \
                    'for %s') % ', '.join(missed_ifaces)
        result.failure_reason = model.SafeException(failure_reason)

    return result
