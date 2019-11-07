# -*- coding: utf-8 -*-

"""
direct PAS
Python Application Services
----------------------------------------------------------------------------
(C) direct Netware Group - All rights reserved
https://www.direct-netware.de/redirect?pas;tasks

The following license agreement remains valid unless any additions or
changes are being made by direct Netware Group in a written form.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 2 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
----------------------------------------------------------------------------
https://www.direct-netware.de/redirect?licenses;gpl
----------------------------------------------------------------------------
#echo(pasTasksVersion)#
#echo(__FILEPATH__)#
"""

from dpt_runtime.not_implemented_exception import NotImplementedException
from pas_timed_tasks import AbstractTimed

from .abstract import Abstract

class AbstractPersistent(Abstract, AbstractTimed):
    """
"AbstractPersistent" instances provide additional scheduling related
methods for persistent tasks.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    def __init__(self):
        """
Constructor __init__(AbstractPersistent)

:since: v1.0.0
        """

        AbstractTimed.__init__(self)
        Abstract.__init__(self)
    #

    @classmethod
    def is_executing_daemon(cls):
        """
True if the instance is executing scheduled tasks.

:return: (bool) True if executing scheduled tasks
:since:  v1.0.0
        """

        _return = False

        try:
            persistent_tasks = cls.get_instance()
            _return = persistent_tasks.is_started
        except NotImplementedException: pass

        return _return
    #
#
