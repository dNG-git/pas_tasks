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

from pas_database import NothingMatchedException

from .persistent_lrt_hook import PersistentLrtHook
from ..database_task_context import DatabaseTaskContext
from ..instances import Task

class DatabaseLrtHook(PersistentLrtHook):
    """
A "DatabaseLrtHook" is a database backed persistent, long running task.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    __slots__ = [ ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def _run_hook(self):
        """
Hook execution

:since: v1.0.0
        """

        task = None
        tid = self.params.get("_tid")

        if (tid is not None):
            try: task = Task.load_tid(tid)
            except NothingMatchedException: pass
        #

        if (task is None):
            if (self._log_handler is not None): self._log_handler.warning("{0!r} is executed without a database task entry", self, context = "pas_tasks")
            PersistentLrtHook._run_hook(self)
        else:
            with DatabaseTaskContext(task): PersistentLrtHook._run_hook(self)
        #
    #
#
