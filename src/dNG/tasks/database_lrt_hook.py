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

from dNG.data.tasks.database_task import DatabaseTask
from dNG.data.tasks.database_task_context import DatabaseTaskContext
from dNG.database.nothing_matched_exception import NothingMatchedException
from dNG.plugins.hook import Hook

from .abstract_lrt_hook import AbstractLrtHook

class DatabaseLrtHook(AbstractLrtHook):
    """
A "DatabaseLrtHook" is an database stored long running task.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2
    """

    def __init__(self, hook, **kwargs):
        """
Constructor __init__(DatabaseLrtHook)

:since: v0.2.00
        """

        AbstractLrtHook.__init__(self)

        self.hook = hook
        """
Hook
        """
        self.params = kwargs
        """
Hook parameters
        """

        self.context_id = hook
        self.independent_scheduling = True
    #

    def get_hook(self):
        """
Returns the hook being called of this long running task instance.

:return: (str) Hook
:since:  v0.2.00
        """

        return self.hook
    #

    def get_params(self):
        """
Returns the hook parameters used when being called.

:return: (dict) Hook parameters
:since:  v0.2.00
        """

        return self.params
    #

    def _run_hook(self):
        """
Hook execution

:since: v0.2.00
        """

        task = None
        tid = self.params.get("_tid")

        if (tid is not None):
            try: task = DatabaseTask.load_tid(tid)
            except NothingMatchedException: pass
        #

        if (task is None):
            if (self.log_handler is not None): self.log_handler.warning("{0!r} is executed without a database task entry", self, context = "pas_tasks")
            Hook.call_one(self.hook, **self.params)
        else:
            with DatabaseTaskContext(task): Hook.call_one(self.hook, **self.params)
        #
    #
#
