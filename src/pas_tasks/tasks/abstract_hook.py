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

from dpt_runtime.exception_log_trap import ExceptionLogTrap
from dpt_runtime.not_implemented_exception import NotImplementedException
from dpt_threading.thread import Thread

class AbstractHook(object):
    """
The hook instance based task can be used if the the task store is memory
based.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    # pylint: disable=unused-argument

    def run(self, task_store, _tid, **kwargs):
        """
Starts the execution of this hook synchronously.

:return: (mixed) Task result
:since:  v1.0.0
        """

        with ExceptionLogTrap("pas_tasks"): return self._run_hook(**kwargs)
    #

    def _run_hook(self, **kwargs):
        """
Hook execution

:return: (mixed) Task result
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    def start(self, task_store, _tid, **kwargs):
        """
Starts the execution of this hook asynchronously.

:since: v1.0.0
        """

        thread = Thread(target = self.run, args = ( task_store, _tid ), kwargs = kwargs)
        thread.start()
    #
#
