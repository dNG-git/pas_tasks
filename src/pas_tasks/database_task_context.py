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

from traceback import format_exception

from dpt_runtime.value_exception import ValueException

from .instances import Task

class DatabaseTaskContext(object):
    """
A "Database" instance stores tasks in the database.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    __slots__ = [ "task" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def __init__(self, task):
        """
Constructor __init__(DatabaseTaskContext)

:param task: Database task

:since: v1.0.0
        """

        if (not isinstance(task, Task)): raise ValueException("Task '{0!r}' given is not a database one".format(task))

        self.task = task
        """
Database task
        """
    #

    def __enter__(self):
        """
python.org: Enter the runtime context related to this object.

:since: v1.0.0
        """

        with self.task:
            try: self.task.status = Task.STATUS_RUNNING
            except Exception:
                self.task.status = Task.STATUS_FAILED
                raise
            finally: self.task.save()
        #
    #

    def __exit__(self, exc_type, exc_value, traceback):
        """
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v1.0.0
        """

        with self.task:
            if (exc_type is not None or exc_value is not None):
                params = self.task.params

                if ("error" not in params):
                    params['error'] = { "type": "exception",
                                        "exception": "".join(format_exception(exc_type, exc_value, traceback))
                                      }

                    self.task.params = params
                #

                self.task.status = Task.STATUS_FAILED
            elif (self.task.status == Task.STATUS_RUNNING):
                self.task.status = (Task.STATUS_WAITING
                                    if (self.task.is_timeout_set) else
                                    Task.STATUS_COMPLETED
                                   )
            #

            self.task.save()
        #

        return False
    #
#
