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

from .database_task import DatabaseTask

class DatabaseTaskContext(object):
    """
A "Database" instance stores tasks in the database.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2
    """

    def __init__(self, task):
        """
Constructor __init__(DatabaseTaskContext)

:param task: Database task

:since: v0.2.00
        """

        self.task = task
        """
Database task
        """
    #

    def __enter__(self):
        """
python.org: Enter the runtime context related to this object.

:since: v0.2.00
        """

        try: self.task.set_status(DatabaseTask.STATUS_RUNNING)
        except Exception:
            self.task.set_status(DatabaseTask.STATUS_FAILED)
            raise
        finally: self.task.save()
    #

    def __exit__(self, exc_type, exc_value, traceback):
        """
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v0.2.00
        """

        if (exc_type is not None or exc_value is not None):
            params = self.task.get_params()

            if ("error" not in params):
                params['error'] = { "type": "exception",
                                    "exception": "".join(format_exception(exc_type, exc_value, traceback))
                                  }

                self.task.set_params(params)
            #

            self.task.set_status(DatabaseTask.STATUS_FAILED)
        elif (self.task.get_status() == DatabaseTask.STATUS_RUNNING):
            self.task.set_status(DatabaseTask.STATUS_WAITING
                                 if (self.task.is_timeout_set()) else
                                 DatabaseTask.STATUS_COMPLETED
                                )
        #

        self.task.save()

        return False
    #
#
