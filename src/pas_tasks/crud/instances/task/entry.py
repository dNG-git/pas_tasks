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

from pas_crud_engine import NothingMatchedException, OperationNotSupportedException
from pas_crud_engine.instances import Abstract

from ....memory import Memory as MemoryTasks
from ....persistent import Persistent as PersistentTasks

class Entry(Abstract):
    """
CRUD entity instance for "Entry".

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    def get_metadata(self, **kwargs):
        """
"get" operation for "task/entry/:id/metadata"

:return: (object) Entry instance
:since:  v1.0.0
        """

        if (kwargs['_selected_value'] is None): raise OperationNotSupportedException()
        task = kwargs['_selected_value']

        tid = (task['tid'] if ("tid" in task) else kwargs['_select_id'])

        _return = { 'tid': tid,
                    'hook': str(task['hook']),
                    'params': task['params'],
                    'type': ('memory'
                             if (MemoryTasks.get_instance().is_registered(tid)) else
                             'persistent'
                            )
                  }

        if ("name" in task): _return['name'] = task['name']
        if ("status" in task): _return['status'] = task['status']
        if ("time_started" in task): _return['time_started'] = task['time_started']
        if ("time_scheduled" in task): _return['time_scheduled'] = task['time_scheduled']
        if ("time_updated" in task): _return['time_updated'] = task['time_updated']
        if ("timeout" in task): _return['timeout'] = task['timeout']

        return _return
    #

    def is_valid(self, **kwargs):
        """
Returns true if the given ID for "task/entry[/:id]" is valid

:return: (bool) True if valid
:since:  v1.0.0
        """

        _return = False

        if (kwargs['_select_id'] is not None):
            try:
                self.select(**kwargs)
                _return = True
            except NothingMatchedException: pass
        #

        return _return
    #

    @Abstract.restrict_to_access_control_validated_execution
    def select(self, **kwargs):
        """
Internal select operation for "task/entry[/:id]"

:return: (object) Entry instance
:since:  v1.0.0
        """

        if (kwargs['_select_id'] is None): raise OperationNotSupportedException()

        self.access_control.validate(self, "task.Entry.get", tid = kwargs['_select_id'])

        persistent_tasks = PersistentTasks.get_instance()

        _return = (persistent_tasks.get(kwargs['_select_id'])
                   if (persistent_tasks.is_registered(kwargs['_select_id'])) else
                   MemoryTasks.get_instance().get(kwargs['_select_id'])
                  )

        if (_return is None): raise NothingMatchedException()
        self.access_control.validate(self, "task.Entry.get", task = _return)

        return _return
    #
#
