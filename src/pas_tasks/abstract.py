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

from dpt_module_loader import NamedClassLoader
from dpt_plugins import Hook
from dpt_runtime.not_implemented_exception import NotImplementedException
from dpt_runtime.supports_mixin import SupportsMixin
from dpt_runtime.value_exception import ValueException
from dpt_settings import Settings
from dpt_threading.thread import Thread

from .tasks import AbstractHook

class Abstract(SupportsMixin):
    """
Abstract class for task stores.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    # pylint: disable=unused-argument

    __slots__ = [ "__weakref__", "_log_handler", "task_timeout" ] + SupportsMixin._mixin_slots_
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def __init__(self):
        """
Constructor __init__(Abstract)

:since: v1.0.0
        """

        SupportsMixin.__init__(self)

        self._log_handler = NamedClassLoader.get_singleton("dpt_logging.LogHandler", False)
        """
The LogHandler is called whenever debug messages should be logged or errors
happened.
        """
        self.task_timeout = int(Settings.get("pas_tasks_timeout", 900))
        """
Default timeout for an activated task
        """
    #

    def add(self, tid, hook, timeout = None, **kwargs):
        """
Add a new task with the given TID to the storage for later activation.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        raise NotImplementedException()
    #

    def call(self, params, last_return = None):
        """
Called to initiate a task if its known and valid. A task is only executed
if "last_return" is None.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v1.0.0
        """

        _return = last_return

        if (_return is None and "tid" in params):
            task = self.get(params['tid'])

            if (task is None): is_valid = False
            else:
                is_valid = (True
                            if ("client" not in task['params']
                                or ("client" in params and params['client'] == task['params']['client'])
                               )
                            else False
                           )
            #

            if (is_valid):
                if ("_timeout" in task): self.reregister_timeout(params['tid'])
                _return = self._run_task(task)
            #
        #

        return _return
    #

    def get(self, tid):
        """
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    def is_registered(self, tid, hook = None):
        """
Checks if a given task ID is known.

:param tid: Task ID
:param hook: Task hook to be called

:return: (bool) True if defined
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    def register_timeout(self, tid, hook, timeout = None, **params):
        """
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        raise NotImplementedException()
    #

    def remove(self, tid):
        """
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    def reregister_timeout(self, tid):
        """
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    def _run_task(self, task_data):
        """
Executes a task synchronously.

:param task_data: Task definition

:return: (mixed) Task result
:since:  v1.0.0
        """

        _return = None

        if ("hook" not in task_data or "params" not in task_data): raise ValueException("Given task is unsupported")

        if (isinstance(task_data['hook'], AbstractHook)): _return = task_data['hook'].run(self, **task_data['params'])
        else: _return = Hook.call_one(task_data['hook'], **task_data['params'])

        return _return
    #

    def _start_task(self, task_data):
        """
Calls a task asynchronously.

:param task_data: Task definition

:since: v1.0.0
        """

        if ("hook" not in task_data or "params" not in task_data): raise ValueException("Given task is unsupported")

        if (isinstance(task_data['hook'], AbstractHook)): task_data['hook'].start(self, **task_data['params'])
        else:
            thread = Thread(target = self._run_task, args = ( task_data, ))
            thread.start()
        #
    #

    def unregister_timeout(self, tid):
        """
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    @staticmethod
    def get_instance():
        """
Get a tasks instance.

:return: (object) Task instance on success
:since:  v1.0.0
        """

        raise NotImplementedException()
    #
#
