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

from time import time
from weakref import ref

from dpt_threading.instance_lock import InstanceLock
from pas_database import Connection, NothingMatchedException

from .abstract_persistent import AbstractPersistent
from .database_task_context import DatabaseTaskContext
from .instances import Task
from .tasks import PersistentLrtHook

class Database(AbstractPersistent):
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

    __slots__ = [ ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """
    _instance_lock = InstanceLock()
    """
Thread safety lock
    """
    _weakref_instance = None
    """
Tasks weakref instance
    """

    @property
    def _next_update_timestamp(self):
        """
Get the implementation specific next "run()" UNIX timestamp.

:return: (float) UNIX timestamp; -1 if no further "run()" is required at the
         moment
:since:  v1.0.0
        """

        _return = -1

        with Connection.get_instance():
            try:
                task = Task.load_next()
                _return = task.time_scheduled
            except NothingMatchedException: pass
        #

        return _return
    #

    def add(self, tid, hook, timeout = None, **kwargs):
        """
Add a new task with the given TID to the storage for later activation.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        if (timeout is None): timeout = self.task_timeout
        timestamp = int(time() + timeout)

        params = kwargs
        self._insert(tid, hook, params, timestamp)
        if (self._log_handler is not None): self._log_handler.debug("{0!r} registered TID '{1}' with target {2!r} and timeout '{3}'", self, tid, hook, timeout, context = "pas_tasks")
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
            task_status = (Task.STATUS_UNKNOWN if (task is None) else task['_task'].status)

            if (task_status == Task.STATUS_WAITING): _return = AbstractPersistent.call(self, params)
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

        task = Task.load_tid(tid)

        _return = { "hook": task.hook,
                    "params": task.params,
                    "tid": tid,
                    "_task": task
                  }

        if ("_timeout" in _return['params']): _return['_timeout'] = _return['params']['_timeout']

        return _return
    #

    def _insert(self, tid, hook, params, time_scheduled = None, timeout = None):
        """
Add a new task with the given TID to the storage for later activation.

:param params: Parameter specified
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        if (self.is_started):
            task = Task()
            task.tid = tid
            task.name = tid

            if (isinstance(hook, PersistentLrtHook)):
                task.hook = hook.underlying_hook

                params.update(hook.params)
                params['_lrt_hook'] = True
            else: task.hook = hook

            task.params = params

            if (time_scheduled is not None):
                time_scheduled = time_scheduled
                task.time_scheduled = time_scheduled
            #

            if (timeout is not None): task.timeout = timeout

            task.save()

            if (time_scheduled is not None): self.update_timestamp(time_scheduled)
        #
    #

    def is_registered(self, tid, hook = None):
        """
Checks if a given task ID is known.

:param tid: Task ID
:param hook: Task hook to be called

:return: (bool) True if defined
:since:  v1.0.0
        """

        _return = False

        try:
            _return = (Task.load_tid(tid) is not None)
        except NothingMatchedException: pass

        return _return
    #

    def register_timeout(self, tid, hook, timeout = None, **kwargs):
        """
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        if (timeout is None): timeout = self.task_timeout
        timeout_time = int(time() + timeout)

        params = kwargs
        params['_timeout'] = timeout
        self._insert(tid, hook, params, timeout = timeout_time)
        if (self._log_handler is not None): self._log_handler.debug("{0!r} registered TID '{1}' with target {2!r}", self, tid, hook, context = "pas_tasks")
    #

    def remove(self, tid):
        """
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v1.0.0
        """

        # pylint: disable=broad-except

        _return = False

        try: Task.load_tid(tid).delete()
        except NothingMatchedException: pass

        if (_return and self._log_handler is not None): self._log_handler.debug("{0!r} removed TID '{1}'", self, tid, context = "pas_tasks")
        return _return
    #

    def reregister_timeout(self, tid):
        """
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v1.0.0
        """

        _return = False

        try:
            task = Task.load_tid(tid)
            params = task.params

            if ("_timeout" in params):
                task.timeout = (time() + params['_timeout'])
                task.save()

                _return = True
            #
        except NothingMatchedException: pass

        return _return
    #

    def run(self):
        """
Timed task execution

:since: v1.0.0
        """

        with self._lock:
            task = None
            task_data = None

            if (self.is_started):
                try: task = Task.load_next()
                except NothingMatchedException: pass
            #

            if (task is not None):
                task.status = Task.STATUS_QUEUED
                task.save()

                task_data = { "hook": task.hook,
                              "params": task.params,
                              "_task": task
                            }
            #

            AbstractPersistent.run(self)
            if (task_data is not None): self._start_task(task_data)
        #
    #

    def _run_task(self, task_data):
        """
Executes a task synchronously.

:param task_data: Task definition

:return: (mixed) Task result
:since:  v1.0.0
        """

        _return = None

        if (isinstance(task_data.get("_task"), Task)):
            with DatabaseTaskContext(task_data['_task']): _return = AbstractPersistent._run_task(self, task_data)
        else: _return = AbstractPersistent._run_task(self, task_data)

        return _return
    #

    def start(self, params = None, last_return = None):
        """
Starts the database tasks scheduler.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:since: v1.0.0
        """

        Task._reset_stale_running()
        AbstractPersistent.start(self, params, last_return)
    #

    def unregister_timeout(self, tid):
        """
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v1.0.0
        """

        # pylint: disable=broad-except

        _return = False

        try:
            task = Task.load_tid(tid)
            _return = task.delete()
        except NothingMatchedException: pass

        if (_return and self._log_handler is not None): self._log_handler.debug("{0!r} removed TID '{1}'", self, tid, context = "pas_tasks")
        return _return
    #

    @staticmethod
    def get_instance():
        """
Get the Database singleton.

:return: (Database) Object on success
:since:  v1.0.0
        """

        _return = None

        with Database._instance_lock:
            if (Database._weakref_instance is not None): _return = Database._weakref_instance()

            if (_return is None):
                _return = Database()
                Database._weakref_instance = ref(_return)
            #
        #

        return _return
    #
#
