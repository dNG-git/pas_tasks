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

from copy import copy
from time import time
from weakref import ref

from dpt_runtime.binary import Binary
from dpt_threading.instance_lock import InstanceLock
from pas_timed_tasks import TimedTasksMixin

from .abstract import Abstract

class Memory(TimedTasksMixin, Abstract):
    """
A "Memory" instance stores tasks in the application memory. Tasks are run
threaded. Use the LRT implementation for long running or CPU intensive
ones.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    __slots__ = [ "tasks" ] + TimedTasksMixin._mixin_slots_
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

    def __init__(self):
        """
Constructor __init__(Memory)

:since: v1.0.0
        """

        Abstract.__init__(self)
        TimedTasksMixin.__init__(self)

        self.tasks = [ ]
        """
Cache for registered tasks
        """
    #

    @property
    def _next_update_timestamp(self):
        """
Get the implementation specific next "run()" UNIX timestamp.

:return: (float) UNIX timestamp; -1 if no further "run()" is required at the
         moment
:since:  v1.0.0
        """

        _return = -1

        with self._lock:
            if (len(self.tasks) > 0): _return = self.tasks[0]['timestamp']
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

        tid = Binary.str(tid)
        if (timeout is None): timeout = self.task_timeout

        if (self._log_handler is not None): self._log_handler.debug("{0!r} added TID '{1}' with target {2!r} and timeout '{3}'", self, tid, hook, timeout, context = "pas_tasks")

        params = kwargs
        params['_tid'] = tid
        self._insert({ "hook": hook, "params": params, "tid": tid }, timeout)
    #

    def _delete(self, tid):
        """
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v1.0.0
        """

        # pylint: disable=no-member

        _return = False

        with self._lock:
            index = len(self.tasks)

            if (index > 0):
                tasks = (self.tasks.copy() if (hasattr(self.tasks, "copy")) else copy(self.tasks))
                tasks.reverse()

                for position in range(index - 1, -1, -1):
                    if (self.tasks[position]['tid'] == tid):
                        index = position
                        self.tasks.pop(position)
                        _return = True

                        break
                    #
                #
            #
        #

        if (index < 1): self.update_timestamp()
        return _return
    #

    def get(self, tid):
        """
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v1.0.0
        """

        _return = None

        tid = Binary.str(tid)

        with self._lock:
            for position in range(0, len(self.tasks)):
                if (self.tasks[position]['tid'] == tid):
                    _return = self.tasks[position]
                    break
                #
            #
        #

        return _return
    #

    def _insert(self, params, timeout):
        """
Add a new task with the given TID to the storage for later activation.

:param params: Parameter specified
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        if (self.is_started):
            timestamp = time() + timeout

            with self._lock:
                tasks_count = len(self.tasks)

                if (timeout > self.task_timeout):
                    index = 0

                    if (tasks_count > 0):
                        for position in range((tasks_count - 1), -1, -1):
                            if (timestamp > self.tasks[position]['timestamp']):
                                index = 1 + position
                                break
                            #
                        #
                    #
                else:
                    index = len(self.tasks)

                    for position in range(0, tasks_count):
                        if (timestamp < self.tasks[position]['timestamp']):
                            index = position
                            break
                        #
                    #
                #

                params['timestamp'] = timestamp
                self.tasks.insert(index, params)
            #

            if (index < 1): self.update_timestamp(timestamp)
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

        with self._lock:
            for position in range(0, len(self.tasks)):
                if (tid == self.tasks[position]['tid'] and (hook is None or hook == self.tasks[position]['hook'])):
                    _return = True
                    break
                #
            #
        #

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

        tid = Binary.str(tid)
        if (timeout is None): timeout = self.task_timeout

        if (self._log_handler is not None): self._log_handler.debug("{0!r} registered TID '{1}' with target {2!r}", self, tid, hook, context = "pas_tasks")

        params = kwargs
        params['_tid'] = tid
        self._insert({ "hook": hook, "params": params, "tid": tid, "_timeout": timeout }, timeout)
    #

    def remove(self, tid):
        """
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v1.0.0
        """

        tid = Binary.str(tid)
        _return = self._delete(tid)

        if (_return and self._log_handler is not None): self._log_handler.debug("{0!r} removed TID '{1}'", self, tid, context = "pas_tasks")
        return _return
    #

    def reregister_timeout(self, tid):
        """
Updates the task with the given TID to push its expiration time.

:param tid: Task ID

:return: (bool) True on success
:since:  v1.0.0
        """

        tid = Binary.str(tid)
        task = self.get(tid)

        if (task is None): _return = False
        else:
            self.unregister_timeout(tid)
            self.register_timeout(tid, task['hook'], task['_timeout'], task['params'])

            _return = True
        #

        return _return
    #

    def run(self):
        """
Timed task execution

:since: v1.0.0
        """

        task = None

        if (self.is_started):
            with self._lock:
                if (len(self.tasks) > 0 and self.tasks[0]['timestamp'] <= time()): task = self.tasks.pop(0)
                TimedTasksMixin.run(self)
            #
        #

        if (task is not None):
            if ("_timeout" not in task): self._start_task(task)
            elif (self._log_handler is not None): self._log_handler.debug("{0!r} timed out TID '{1}'", self, task['tid'], context = "pas_tasks")
        #
    #

    def unregister_timeout(self, tid):
        """
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v1.0.0
        """

        tid = Binary.str(tid)
        _return = self._delete(tid)

        if (_return and self._log_handler is not None): self._log_handler.debug("{0!r} removed TID '{1}'", self, tid, context = "pas_tasks")
        return _return
    #

    @staticmethod
    def get_instance():
        """
Get the Memory singleton.

:return: (Memory) Object on success
:since:  v1.0.0
        """

        _return = None

        with Memory._instance_lock:
            if (Memory._weakref_instance is not None): _return = Memory._weakref_instance()

            if (_return is None):
                _return = Memory()
                Memory._weakref_instance = ref(_return)
            #
        #

        return _return
    #
#
