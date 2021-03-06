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

from collections import deque

from dpt_module_loader import NamedClassLoader
from dpt_runtime.exception_log_trap import ExceptionLogTrap
from dpt_runtime.not_implemented_exception import NotImplementedException
from dpt_settings import Settings
from dpt_threading.thread import Thread
from dpt_threading.thread_lock import ThreadLock

from .abstract_hook import AbstractHook

class AbstractLrtHook(AbstractHook):
    """
Long running tasks (LRT) are resource intensive calls where only a limited
amount should run parallel.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    # pylint: disable=unused-argument

    __slots__ = [ "context_id", "independent_scheduling", "_log_handler", "max_retry_delay", "min_retry_delay", "_params", "tid" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """
    _context_limit = 0
    """
Limit for allowed context executions
    """
    _context_queues = { }
    """
Currently active context queues
    """
    _lock = ThreadLock()
    """
Thread safety lock
    """

    def __init__(self):
        """
Constructor __init__(AbstractLrtHook)

:since: v1.0.0
        """

        AbstractHook.__init__(self)

        self.context_id = "pas.tasks.Context"
        """
Default timeout for an activated task
        """
        self.independent_scheduling = False
        """
Usually queues are filled and executed as long as new tasks of the same type
arrive. Set this variable to true to reschedule these tasks independently.
        """
        self._log_handler = NamedClassLoader.get_singleton("dpt_logging.LogHandler", False)
        """
The LogHandler is called whenever debug messages should be logged or errors
happened.
        """
        self.max_retry_delay = Settings.get("pas_tasks_lrt_retry_delay_max", 120)
        """
Maximum delay for rescheduled tasks
        """
        self.min_retry_delay = Settings.get("pas_tasks_lrt_retry_delay_min", 10)
        """
Minimum delay for rescheduled tasks
        """
        self._params = None
        """
Hook parameters
        """
        self.tid = None
        """
Task ID
        """
    #

    @property
    def params(self):
        """
Returns the hook parameters used when being called.

:return: (dict) Hook parameters
:since:  v1.0.0
        """

        return self._params
    #

    @property
    def _queue_delay(self):
        """
Returns the delay value for rescheduling.

:return: (float) Delay for rescheduling a task
:since:  v1.0.0
        """

        _return = self.min_retry_delay

        with AbstractLrtHook._lock:
            multiplier = 1

            for context_name in AbstractLrtHook._context_queues:
                multiplier += len(AbstractLrtHook._context_queues[context_name])
            #

            _return *= (multiplier / AbstractLrtHook._context_limit)
        #

        if (_return > self.max_retry_delay): _return = self.max_retry_delay

        return _return
    #

    def _get_task_or_finish(self):
        """
Returns the next task from the context queue if any. Otherwise finish the
execution.

:return: (mixed) Task from the same context queue; None otherwise
:since:  v1.0.0
        """

        with AbstractLrtHook._lock:
            _return = (AbstractLrtHook._context_queues[self.context_id].popleft()
                       if (len(AbstractLrtHook._context_queues[self.context_id]) > 0) else
                       None
                      )

            if (_return is None):
                del(AbstractLrtHook._context_queues[self.context_id])
                if (self._log_handler is not None): self._log_handler.debug("{0!r} finished context '{1}'", self, self.context_id, context = "pas_tasks")
            #
        #

        return _return
    #

    def run(self, task_store, _tid, **kwargs):
        """
Starts the execution of this hook synchronously.

:return: (mixed) Task result
:since:  v1.0.0
        """

        raise NotImplementedException()
    #

    def _run(self):
        """
Returns the manager instance responsible for this hook.

:return: (object) Hook manager on success
:since:  v1.0.0
        """

        # pylint: disable=protected-access

        task = self._get_task_or_finish()

        while (task is not None):
            if (self._log_handler is not None): self._log_handler.debug("{0!r} is executing tasks with context '{1}'", self, self.context_id, context = "pas_tasks")
            with ExceptionLogTrap("pas_tasks"): task._run_hook()

            task = self._get_task_or_finish()
        #
    #

    def _run_hook(self):
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

        is_active = False
        is_queued = False

        if (self.params is None): self._params = kwargs

        with AbstractLrtHook._lock:
            if (AbstractLrtHook._context_limit < 1): AbstractLrtHook._context_limit = Settings.get("pas_tasks_lrt_limit", 1)

            if (self.context_id in AbstractLrtHook._context_queues):
                if (not self.independent_scheduling):
                    AbstractLrtHook._context_queues[self.context_id].append(self)
                    if (self._log_handler is not None): self._log_handler.debug("{0!r} queued with context '{1}'", self, self.context_id, context = "pas_tasks")

                    is_active = True
                    is_queued = True
                #
            elif (len(AbstractLrtHook._context_queues) < AbstractLrtHook._context_limit):
                AbstractLrtHook._context_queues[self.context_id] = deque()
                AbstractLrtHook._context_queues[self.context_id].append(self)
                if (self._log_handler is not None): self._log_handler.debug("{0!r} initialized in context '{1}'", self, self.context_id, context = "pas_tasks")

                is_queued = True
            #
        #

        if (not is_queued): task_store.add(_tid, self, self._queue_delay)
        elif (not is_active):
            thread = Thread(target = self._run)
            thread.start()
        #
    #
#
