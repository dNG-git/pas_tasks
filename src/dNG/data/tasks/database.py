# -*- coding: utf-8 -*-
##j## BOF

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

from dNG.database.connection import Connection
from dNG.database.nothing_matched_exception import NothingMatchedException
from dNG.runtime.instance_lock import InstanceLock
from dNG.tasks.abstract_timed import AbstractTimed

from .abstract import Abstract
from .database_task import DatabaseTask
from .database_task_context import DatabaseTaskContext

class Database(Abstract, AbstractTimed):
#
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

	_instance_lock = InstanceLock()
	"""
Thread safety lock
	"""
	_weakref_instance = None
	"""
Tasks weakref instance
	"""

	def __init__(self):
	#
		"""
Constructor __init__(Database)

:since: v0.2.00
		"""

		AbstractTimed.__init__(self)
		Abstract.__init__(self)
	#

	def add(self, tid, hook, timeout = None, **kwargs):
	#
		"""
Add a new task with the given TID to the storage for later activation.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.2.00
		"""

		if (timeout is None): timeout = self.task_timeout
		timestamp = int(time() + timeout)

		params = kwargs
		self._insert(tid, hook, params, timestamp)
		if (self.log_handler is not None): self.log_handler.debug("{0!r} registered TID '{1}' with target {2!r} and timeout '{3}'", self, tid, hook, timeout, context = "pas_tasks")
	#

	def call(self, params, last_return = None):
	#
		"""
Called to initiate a task if its known and valid. A task is only executed
if "last_return" is None.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v0.2.00
		"""

		_return = last_return

		if (_return is None and "tid" in params):
		#
			task = self.get(params['tid'])
			task_status = (DatabaseTask.STATUS_UNKNOWN if (task is None) else task['_task'].get_status())

			if (task_status == DatabaseTask.STATUS_WAITING): _return = Abstract.call(self, params)
		#

		return _return
	#

	def get(self, tid):
	#
		"""
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v0.2.00
		"""

		task = DatabaseTask.load_tid(tid)

		_return = { "hook": task.get_hook(),
		            "params": task.get_params(),
		            "tid": tid,
		            "_task": task
		          }

		if ("_timeout" in _return['params']): _return['_timeout'] = _return['params']['_timeout']

		return _return
	#

	def _get_next_update_timestamp(self):
	#
		"""
Get the implementation specific next "run()" UNIX timestamp.

:return: (float) UNIX timestamp; -1 if no further "run()" is required at the
         moment
:since:  v0.2.00
		"""

		_return = -1

		with Connection.get_instance():
		#
			try:
			#
				task = DatabaseTask.load_next()
				_return = task.get_time_scheduled()
			#
			except NothingMatchedException: pass
		#

		return _return
	#

	def _insert(self, tid, hook, params, time_scheduled = None, timeout = None):
	#
		"""
Add a new task with the given TID to the storage for later activation.

:param params: Parameter specified
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.2.00
		"""

		if (self.timer_active):
		#
			task = DatabaseTask()
			task.set_tid(tid)
			task.set_name(tid)
			task.set_hook(hook)
			task.set_params(params)

			if (time_scheduled is not None):
			#
				time_scheduled = int(time_scheduled)
				task.set_time_scheduled(time_scheduled)
			#

			if (timeout is not None):
			#
				timeout = int(timeout)
				task.set_timeout(timeout)
			#

			task.save()

			time_to_execution = (None if (time_scheduled is None) else time_scheduled - int(time()))

			if (time_to_execution is not None
			    and (self.timer_timeout < 0 or self.timer_timeout > time_to_execution)
			   ): self.update_timestamp(time_scheduled)
		#
	#

	def is_registered(self, tid, hook = None):
	#
		"""
Checks if a given task ID is known.

:param tid: Task ID
:param hook: Task hook to be called

:return: (bool) True if defined
:since:  v0.2.00
		"""

		return (False if (DatabaseTask.load_tid(tid) is None) else True)
	#

	def register_timeout(self, tid, hook, timeout = None, **kwargs):
	#
		"""
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.2.00
		"""

		if (timeout is None): timeout = self.task_timeout
		timeout_time = int(time() + timeout)

		params = kwargs
		params['_timeout'] = timeout
		self._insert(tid, hook, params, timeout = timeout_time)
		if (self.log_handler is not None): self.log_handler.debug("{0!r} registered TID '{1}' with target {2!r}", self, tid, hook, context = "pas_tasks")
	#

	def remove(self, tid):
	#
		"""
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.2.00
		"""

		# pylint: disable=broad-except

		_return = False

		try:
		#
			task = DatabaseTask.load_tid(tid)
			_return = task.delete()
		#
		except NothingMatchedException: pass

		if (_return and self.log_handler is not None): self.log_handler.debug("{0!r} removed TID '{1}'", self, tid, context = "pas_tasks")
		return _return
	#

	def reregister_timeout(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.2.00
		"""

		_return = False

		try:
		#
			task = DatabaseTask.load_tid(tid)
			params = task.get_params()

			if ("_timeout" in params):
			#
				task.set_timeout(time() + params['_timeout'])
				task.save()

				_return = True
			#
		#
		except NothingMatchedException: pass

		return _return
	#

	def run(self):
	#
		"""
Timed task execution

:since: v0.2.00
		"""

		with self.lock:
		#
			task = None
			task_data = None

			if (self.timer_active):
			#
				try: task = DatabaseTask.load_next()
				except NothingMatchedException: pass
			#

			if (task is not None):
			#
				task.set_status(DatabaseTask.STATUS_QUEUED)
				task.save()

				task_data = { "hook": task.get_hook(),
				              "params": task.get_params(),
				              "_task": task
				            }
			#

			AbstractTimed.run(self)
			if (task_data is not None): self._start_task(task_data)
		#
	#

	def _run_task(self, task_data):
	#
		"""
Executes a task synchronously.

:param task_data: Task definition

:return: (mixed) Task result
:since:  v0.2.00
		"""

		_return = None

		if (isinstance(task_data.get("_task"), DatabaseTask)):
		#
			task_context = DatabaseTaskContext(task_data['_task'])
			with task_context: _return = Abstract._run_task(self, task_data)
		#
		else: _return = Abstract._run_task(self, task_data)

		return _return
	#

	def start(self, params = None, last_return = None):
	#
		"""
Starts the database tasks scheduler.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:since: v0.2.00
		"""

		DatabaseTask._reset_stale_running()
		AbstractTimed.start(self, params, last_return)
	#

	def unregister_timeout(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.2.00
		"""

		# pylint: disable=broad-except

		_return = False

		try:
		#
			task = DatabaseTask.load_tid(tid)
			_return = task.delete()
		#
		except NothingMatchedException: pass

		if (_return and self.log_handler is not None): self.log_handler.debug("{0!r} removed TID '{1}'", self, tid, context = "pas_tasks")
		return _return
	#

	@staticmethod
	def get_instance():
	#
		"""
Get the Database singleton.

:return: (Database) Object on success
:since:  v0.2.00
		"""

		_return = None

		with Database._instance_lock:
		#
			if (Database._weakref_instance is not None): _return = Database._weakref_instance()

			if (_return is None):
			#
				_return = Database()
				Database._weakref_instance = ref(_return)
			#
		#

		return _return
	#
#

##j## EOF