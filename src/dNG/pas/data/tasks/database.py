# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.Database
"""
"""n// NOTE
----------------------------------------------------------------------------
direct PAS
Python Application Services
----------------------------------------------------------------------------
(C) direct Netware Group - All rights reserved
http://www.direct-netware.de/redirect.py?pas;tasks

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
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
----------------------------------------------------------------------------
http://www.direct-netware.de/redirect.py?licenses;gpl
----------------------------------------------------------------------------
#echo(pasTasksVersion)#
#echo(__FILEPATH__)#
----------------------------------------------------------------------------
NOTE_END //n"""

from time import time
from weakref import ref

from dNG.pas.database.connection import Connection
from dNG.pas.database.nothing_matched_exception import NothingMatchedException
from dNG.pas.plugins.hook import Hook
from dNG.pas.runtime.thread_lock import ThreadLock
from dNG.pas.tasks.abstract_timed import AbstractTimed
from .abstract import Abstract
from .database_task import DatabaseTask
from .database_task_context import DatabaseTaskContext

class Database(Abstract, AbstractTimed):
#
	"""
A "Database" instance stores tasks in the database.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	_lock = ThreadLock()
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

:since: v0.1.00
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

:since: v0.1.00
		"""

		if (timeout == None): timeout = self.task_timeout
		timestamp = int(time() + timeout)

		params = kwargs
		self._insert(tid, hook, params, timestamp)
		if (self.log_handler != None): self.log_handler.debug("pas.Tasks registered TID '{0}' with target '{1!r}'".format(tid, hook))
	#

	def call(self, params, last_return = None):
	#
		"""
Called to initiate a task if its known and valid. A task is only executed
if "last_return" is None.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v0.1.00
		"""

		_return = last_return

		if (_return == None and "tid" in params):
		#
			task = self.get(params['tid'])

			if (task != None
			    and task['_task'].get_status() == DatabaseTask.STATUS_WAITING
			   ): _return = Abstract.call(self, params)
		#

		return _return
	#

	def get(self, tid):
	#
		"""
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v0.1.00
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

:return: (int) UNIX timestamp; -1 if no further "run()" is required at the
         moment
:since:  v0.1.00
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

:since: v0.1.00
		"""

		if (self.timer_active):
		#
			with Connection.get_instance():
			#
				task = DatabaseTask()
				task.set_tid(tid)
				task.set_name(tid)
				task.set_hook(hook)
				task.set_params(params)

				if (time_scheduled != None):
				#
					time_scheduled = int(time_scheduled)
					task.set_time_scheduled(time_scheduled)
				#

				if (timeout != None):
				#
					timeout = int(timeout)
					task.set_timeout(timeout)
				#

				task.save()

				time_to_execution = (None if (time_scheduled == None) else time_scheduled - int(time()))

				if (time_to_execution != None
				    and (self.timer_timeout < 0 or self.timer_timeout > time_to_execution)
				   ): self.update_timestamp(time_scheduled)
			#
		#
	#

	def is_registered(self, tid, hook = None):
	#
		"""
Checks if a given task ID is known.

:param tid: Task ID
:param hook: Task hook to be called

:return: (bool) True if defined
:since:  v0.1.00
		"""

		return (False if (DatabaseTask.load_tid(tid) == None) else True)
	#

	def register_timeout(self, tid, hook, timeout = None, **kwargs):
	#
		"""
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		if (timeout == None): timeout = self.task_timeout
		timeout_time = int(time() + timeout)

		params = kwargs
		params['_timeout'] = timeout
		self._insert(tid, hook, params, timeout = timeout_time)
		if (self.log_handler != None): self.log_handler.debug("pas.Tasks registered TID '{0}' with target '{1!r}'".format(tid, hook))
	#

	def remove(self, tid):
	#
		"""
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.1.00
		"""

		# pylint: disable=broad-except

		_return = False

		try:
		#
			task = DatabaseTask.load_tid(tid)
			_return = task.delete()
		#
		except NothingMatchedException: pass

		if (_return and self.log_handler != None): self.log_handler.debug("pas.Tasks removed TID '{0}'".format(tid))
		return _return
	#

	def reregister_timeout(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.1.00
		"""

		_return = False

		try:
		#
			task = DatabaseTask.load_tid(tid)
			params = task.get_params()

			if ("_timeout" in params):
			#
				task.set_timeout(time() + params['_timeout'])
				_return = True

				if (self.timer_timeout < 0 or self.timer_timeout > params['_timeout']): self.update_timestamp()
			#
		#
		except NothingMatchedException: pass

		return _return
	#

	def run(self):
	#
		"""
Timed task execution

:since: v0.1.00
		"""

		with Connection.get_instance(), Database._lock:
		#
			task_data = None

			try:
			#
				task = DatabaseTask.load_next()
				task.set_status(DatabaseTask.STATUS_QUEUED)
				task.save()

				if (not task.is_timeout_set()):
				#
					task_data = { "hook": task.get_hook(),
					              "params": task.get_params(),
					              "_task": task
					            }
				#
			#
			except NothingMatchedException: pass

			AbstractTimed.run(self)
			if (task_data != None): self._start_task(task_data)
		#
	#

	def _run_task(self, task_data):
	#
		"""
Executes a task synchronously.

:param task_data: Task definition

:return: (mixed) Task result
:since:  v0.1.00
		"""

		_return = None

		if ("_task" in task_data and isinstance(task_data['_task'], DatabaseTask)):
		#
			task_context = DatabaseTaskContext(task_data['_task'])
			with task_context: _return = Abstract._run_task(self, task_data)
		#
		else: _return = Abstract._run_task(self, task_data)

		return _return
	#

	def unregister_timeout(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		# pylint: disable=broad-except

		_return = False

		try:
		#
			task = DatabaseTask.load_tid(tid)
			_return = task.delete()
		#
		except NothingMatchedException: pass

		if (_return and self.log_handler != None): self.log_handler.debug("pas.Tasks removed TID '{0}'".format(tid))
		return _return
	#

	@staticmethod
	def get_instance():
	#
		"""
Get the Database singleton.

:return: (Database) Object on success
:since:  v0.1.00
		"""

		_return = None

		with Database._lock:
		#
			if (Database._weakref_instance == None): Hook.load("tasks")
			else: _return = Database._weakref_instance()

			if (_return == None):
			#
				_return = Database()
				Database._weakref_instance = ref(_return)
			#
		#

		return _return
	#
#

##j## EOF