# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.Memory
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

from copy import copy
from time import time
from weakref import ref

from dNG.pas.data.binary import Binary
from dNG.pas.plugins.hooks import Hooks
from dNG.pas.runtime.thread_lock import ThreadLock
from dNG.pas.tasks.abstract_timed import AbstractTimed
from .abstract import Abstract

class Memory(Abstract, AbstractTimed):
#
	"""
A "Memory" instance stores tasks in the application memory. Tasks are run
threaded. Use the LRT implementation for long running or CPU intensive
ones.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	lock = ThreadLock()
	"""
Thread safety lock
	"""
	weakref_instance = None
	"""
Tasks weakref instance
	"""

	def __init__(self):
	#
		"""
Constructor __init__(Memory)

:since: v0.1.00
		"""

		Abstract.__init__(self)
		AbstractTimed.__init__(self)

		self.tasks = [ ]
		"""
Cache for registered tasks
		"""
	#

	def _delete(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		# pylint: disable=no-member

		_return = False

		with Memory.lock:
		#
			index = len(self.tasks)

			if (index > 0):
			#
				tasks = (self.tasks.copy() if (hasattr(self.tasks, "copy")) else copy(self.tasks))
				tasks.reverse()

				for position in range(index - 1, -1, -1):
				#
					if (self.tasks[position]['tid'] == tid):
					#
						index = position
						self.tasks.pop(position)
						_return = True

						break
					#
				#
			#

			if (len(self.tasks) == 0):
			#
				Hooks.unregister("dNG.pas.Status.shutdown", self.stop)
				Hooks.unregister("dNG.pas.tasks.call", self.task_call)
			#
		#

		if (index < 1): self.update_timestamp()
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

		with Memory.lock:
		#
			if (len(self.tasks) > 0): _return = self.tasks[0]['timestamp']
		#

		return _return
	#

	def _insert(self, params, timeout):
	#
		"""
Add a new task with the given TID to the storage for later activation.

:param params: Parameter specified
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		if (self.timer_active):
		#
			timestamp = int(time() + timeout)

			with Memory.lock:
			#
				tasks_count = len(self.tasks)

				if (timeout > self.task_timeout):
				#
					index = 0

					if (tasks_count > 0):
					#
						for position in range((tasks_count - 1), -1, -1):
						#
							if (timestamp > self.tasks[position]['timestamp']):
							#
								index = 1 + position
								break
							#
						#
					#
				#
				else:
				#
					index = len(self.tasks)

					for position in range(0, tasks_count):
					#
						if (timestamp < self.tasks[position]['timestamp']):
						#
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
	#
		"""
Checks if a given task ID is known.

:param tid: Task ID
:param hook: Task hook to be called

:return: (bool) True if defined
:since:  v0.1.00
		"""

		_return = False

		with Memory.lock:
		#
			for position in range(0, len(self.tasks)):
			#
				if (tid == self.tasks[position]['tid'] and (hook == None or hook == self.tasks[position]['hook'])):
				#
					_return = True
					break
				#
			#
		#

		return _return
	#

	def run(self):
	#
		"""
Timed task execution

:since: v0.1.00
		"""

		with Memory.lock:
		#
			task = (self.tasks.pop(0) if (len(self.tasks) > 0 and self.tasks[0]['timestamp'] <= time()) else None)
			AbstractTimed.run(self)
		#

		if (task != None):
		#
			if ("timeout" not in task): self._task_start(task)
			elif (self.log_handler != None): self.log_handler.debug("pas.Tasks timed out TID '{0}'".format(task['tid']))
		#
	#

	def task_add(self, tid, hook, timeout = None, **kwargs):
	#
		"""
Add a new task with the given TID to the storage for later activation.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		tid = Binary.str(tid)
		if (timeout == None): timeout = self.task_timeout

		if (self.log_handler != None): self.log_handler.debug("pas.Tasks added TID '{0}' with target '{1!r}'".format(tid, hook))

		params = kwargs
		params['tid'] = tid
		self._insert({ "hook": hook, "params": params, "tid": tid }, timeout)
	#

	def task_get(self, tid):
	#
		"""
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v0.1.00
		"""

		_return = None

		tid = Binary.str(tid)

		with Memory.lock:
		#
			for position in range(0, len(self.tasks)):
			#
				if (self.tasks[position]['tid'] == tid):
				#
					_return = self.tasks[position]
					break
				#
			#
		#

		return _return
	#

	def task_remove(self, tid):
	#
		"""
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.1.00
		"""

		tid = Binary.str(tid)
		_return = self._delete(tid)

		if (_return and self.log_handler != None): self.log_handler.debug("pas.Tasks removed TID '{0}'".format(tid))
		return _return
	#

	def timeout_register(self, tid, hook, timeout = None, **kwargs):
	#
		"""
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		tid = Binary.str(tid)
		if (timeout == None): timeout = self.task_timeout

		if (self.log_handler != None): self.log_handler.debug("pas.Tasks registered TID '{0}' with target '{1!r}'".format(tid, hook))

		params = kwargs
		params['tid'] = tid
		self._insert({ "hook": hook, "params": params, "tid": tid, "timeout": timeout }, timeout)
	#

	def timeout_reregister(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.1.00
		"""

		tid = Binary.str(tid)
		task = self.task_get(tid)

		if (task == None): _return = False
		else:
		#
			self.timeout_unregister(tid)
			self.timeout_register(tid, task['hook'], task['timeout'], task['params'])

			_return = True
		#

		return _return
	#

	def timeout_unregister(self, tid):
	#
		"""
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.1.00
		"""

		tid = Binary.str(tid)
		_return = self._delete(tid)

		if (_return and self.log_handler != None): self.log_handler.debug("pas.Tasks removed TID '{0}'".format(tid))
		return _return
	#

	@staticmethod
	def get_instance():
	#
		"""
Get the Memory singleton.

:return: (Memory) Object on success
:since:  v0.1.00
		"""

		_return = None

		with Memory.lock:
		#
			if (Memory.weakref_instance != None): _return = Memory.weakref_instance()

			if (_return == None):
			#
				_return = Memory()
				Memory.weakref_instance = ref(_return)
			#
		#

		return _return
	#
#

##j## EOF