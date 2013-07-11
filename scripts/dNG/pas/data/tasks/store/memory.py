# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.store.Memory
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
from .abstract import Abstract

class Memory(Abstract):
#
	"""
A "Memory" instance stores tasks in the application memory.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	def __init__(self):
	#
		"""
Constructor __init__(Memory)

:since: v0.1.00
		"""

		Abstract.__init__(self)

		self.tasks = [ ]
		"""
Cache for registered tasks
		"""
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

		with Memory.synchronized:
		#
			if (len(self.tasks) > 0): _return = self.tasks[0]['timestamp']
		#

		return _return
	#

	def run(self):
	#
		"""
Worker loop

:since: v0.1.00
		"""

		with Memory.synchronized:
		#
			if (len(self.tasks) > 0 and self.tasks[0]['timestamp'] <= time()):
			#
				task = self.tasks.pop(0)

				if ("timeout" not in task): Hooks.call(task['hook'], **task['params'])
				elif (self.log_handler != None): self.log_handler.debug("pas.Tasks timed out TID '{0}'".format(task['tid']))
			#
		#

		Abstract.run(self)
	#

	def task_add(self, tid, hook, timeout = None, params = { }):
	#
		"""
Add a new task with the given TID to the storage for later activation.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		_index = 1
		tid = Binary.str(tid)
		if (timeout == None): timeout = self.task_timeout

		with Memory.synchronized:
		#
			if (self.log_handler != None): self.log_handler.debug("pas.Tasks added TID '{0}' with target '{1}'".format(tid, hook))
			_index = len(self.tasks)
			timestamp = time() + timeout

			if (_index == 0):
			#
				Hooks.register("dNG.pas.status.shutdown", self.stop)
				Hooks.register("dNG.pas.tasks.call", self.task_call)
				Memory.get_instance()
			#

			if (timeout > self.task_timeout):
			#
				if (_index > 0):
				#
					for position in range(_index - 1, -1, -1):
					#
						if (timestamp > self.tasks[position]['timestamp']):
						#
							_index = position
							break
						#
					#
				#
			#
			else:
			#
				_index = None

				for position in range(0, len(self.tasks)):
				#
					if (timestamp < self.tasks[position]['timestamp']):
					#
						_index = position
						break
					#
				#

				if (_index == None): _index = len(self.tasks)
			#

			params['tid'] = tid
			self.tasks.insert(_index, { "hook": hook, "params": params, "tid": tid, "timestamp": timestamp })
		#

		if (_index < 1): self.update_timestamp()
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

		with Memory.synchronized:
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

	def timeout_register(self, tid, hook, timeout = None, params = { }):
	#
		"""
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		_index = 1
		tid = Binary.str(tid)
		if (timeout == None): timeout = self.task_timeout

		with Memory.synchronized:
		#
			if (self.log_handler != None): self.log_handler.debug("pas.Tasks registered TID '{0}' with target '{1}'".format(tid, hook))
			_index = len(self.tasks)
			timestamp = time() + timeout

			if (_index == 0):
			#
				Hooks.register("dNG.pas.status.shutdown", self.stop)
				Hooks.register("dNG.pas.tasks.call", self.task_call)
				Memory.get_instance()
			#

			if (timeout > self.task_timeout):
			#
				if (_index > 0):
				#
					for position in range(_index - 1, -1, -1):
					#
						if (timestamp > self.tasks[position]['timestamp']):
						#
							_index = position
							break
						#
					#
				#
			#
			else:
			#
				_index = None

				for position in range(0, len(self.tasks)):
				#
					if (timestamp < self.tasks[position]['timestamp']):
					#
						_index = position
						break
					#
				#

				if (_index == None): _index = len(self.tasks)
			#

			params['tid'] = tid
			self.tasks.insert(_index, { "hook": hook, "params": params, "tid": tid, "timestamp": timestamp, "timeout": timeout })
		#

		if (_index < 1): self.update_timestamp()
	#

	def timeout_reregister(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

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

:return: (bool) True on success
:since:  v0.1.00
		"""

		_return = True

		_index = 1
		tid = Binary.str(tid)

		with Memory.synchronized:
		#
			_index = len(self.tasks)

			if (_index > 0):
			#
				tasks = (self.tasks.copy() if (hasattr(self.tasks, "copy")) else copy(self.tasks))
				tasks.reverse()

				for position in range(_index - 1, -1, -1):
				#
					if (self.tasks[position]['tid'] == tid):
					#
						if (self.log_handler != None): self.log_handler.debug("pas.Tasks removed TID '{0}'".format(tid))

						_index = position
						self.tasks.pop(position)
						_return = True

						break
					#
				#
			#

			if (len(self.tasks) == 0):
			#
				Hooks.unregister("dNG.pas.status.shutdown", self.stop)
				Hooks.unregister("dNG.pas.tasks.call", self.task_call)
			#
		#

		if (_index < 1): self.update_timestamp()
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

		with Memory.synchronized:
		#
			if (Memory.weakref_instance != None): _return = Memory.weakref_instance()

			if (_return == None):
			#
				_return = Memory()
				_return.start()
				Memory.weakref_instance = ref(_return)
			#
		#

		return _return
	#

	@staticmethod
	def register(tid, hook, timeout = None, **params):
	#
		"""
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		Memory.get_instance().timeout_register(tid, hook, timeout, params)
	#

	@staticmethod
	def reregister(tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.1.00
		"""

		return Memory.get_instance().timeout_reregister(tid)
	#

	@staticmethod
	def unregister(tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		return Memory.get_instance().timeout_unregister(tid)
	#
#

##j## EOF