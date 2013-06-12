# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.store.memory
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

from dNG.pas.data.binary import direct_binary
from dNG.pas.plugins.hooks import direct_hooks
from .abstract import direct_abstract

class direct_memory(direct_abstract):
#
	"""
"direct_memory" stores tasks in the application memory.

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
Constructor __init__(direct_memory)

:since: v0.1.00
		"""

		direct_abstract.__init__(self)

		self.tasks = [ ]
		"""
Cache for registered tasks
		"""
	#

	def get_next_update_timestamp(self):
	#
		"""
Get the implementation specific next "run()" UNIX timestamp.

:access: protected
:return: (int) UNIX timestamp; -1 if no further "run()" is required at the
         moment
:since:  v0.1.00
		"""

		var_return = -1

		with direct_memory.synchronized:
		#
			if (len(self.tasks) > 0): var_return = self.tasks[0]['timestamp']
		#

		return var_return
	#

	def run(self):
	#
		"""
Worker loop

:access: protected
:since:  v0.1.00
		"""

		with direct_memory.synchronized:
		#
			if (len(self.tasks) > 0 and self.tasks[0]['timestamp'] <= time()):
			#
				task = self.tasks.pop(0)

				if ("timeout" not in task): direct_hooks.call(task['hook'], **task['params'])
				elif (self.log_handler != None): self.log_handler.debug("pas.tasks timed out TID '{0}'".format(task['tid']))
			#
		#

		direct_abstract.run(self)
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

		index = 1
		tid = direct_binary.str(tid)
		if (timeout == None): timeout = self.task_timeout

		with direct_memory.synchronized:
		#
			if (self.log_handler != None): self.log_handler.debug("pas.tasks added TID '{0}' with target '{1}'".format(tid, hook))
			index = len(self.tasks)
			timestamp = time() + timeout

			if (index == 0):
			#
				direct_hooks.register("dNG.pas.status.shutdown", self.stop)
				direct_hooks.register("dNG.pas.tasks.call", self.task_call)
				direct_memory.get_instance()
			#

			if (timeout > self.task_timeout):
			#
				if (index > 0):
				#
					for position in range(index - 1, -1, -1):
					#
						if (timestamp > self.tasks[position]['timestamp']):
						#
							index = position
							break
						#
					#
				#
			#
			else:
			#
				index = None

				for position in range(0, len(self.tasks)):
				#
					if (timestamp < self.tasks[position]['timestamp']):
					#
						index = position
						break
					#
				#

				if (index == None): index = len(self.tasks)
			#

			params['tid'] = tid
			self.tasks.insert(index, { "hook": hook, "params": params, "tid": tid, "timestamp": timestamp })
		#

		if (index < 1): self.update_timestamp()
	#

	def task_get(self, tid):
	#
		"""
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v0.1.00
		"""

		var_return = None

		tid = direct_binary.str(tid)

		with direct_memory.synchronized:
		#
			for position in range(0, len(self.tasks)):
			#
				if (self.tasks[position]['tid'] == tid):
				#
					var_return = self.tasks[position]
					break
				#
			#
		#

		return var_return
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

		index = 1
		tid = direct_binary.str(tid)
		if (timeout == None): timeout = self.task_timeout

		with direct_memory.synchronized:
		#
			if (self.log_handler != None): self.log_handler.debug("pas.tasks registered TID '{0}' with target '{1}'".format(tid, hook))
			index = len(self.tasks)
			timestamp = time() + timeout

			if (index == 0):
			#
				direct_hooks.register("dNG.pas.status.shutdown", self.stop)
				direct_hooks.register("dNG.pas.tasks.call", self.task_call)
				direct_memory.get_instance()
			#

			if (timeout > self.task_timeout):
			#
				if (index > 0):
				#
					for position in range(index - 1, -1, -1):
					#
						if (timestamp > self.tasks[position]['timestamp']):
						#
							index = position
							break
						#
					#
				#
			#
			else:
			#
				index = None

				for position in range(0, len(self.tasks)):
				#
					if (timestamp < self.tasks[position]['timestamp']):
					#
						index = position
						break
					#
				#

				if (index == None): index = len(self.tasks)
			#

			params['tid'] = tid
			self.tasks.insert(index, { "hook": hook, "params": params, "tid": tid, "timestamp": timestamp, "timeout": timeout })
		#

		if (index < 1): self.update_timestamp()
	#

	def timeout_reregister(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.1.00
		"""

		tid = direct_binary.str(tid)
		task = self.task_get(tid)

		if (task == None): var_return = False
		else:
		#
			self.timeout_unregister(tid)
			self.timeout_register(tid, task['hook'], task['timeout'], task['params'])

			var_return = True
		#

		return var_return
	#

	def timeout_unregister(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		index = 1
		var_return = True

		tid = direct_binary.str(tid)

		with direct_memory.synchronized:
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
						if (self.log_handler != None): self.log_handler.debug("pas.tasks removed TID '{0}'".format(tid))

						index = position
						self.tasks.pop(position)
						var_return = True

						break
					#
				#
			#

			if (len(self.tasks) == 0):
			#
				direct_hooks.unregister("dNG.pas.status.shutdown", self.stop)
				direct_hooks.unregister("dNG.pas.tasks.call", self.task_call)
				self.return_instance()
			#
		#

		if (index < 1): self.update_timestamp()
		return var_return
	#

	@staticmethod
	def get_instance(count = True):
	#
		"""
Get the control_point singleton.

:param count: Count "get()" request

:return: (direct_control_point) Object on success
:since:  v0.1.00
		"""

		with direct_memory.synchronized:
		#
			if (direct_memory.instance == None):
			#
				direct_memory.instance = direct_memory()
				direct_memory.instance.start()
			#

			if (count): direct_memory.ref_count += 1
		#

		return direct_memory.instance
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

		instance = direct_memory.get_instance(False)
		instance.timeout_register(tid, hook, timeout, params)
	#

	@staticmethod
	def reregister(tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.1.00
		"""

		instance = direct_memory.get_instance(False)
		return instance.timeout_reregister(tid)
	#

	@staticmethod
	def unregister(tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		instance = direct_memory.get_instance(False)
		return instance.timeout_unregister(tid)
	#
#

##j## EOF