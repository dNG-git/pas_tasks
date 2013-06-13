# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.store.Abstract
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

from dNG.pas.data.abstract_timed_tasks import AbstractTimedTasks
from dNG.pas.data.settings import Settings
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.plugins.hooks import Hooks

class Abstract(AbstractTimedTasks):
#
	"""
Abstract class for task stores.

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
Constructor __init__(Abstract)

:since: v0.1.00
		"""

		AbstractTimedTasks.__init__(self)

		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)
		"""
The log_handler is called whenever debug messages should be logged or errors
happened.
		"""
		self.task_timeout = int(Settings.get("pas_tasks_timeout", 900))
		"""
Default timeout for an activated task
		"""
	#

	def __del__(self):
	#
		"""
Destructor __del__(Abstract)

:since: v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.return_instance()
	#

	def return_instance(self):
	#
		"""
The last "return_instance()" call will free the singleton reference.

:since: v0.1.00
		"""

		with Abstract.synchronized:
		#
			if (Abstract.instance != None):
			#
				if (Abstract.ref_count > 0): Abstract.ref_count -= 1

				if (Abstract.ref_count == 0):
				#
					self.stop()
					Abstract.instance = None
				#
			#
		#
	#

	def task_call(self, params = None, last_return = None):
	#
		"""
Called to initiate a task if its known and valid.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v0.1.00
		"""

		var_return = last_return

		if (var_return == None):
		#
			task = self.task_get(params['tid'])

			if (task == None): is_valid = False
			else: is_valid = (True if ("client" not in task['params'] or ("client" in params and params['client'] == task['params']['client'])) else False)

			if (is_valid):
			#
				if ("timeout" in task): self.timeout_reregister(params['tid'])
				var_return = Hooks.call(task['hook'], **task['params'])
			#
		#

		return var_return
	#

	def task_get(self, tid):
	#
		"""
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v0.1.00
		"""

		raise RuntimeError("Not implemented", 38)
	#

	def timeout_register(self, tid, hook, timeout = None, **params):
	#
		"""
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v0.1.00
		"""

		raise RuntimeError("Not implemented", 38)
	#

	def timeout_reregister(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.1.00
		"""

		raise RuntimeError("Not implemented", 38)
	#

	def timeout_unregister(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		raise RuntimeError("Not implemented", 38)
	#
#

##j## EOF