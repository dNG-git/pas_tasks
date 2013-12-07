# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.tasks.AbstractLrtHook
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

from threading import Thread

try: from queue import Queue
except ImportError: from Queue import Queue

from dNG.pas.data.settings import Settings
from dNG.pas.data.traced_exception import TracedException
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.runtime.thread_lock import ThreadLock
from .abstract import Abstract

class AbstractLrtHook(Abstract):
#
	"""
Long running tasks (LRT) are resource intensive calls where only a limited
amount should run parallel.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	context_limit = 0
	"""
Limit for allowed context executions
	"""
	context_queues = { }
	"""
Currently active context queues
	"""
	lock = ThreadLock()
	"""
Thread safety lock
	"""

	def __init__(self):
	#
		"""
Constructor __init__(AbstractLrtHook)

:since: v0.1.00
		"""

		Abstract.__init__(self)

		self.context_id = "dNG.pas.tasks.Context"
		"""
Default timeout for an activated task
		"""
		self.independent_scheduling = False
		"""
Usually queues are filled and executed as long as new tasks of the same type
arrive. Set this variable to true to reschedule these tasks independently.
		"""
		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)
		"""
The LogHandler is called whenever debug messages should be logged or errors
happened.
		"""
		self.params = None
		"""
Task parameters
		"""
		self.tid = None
		"""
Task ID
		"""
	#

	def __enter__(self):
	#
		"""
python.org: Enter the runtime context related to this object.

:since: v0.1.00
		"""

		pass
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:since: v0.1.00
		"""

		with AbstractLrtHook.lock:
		#
			if (self.context_id in AbstractLrtHook.context_queues):
			#
				if (self._task_get() != None): self.log_handler.error("LRT process found queue elements after execution")
				del(AbstractLrtHook.context_queues[self.context_id])
			#
		#
	#

	def run(self, task_store, tid, **params):
	#
		"""
Starts the execution of this hook synchronously.

:return: (mixed) Task result
:since:  v0.1.00
		"""

		raise TracedException("Not implemented")
	#

	def _run(self):
	#
		"""
Returns the manager instance responsible for this hook.

:return: (object) Hook manager on success
:since:  v0.1.00
		"""

		with self:
		#
			with AbstractLrtHook.lock: task = self._task_get()

			while (task != None):
			#
				try: task._run_hook()
				except Exception as handled_exception:
				#
					if (self.log_handler != None): self.log_handler.error(handled_exception)
				#

				with AbstractLrtHook.lock:
				#
					AbstractLrtHook.context_queues[self.context_id].task_done()
					task = self._task_get()
					if (task == None): del(AbstractLrtHook.context_queues[self.context_id])
				#
			#
		#
	#

	def _run_hook(self):
	#
		"""
Hook execution

:return: (mixed) Task result
:since:  v0.1.00
		"""

		raise TracedException("Not implemented")
	#

	def _task_get(self):
	#
		"""
Returns the next task from the context queue if any.

:return: (mixed) Task from the same context queue
:since:  v0.1.00
		"""

		return (None if (AbstractLrtHook.context_queues[self.context_id].empty()) else AbstractLrtHook.context_queues[self.context_id].get(False))
	#

	def start(self, task_store, tid, **params):
	#
		"""
Starts the execution of this hook asynchronously.

:since: v0.1.00
		"""

		is_queued = False

		with AbstractLrtHook.lock:
		#
			if (AbstractLrtHook.context_limit < 1): AbstractLrtHook.context_limit = Settings.get("pas_global_tasks_lrt_limit", 1)
			if (self.params == None): self.params = params

			if (self.context_id in AbstractLrtHook.context_queues):
			#
				if (not self.independent_scheduling):
				#
					AbstractLrtHook.context_queues[self.context_id].put(self, False)
					is_queued = True
				#
			#
			elif (len(AbstractLrtHook.context_queues) < AbstractLrtHook.context_limit):
			#
				AbstractLrtHook.context_queues[self.context_id] = Queue()
				AbstractLrtHook.context_queues[self.context_id].put(self, False)

				thread = Thread(target = self._run)
				thread.start()

				is_queued = True
			#
		#

		if (not is_queued): task_store.task_add(tid, self, 10) # TODO: Setting or constant please ;)
	#
#

##j## EOF