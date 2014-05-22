# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.DatabaseProxy
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

from weakref import ref

from dNG.pas.data.settings import Settings
from dNG.pas.net.bus.client import Client
from dNG.pas.plugins.hook import Hook
from dNG.pas.runtime.instance_lock import InstanceLock
from dNG.pas.runtime.operation_not_supported_exception import OperationNotSupportedException
from .abstract import Abstract
from .database import Database

class DatabaseProxy(Abstract):
#
	"""
The task proxy forwards tasks to a background daemon executing it.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	_lock = InstanceLock()
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
Constructor __init__(DatabaseProxy)

:since: v0.1.00
		"""

		Abstract.__init__(self)

		self.client = None
		"""
IPC bus client
		"""

		Settings.read_file("{0}/settings/pas_tasks_daemon.json".format(Settings.get("path_data")))
	#

	def __del__(self):
	#
		"""
Destructor __del__(Client)

:since: v0.1.00
		"""

		self.disconnect()
	#

	def connect(self):
	#
		"""
Opens a bus connection.

:since: v0.1.00
		"""

		if (self.client == None): self.client = Client("pas_tasks_daemon")
	#

	def disconnect(self):
	#
		"""
Closes an active bus connection.

:since: v0.1.00
		"""

		if (self.client != None): self.client.disconnect()
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

		self.connect()
		self.client.request("dNG.pas.tasks.Database.add", tid = tid, hook = hook, timeout = timeout, kwargs = kwargs)
	#

	def call(self, params = None, last_return = None):
	#
		"""
Called to initiate a task if its known and valid.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v0.1.00
		"""

		self.connect()
		return self.client.request("dNG.pas.tasks.Database.call", params = params, last_return = last_return)
	#

	def get(self, tid):
	#
		"""
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v0.1.00
		"""

		raise OperationNotSupportedException()
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

		self.connect()
		return (True if (self.client.request("dNG.pas.tasks.Database.isRegistered", tid = tid, hook = hook) == True) else False)
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

		self.connect()
		self.client.request("dNG.pas.tasks.Database.registerTimeout", tid = tid, hook = hook, timeout = timeout, kwargs = kwargs)
	#

	def remove(self, tid):
	#
		"""
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.1.00
		"""

		self.connect()
		return self.client.request("dNG.pas.tasks.Database.remove", tid = tid)
	#

	def reregister_timeout(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.1.00
		"""

		self.connect()
		return self.client.request("dNG.pas.tasks.Database.reregisterTimeout", tid = tid)
	#

	def unregister_timeout(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.1.00
		"""

		self.connect()
		return self.client.request("dNG.pas.tasks.Database.unregisterTimeout", tid = tid)
	#

	@staticmethod
	def get_instance():
	#
		"""
Get a database tasks instance.

:return: (object) Database tasks instance on success
:since:  v0.1.00
		"""

		database_tasks = Database.get_instance()
		return (database_tasks if (database_tasks.is_started()) else DatabaseProxy._get_instance())
	#

	@staticmethod
	def _get_instance():
	#
		"""
Get the DatabaseProxy singleton.

:return: (DatabaseProxy) Object on success
:since:  v0.1.00
		"""

		_return = None

		with DatabaseProxy._lock:
		#
			if (DatabaseProxy._weakref_instance == None): Hook.load("tasks")
			else: _return = DatabaseProxy._weakref_instance()

			if (_return == None):
			#
				_return = DatabaseProxy()
				DatabaseProxy._weakref_instance = ref(_return)
			#
		#

		return _return
	#

	@staticmethod
	def is_available():
	#
		"""
True if a database tasks instance is available.

:return: (bool) True if available
:since:  v0.1.00
		"""

		_return = Database.get_instance().is_started()

		if (not _return):
		#
			settings_filepath = "{0}/settings/pas_tasks_daemon.json".format(Settings.get("path_data"))
			if (not Settings.is_file_known(settings_filepath)): Settings.read_file(settings_filepath)
			_return = Settings.is_defined("pas_tasks_daemon_listener_address")
		#

		return _return
	#
#

##j## EOF