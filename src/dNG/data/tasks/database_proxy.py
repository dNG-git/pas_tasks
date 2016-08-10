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

from weakref import ref

from dNG.data.binary import Binary
from dNG.data.settings import Settings
from dNG.net.bus.client import Client
from dNG.runtime.instance_lock import InstanceLock
from dNG.runtime.operation_not_supported_exception import OperationNotSupportedException
from dNG.runtime.type_exception import TypeException
from dNG.tasks.database_lrt_hook import DatabaseLrtHook

from .abstract import Abstract
from .database import Database

class DatabaseProxy(Abstract):
#
	"""
The task proxy forwards tasks to a background daemon executing it.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;gpl
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

:since: v0.2.00
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
Destructor __del__(DatabaseProxy)

:since: v0.2.00
		"""

		self.disconnect()
	#

	def connect(self):
	#
		"""
Opens a bus connection.

:since: v0.2.00
		"""

		if (self.client is None): self.client = Client("pas_tasks_daemon")
	#

	def disconnect(self):
	#
		"""
Closes an active bus connection.

:since: v0.2.00
		"""

		if (self.client is not None): self.client.disconnect()
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

		params = self._get_hook_proxy_params(hook, timeout, kwargs)

		self.connect()
		self.client.request("dNG.pas.tasks.Database.add", tid = tid, **params)
	#

	def call(self, params = None, last_return = None):
	#
		"""
Called to initiate a task if its known and valid.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v0.2.00
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
:since:  v0.2.00
		"""

		raise OperationNotSupportedException()
	#

	def _get_hook_proxy_params(self, hook, timeout = None, kwargs = None):
	#
		"""
Returns serializable parameters for the database proxy.

:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout
:param kwargs: Keyword arguments

:return: (dict)
:since:  v0.2.00
		"""

		_return = { }

		if (isinstance(hook, DatabaseLrtHook)):
		#
			_return['hook'] = hook.get_hook()

			_return['kwargs'] = ({ } if (kwargs is None) else kwargs)
			_return['kwargs'].update(hook.get_params())
			_return['kwargs']['_lrt_hook'] = True
		#
		else:
		#
			hook = Binary.str(hook)
			if (type(hook) is not str): raise TypeException("Hook given is invalid")

			_return['hook'] = hook
			if (kwargs is not None): _return['kwargs'] = kwargs
		#

		if (timeout is not None): _return['timeout'] = timeout

		return _return
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

		params = self._get_hook_proxy_params(hook)

		self.connect()
		return (True if (self.client.request("dNG.pas.tasks.Database.isRegistered", tid = tid, **params) == True) else False)
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

		params = self._get_hook_proxy_params(hook, timeout, kwargs)

		self.connect()
		self.client.request("dNG.pas.tasks.Database.registerTimeout", tid = tid, **params)
	#

	def remove(self, tid):
	#
		"""
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v0.2.00
		"""

		self.connect()
		return self.client.request("dNG.pas.tasks.Database.remove", tid = tid)
	#

	def reregister_timeout(self, tid):
	#
		"""
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v0.2.00
		"""

		self.connect()
		return self.client.request("dNG.pas.tasks.Database.reregisterTimeout", tid = tid)
	#

	def unregister_timeout(self, tid):
	#
		"""
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v0.2.00
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
:since:  v0.2.00
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
:since:  v0.2.00
		"""

		_return = None

		with DatabaseProxy._lock:
		#
			if (DatabaseProxy._weakref_instance is not None): _return = DatabaseProxy._weakref_instance()

			if (_return is None):
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
:since:  v0.2.00
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