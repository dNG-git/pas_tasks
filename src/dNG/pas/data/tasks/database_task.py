# -*- coding: utf-8 -*-
##j## BOF

"""
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
"""

from random import randrange
from time import time

from sqlalchemy.sql.expression import and_, or_

from dNG.data.json_resource import JsonResource
from dNG.pas.data.binary import Binary
from dNG.pas.data.settings import Settings
from dNG.pas.data.text.md5 import Md5
from dNG.pas.database.connection import Connection
from dNG.pas.database.instance import Instance
from dNG.pas.database.nothing_matched_exception import NothingMatchedException
from dNG.pas.database.instances.task import Task as _DbTask
from dNG.pas.runtime.io_exception import IOException
from dNG.pas.runtime.type_exception import TypeException

class DatabaseTask(Instance):
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

	STATUS_COMPLETED = 32
	"""
Task has been completed
	"""
	STATUS_FAILED = 64
	"""
Task execution failed
	"""
	STATUS_RUNNING = 128
	"""
Task is currently being executed
	"""
	STATUS_QUEUED = 112
	"""
Task is queued for execution
	"""
	STATUS_UNKNOWN = 0
	"""
Task status is unknown
	"""
	STATUS_WAITING = 96
	"""
Task waits for execution
	"""

	def __init__(self, db_instance = None):
	#
		"""
Constructor __init__(Database)

:param db_instance: Encapsulated SQLAlchemy database instance

:since: v0.1.00
		"""

		Instance.__init__(self, db_instance)

		self.db_id = None
		"""
Database ID used for reloading
		"""
		self.hook = ""
		"""
Task hook to be called
		"""
		self.params = None
		"""
Task parameter specified
		"""
		self.tid = None
		"""
Task ID
		"""

		if (db_instance != None):
		#
			with self:
			#
				self.db_id = self.local.db_instance.id
				self.hook = self.local.db_instance.hook
				if (self.local.db_instance.params != ""): self.params = JsonResource().json_to_data(self.local.db_instance.params)

				if ("_tid" in self.params): self.tid = self.params['_tid']
			#
		#

		if (self.params == None): self.params = { }
	#

	def get_hook(self):
	#
		"""
Returns the task hook to be called.

:return: (str) Task hook
:since:  v0.1.00
		"""

		return self.hook
	#

	def get_params(self):
	#
		"""
Returns the task parameter used.

:return: (dict) Task parameter
:since:  v0.1.00
		"""

		return self.params
	#

	get_status = Instance._wrap_getter("status")
	"""
Returns the task status.

:return: (int) Task status
:since:  v0.1.00
	"""

	def get_tid(self):
	#
		"""
Returns the task ID.

:return: (str) Task ID
:since:  v0.1.00
		"""

		return self.tid
	#

	get_time_started = Instance._wrap_getter("time_started")
	"""
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v0.1.00
	"""

	get_time_scheduled = Instance._wrap_getter("time_scheduled")
	"""
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v0.1.00
	"""

	get_time_updated = Instance._wrap_getter("time_updated")
	"""
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v0.1.00
	"""

	get_timeout = Instance._wrap_getter("timeout")
	"""
Returns the UNIX timestamp this task will time out.

:return: (int) UNIX timestamp
:since:  v0.1.00
	"""

	def is_reloadable(self):
	#
		"""
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v0.1.00
		"""

		_return = True

		if (self.db_id == None):
		# Thread safety
			with self._lock: _return = (self.db_id != None)
		#

		return _return
	#

	def is_timed_out(self):
	#
		"""
Returns true if the task timed out.

:return: (bool) True if timed out
:sinve:  v0.1.00
		"""

		timeout = self.get_timeout()
		return (timeout > 0 and timeout < int(time()))
	#

	def is_timeout_set(self):
	#
		"""
Returns true if the task has a timeout value.

:return: (bool) True if timeout set
:sinve:  v0.1.00
		"""

		return (self.get_timeout() > 0)
	#

	def _reload(self):
	#
		"""
Implementation of the reloading SQLAlchemy database instance logic.

:since: v0.1.00
		"""

		with self._lock:
		#
			if (self.local.db_instance == None):
			#
				if (self.db_id == None): raise IOException("Database instance is not reloadable.")
				self.local.db_instance = self._database.query(_DbTask).filter(_DbTask.id == self.db_id).one()
			#
			else: Instance._reload(self)
		#
	#

	def save(self):
	#
		"""
Saves changes of the database task instance.

:since: v0.1.00
		"""

		with self:
		#
			self.local.db_instance.tid = Binary.utf8(Md5.hash(self.tid))
			self.params['_tid'] = self.tid

			if (self.local.db_instance.name == ""): self.local.db_instance.name = Binary.utf8(self.hook[-100:])
			if (self.local.db_instance.status == None): self.local.db_instance.status = DatabaseTask.STATUS_WAITING
			self.local.db_instance.hook = Binary.utf8(self.hook)
			self.local.db_instance.params = Binary.utf8(JsonResource().data_to_json(self.params))
			self.local.db_instance.time_updated = int(time())

			Instance.save(self)
		#
	#

	def set_data_attributes(self, **kwargs):
	#
		"""
Sets values given as keyword arguments to this method.

:since: v0.1.00
		"""

		self._ensure_thread_local_instance(_DbTask)

		with self:
		#
			if (self.db_id == None): self.db_id = self.local.db_instance.id

			if ("tid" in kwargs): self.tid = kwargs['tid']
			if ("name" in kwargs): self.local.db_instance.name = Binary.utf8(kwargs['name'][-100:])
			if ("status" in kwargs): self.local.db_instance.status = kwargs['status']

			if ("hook" in kwargs): self.hook = kwargs['hook']
			if ("params" in kwargs and isinstance(kwargs['params'], dict)): self.params = kwargs['params']

			if ("time_started" in kwargs): self.local.db_instance.time_started = int(kwargs['time_started'])
			if ("time_scheduled" in kwargs): self.local.db_instance.time_scheduled = int(kwargs['time_scheduled'])
			if ("time_updated" in kwargs): self.local.db_instance.time_updated = int(kwargs['time_updated'])
			if ("timeout" in kwargs): self.local.db_instance.timeout = int(kwargs['timeout'])
		#
	#

	def set_hook(self, hook):
	#
		"""
Sets the task hook to be called.

:param hook: Task hook

:since: v0.1.00
		"""

		self.hook = hook
	#

	def set_name(self, name):
	#
		"""
Sets the task name.

:param name: Task name

:since: v0.1.00
		"""

		self.set_data_attributes(name = name)
	#
	def set_params(self, params):
	#
		"""
Sets the task parameter.

:param params: Task parameter

:since: v0.1.00
		"""

		if (not isinstance(params, dict)): raise TypeException("Parameter given are invalid")
		self.params = params
	#

	def set_status(self, status):
	#
		"""
Sets the task status.

:param status: Task status

:since: v0.1.00
		"""

		self.set_data_attributes(status = status)
	#

	def set_status_completed(self):
	#
		"""
Sets the task status to "completed".

:since: v0.1.00
		"""

		self.set_data_attributes(status = DatabaseTask.STATUS_COMPLETED)
	#

	def set_tid(self, tid):
	#
		"""
Sets the task ID.

:param _id: Task ID

:since: v0.1.00
		"""

		self.tid = tid
	#

	def set_time_scheduled(self, timestamp):
	#
		"""
Sets the time the task is scheduled to be executed.

:param timestamp: UNIX timestamp

:since: v0.1.00
		"""

		self.set_data_attributes(time_scheduled = timestamp)
	#

	def set_timeout(self, timestamp):
	#
		"""
Sets a timeout for the task.

:param timestamp: UNIX timestamp

:since: v0.1.00
		"""

		self.set_data_attributes(timeout = timestamp)
	#

	@staticmethod
	def _load(db_instance):
	#
		"""
Load DatabaseTask entry from database.

:param db_instance: SQLAlchemy database instance

:return: (object) DatabaseTask instance on success
:since:  v0.1.00
		"""

		_return = None

		if (db_instance != None):
		#
			with Connection.get_instance() as database:
			#
				if ((not Settings.get("pas_database_auto_maintenance", False)) and randrange(0, 3) < 1):
				#
					archive_timeout = int(Settings.get("pas_tasks_database_tasks_archive_timeout", 28)) * 86400
					timestamp = int(time())
					timestamp_archive = timestamp - archive_timeout

					if (database.query(_DbTask)
					    .filter(or_(and_(_DbTask.status == DatabaseTask.STATUS_COMPLETED,
					                     _DbTask.time_scheduled > 0,
					                     _DbTask.time_scheduled < timestamp_archive
					                    ),
					                and_(_DbTask.timeout > 0, _DbTask.timeout < timestamp)
					               )
					           )
					    .delete() > 0
					   ): database.optimize_random(_DbTask)
				#

				_return = DatabaseTask(db_instance)
				if (_return.is_timed_out()): _return = None
			#
		#

		if (_return == None): raise NothingMatchedException("Task not found")
		return _return
	#

	@staticmethod
	def load_id(_id):
	#
		"""
Load KeyStore value by ID.

:param _id: KeyStore ID

:return: (object) KeyStore instance on success
:since:  v0.1.00
		"""

		with Connection.get_instance() as database: return DatabaseTask._load(database.query(_DbTask).get(_id))
	#

	@staticmethod
	def load_next(status = None):
	#
		"""
Load KeyStore value by key.

:param status: Task status

:return: (object) KeyStore instance on success
:since:  v0.1.00
		"""

		if (status == None): status = DatabaseTask.STATUS_WAITING

		with Connection.get_instance() as database:
		#
			return DatabaseTask._load(database.query(_DbTask)
			                          .filter(_DbTask.status == status,
			                                  or_(_DbTask.timeout == 0,
			                                      _DbTask.timeout <= int(time())
			                                     )
			                                 )
			                          .order_by(_DbTask.time_scheduled.asc()).first()
			                         )
		#
	#

	@staticmethod
	def load_tid(tid):
	#
		"""
Load KeyStore value by ID.

:param tid: KeyStore ID

:return: (object) KeyStore instance on success
:since:  v0.1.00
		"""

		with Connection.get_instance() as database: return DatabaseTask._load(database.query(_DbTask).filter(_DbTask.tid == Md5.hash(tid)).limit(1).first())
	#

	@staticmethod
	def _reset_stale_running():
	#
		"""
Resets all stale tasks with the "running" status.

:since:  v0.1.00
		"""

		with Connection.get_instance() as database:
		#
			database.query(_DbTask).filter(_DbTask.status == DatabaseTask.STATUS_RUNNING).update({ "status": DatabaseTask.STATUS_WAITING })
		#
	#
#

##j## EOF