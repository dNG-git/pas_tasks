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
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
----------------------------------------------------------------------------
https://www.direct-netware.de/redirect?licenses;gpl
----------------------------------------------------------------------------
#echo(pasTasksVersion)#
#echo(__FILEPATH__)#
"""

# pylint: disable=unused-argument

from dNG.pas.data.tasks.database import Database as DatabaseTasks
from dNG.pas.data.tasks.memory import Memory as MemoryTasks
from dNG.pas.plugins.hook import Hook
from dNG.pas.runtime.thread_lock import ThreadLock
from dNG.pas.runtime.value_exception import ValueException

_lock = ThreadLock()
"""
Thread safety lock
"""
_database_tasks_instance = None
"""
DatabaseTasks instance
"""
_memory_tasks_instance = None
"""
MemoryTasks instance
"""

def add_database_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.add"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	# pylint: disable=star-args

	if (last_return is not None): _return = last_return
	elif ("tid" not in params
	      or "hook" not in params
	      or "timeout" not in params
	     ): raise ValueException("Missing required arguments")
	else:
	#
		kwargs = params.get("kwargs", { })
		DatabaseTasks.get_instance().add(params['tid'], params['hook'], params['timeout'], **kwargs)
		_return = True
	#

	return _return
#

def call(params, last_return = None):
#
	"""
Called for "dNG.pas.Tasks.call"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else:
	#
		_return = MemoryTasks.get_instance().call(params)
		if (_return is None): _return = DatabaseTasks.get_instance().call(params)
	#

	return _return
#

def call_database_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.call"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("params" not in params): raise ValueException("Missing required argument")
	else:
	#
		chained_last_return = params.get("last_return")
		_return = DatabaseTasks.get_instance().call(params['params'], chained_last_return)
	#

	return _return
#

def get_database_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.get"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().get(params['tid'])

	return _return
#

def is_database_task_registered(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.isRegistered"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else:
	#
		hook = params.get("hook")
		_return = DatabaseTasks.get_instance().is_registered(params['tid'], hook)
	#

	return _return
#

def on_shutdown(params, last_return = None):
#
	"""
Called for "dNG.pas.Status.onShutdown"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:since: v0.1.00
	"""

	# global: _lock
	global _database_tasks_instance, _memory_tasks_instance

	with _lock:
	#
		if (_database_tasks_instance is not None):
		#
			_database_tasks_instance.stop()
			_database_tasks_instance = None
		#

		if (_memory_tasks_instance is not None):
		#
			_memory_tasks_instance.stop()
			_memory_tasks_instance = None
		#
	#

	return last_return
#

def on_startup(params, last_return = None):
#
	"""
Called for "dNG.pas.Status.onStartup"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:since: v0.1.00
	"""

	# global: _lock
	global _database_tasks_instance, _memory_tasks_instance

	with _lock:
	#
		if (_database_tasks_instance is None):
		#
			_database_tasks_instance = DatabaseTasks.get_instance()
			_database_tasks_instance.start()
		#

		if (_memory_tasks_instance is None):
		#
			_memory_tasks_instance = MemoryTasks.get_instance()
			_memory_tasks_instance.start()
		#
	#

	return last_return
#

def register_database_timeout_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.registerTimeout"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	# pylint: disable=star-args

	if (last_return is not None): _return = last_return
	elif ("tid" not in params
	      or "hook" not in params
	      or "timeout" not in params
	     ): raise ValueException("Missing required arguments")
	else:
	#
		kwargs = params.get("kwargs", { })
		DatabaseTasks.get_instance().register_timeout(params['tid'], params['hook'], params['timeout'], **kwargs)
		_return = True
	#

	return _return
#

def register_plugin():
#
	"""
Register plugin hooks.

:since: v0.1.00
	"""

	Hook.register("dNG.pas.Status.onShutdown", on_shutdown)
	Hook.register("dNG.pas.Status.onStartup", on_startup)
	Hook.register("dNG.pas.Tasks.call", call)
	Hook.register("dNG.pas.tasks.Database.isRegistered", is_database_task_registered)
	Hook.register("dNG.pas.tasks.Database.add", add_database_task)
	Hook.register("dNG.pas.tasks.Database.call", call_database_task)
	Hook.register("dNG.pas.tasks.Database.get", get_database_task)
	Hook.register("dNG.pas.tasks.Database.registerTimeout", register_database_timeout_task)
	Hook.register("dNG.pas.tasks.Database.remove", remove_database_task)
	Hook.register("dNG.pas.tasks.Database.reregisterTimeout", reregister_database_timeout_task)
	Hook.register("dNG.pas.tasks.Database.unregisterTimeout", unregister_database_timeout_task)
#

def remove_database_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.remove"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().remove(params['tid'])

	return _return
#

def reregister_database_timeout_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.reregisterTimeout"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().reregister_timeout(params['tid'])

	return _return
#

def unregister_database_timeout_task(params, last_return = None):
#
	"""
Called for "dNG.pas.tasks.Database.unregisterTimeout"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return is not None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().unregister_timeout(params['tid'])

	return _return
#

def unregister_plugin():
#
	"""
Unregister plugin hooks.

:since: v0.1.00
	"""

	Hook.unregister("dNG.pas.Status.onShutdown", on_shutdown)
	Hook.unregister("dNG.pas.Status.onStartup", on_startup)
	Hook.unregister("dNG.pas.Tasks.call", call)
	Hook.unregister("dNG.pas.tasks.Database.isRegistered", is_database_task_registered)
	Hook.unregister("dNG.pas.tasks.Database.add", add_database_task)
	Hook.unregister("dNG.pas.tasks.Database.call", call_database_task)
	Hook.unregister("dNG.pas.tasks.Database.get", get_database_task)
	Hook.unregister("dNG.pas.tasks.Database.registerTimeout", register_database_timeout_task)
	Hook.unregister("dNG.pas.tasks.Database.remove", remove_database_task)
	Hook.unregister("dNG.pas.tasks.Database.reregisterTimeout", reregister_database_timeout_task)
	Hook.unregister("dNG.pas.tasks.Database.unregisterTimeout", unregister_database_timeout_task)
#

##j## EOF