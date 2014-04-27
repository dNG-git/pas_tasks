# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.plugins.tasks.pas_tasks
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

# pylint: disable=unused-argument

from dNG.pas.data.tasks.database import Database as DatabaseTasks
from dNG.pas.data.tasks.memory import Memory as MemoryTasks
from dNG.pas.plugins.hooks import Hooks
from dNG.pas.runtime.value_exception import ValueException

def plugin_database_is_registered(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.isRegistered"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else:
	#
		hook = (params['hook'] if ("hook" in params) else None)
		_return = DatabaseTasks.get_instance().is_registered(params['tid'], hook)
	#

	return _return
#

def plugin_database_task_add(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.taskAdd"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	# pylint: disable=star-args

	if (last_return != None): _return = last_return
	elif (
		"tid" not in params or
		"hook" not in params or
		"timeout" not in params
	): raise ValueException("Missing required arguments")
	else:
	#
		kwargs = (params['kwargs'] if ("kwargs" in params) else { })
		DatabaseTasks.get_instance().task_add(params['tid'], params['hook'], params['timeout'], **kwargs)
		_return = True
	#

	return _return
#

def plugin_database_task_call(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.taskCall"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("params" not in params): raise ValueException("Missing required argument")
	else:
	#
		chained_last_return = (params['last_return'] if ("last_return" in params) else None)
		_return = DatabaseTasks.get_instance().task_call(params['params'], chained_last_return)
	#

	return _return
#

def plugin_database_task_get(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.taskGet"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().task_get(params['tid'])

	return _return
#

def plugin_database_task_remove(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.taskRemove"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().task_remove(params['tid'])

	return _return
#

def plugin_database_timeout_register(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.timeoutRegister"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	# pylint: disable=star-args

	if (last_return != None): _return = last_return
	elif (
		"tid" not in params or
		"hook" not in params or
		"timeout" not in params
	): raise ValueException("Missing required arguments")
	else:
	#
		kwargs = (params['kwargs'] if ("kwargs" in params) else { })
		DatabaseTasks.get_instance().timeout_register(params['tid'], params['hook'], params['timeout'], **kwargs)
		_return = True
	#

	return _return
#

def plugin_database_timeout_reregister(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.timeoutReregister"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().timeout_reregister(params['tid'])

	return _return
#

def plugin_database_timeout_unregister(params, last_return):
#
	"""
Called for "dNG.pas.tasks.Database.timeoutUnregister"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else: _return = DatabaseTasks.get_instance().timeout_unregister(params['tid'])

	return _return
#

def plugin_tasks_call(params, last_return):
#
	"""
Called for "dNG.pas.Tasks.call"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v0.1.00
	"""

	if (last_return != None): _return = last_return
	elif ("tid" not in params): raise ValueException("Missing required argument")
	else:
	#
		_return = MemoryTasks.get_instance().task_call(params)
		if (_return == None): _return = DatabaseTasks.get_instance().task_call(params)
	#

	return _return
#

def plugin_deregistration():
#
	"""
Unregister plugin hooks.

:since: v0.1.00
	"""

	Hooks.unregister("dNG.pas.Tasks.call", plugin_tasks_call)
	Hooks.unregister("dNG.pas.tasks.Database.isRegistered", plugin_database_is_registered)
	Hooks.unregister("dNG.pas.tasks.Database.taskAdd", plugin_database_task_add)
	Hooks.unregister("dNG.pas.tasks.Database.taskCall", plugin_database_task_call)
	Hooks.unregister("dNG.pas.tasks.Database.taskGet", plugin_database_task_get)
	Hooks.unregister("dNG.pas.tasks.Database.taskRemove", plugin_database_task_remove)
	Hooks.unregister("dNG.pas.tasks.Database.timeoutRegister", plugin_database_timeout_register)
	Hooks.unregister("dNG.pas.tasks.Database.timeoutReregister", plugin_database_timeout_reregister)
	Hooks.unregister("dNG.pas.tasks.Database.timeoutUnregister", plugin_database_timeout_unregister)
#

def plugin_registration():
#
	"""
Register plugin hooks.

:since: v0.1.00
	"""

	Hooks.register("dNG.pas.Tasks.call", plugin_tasks_call)
	Hooks.register("dNG.pas.tasks.Database.isRegistered", plugin_database_is_registered)
	Hooks.register("dNG.pas.tasks.Database.taskAdd", plugin_database_task_add)
	Hooks.register("dNG.pas.tasks.Database.taskCall", plugin_database_task_call)
	Hooks.register("dNG.pas.tasks.Database.taskGet", plugin_database_task_get)
	Hooks.register("dNG.pas.tasks.Database.taskRemove", plugin_database_task_remove)
	Hooks.register("dNG.pas.tasks.Database.timeoutRegister", plugin_database_timeout_register)
	Hooks.register("dNG.pas.tasks.Database.timeoutReregister", plugin_database_timeout_reregister)
	Hooks.register("dNG.pas.tasks.Database.timeoutUnregister", plugin_database_timeout_unregister)
#

##j## EOF