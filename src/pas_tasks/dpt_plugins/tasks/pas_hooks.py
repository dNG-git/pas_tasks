# -*- coding: utf-8 -*-

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

# pylint: disable=unused-argument

from dpt_plugins import Hook
from dpt_runtime.value_exception import ValueException
from dpt_threading.thread_lock import ThreadLock

from ...memory import Memory as MemoryTasks
from ...persistent import Persistent as PersistentTasks

_lock = ThreadLock()
"""
Thread safety lock
"""
_memory_tasks_instance = None
"""
MemoryTasks instance
"""
_persistent_tasks_instance = None
"""
PersistentTasks instance
"""

def add_persistent_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.add"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    # pylint: disable=star-args

    if (last_return is not None): _return = last_return
    elif ("tid" not in params
          or "hook" not in params
          or "timeout" not in params
         ): raise ValueException("Missing required arguments")
    else:
        kwargs = params.get("kwargs", { })
        PersistentTasks.get_instance().add(params['tid'], params['hook'], params['timeout'], **kwargs)
        _return = True
    #

    return _return
#

def call(params, last_return = None):
    """
Called for "pas.Tasks.call"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("tid" not in params): raise ValueException("Missing required argument")
    else:
        memory_tasks = MemoryTasks.get_instance()

        _return = (memory_tasks.call(params)
                   if (memory_tasks.is_registered(params['tid'])) else
                   PersistentTasks.get_instance().call(params)
                  )
    #

    return _return
#

def call_persistent_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.call"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("params" not in params): raise ValueException("Missing required argument")
    else:
        chained_last_return = params.get("last_return")
        _return = PersistentTasks.get_instance().call(params['params'], chained_last_return)
    #

    return _return
#

def get_persistent_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.get"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("tid" not in params): raise ValueException("Missing required argument")
    else: _return = PersistentTasks.get_instance().get(params['tid'])

    return _return
#

def is_persistent_task_registered(params, last_return = None):
    """
Called for "pas.tasks.Persistent.isRegistered"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("tid" not in params): raise ValueException("Missing required argument")
    else:
        hook = params.get("hook")
        _return = PersistentTasks.get_instance().is_registered(params['tid'], hook)
    #

    return _return
#

def on_shutdown(params, last_return = None):
    """
Called for "pas.Application.onShutdown"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:since: v1.0.0
    """

    # global: _lock
    global _memory_tasks_instance, _persistent_tasks_instance

    with _lock:
        if (_persistent_tasks_instance is not None):
            _persistent_tasks_instance.stop()
            _persistent_tasks_instance = None
        #

        if (_memory_tasks_instance is not None):
            _memory_tasks_instance.stop()
            _memory_tasks_instance = None
        #
    #

    return last_return
#

def on_startup(params, last_return = None):
    """
Called for "pas.Application.onStartup"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:since: v1.0.0
    """

    # global: _lock
    global _memory_tasks_instance, _persistent_tasks_instance

    with _lock:
        if (_memory_tasks_instance is None):
            _memory_tasks_instance = MemoryTasks.get_instance()
            _memory_tasks_instance.start()
        #

        if (_persistent_tasks_instance is None):
            _persistent_tasks_instance = PersistentTasks.get_executing_instance()
            _persistent_tasks_instance.start()
        #
    #

    return last_return
#

def register_persistent_timeout_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.registerTimeout"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    # pylint: disable=star-args

    if (last_return is not None): _return = last_return
    elif ("tid" not in params
          or "hook" not in params
          or "timeout" not in params
         ): raise ValueException("Missing required arguments")
    else:
        kwargs = params.get("kwargs", { })
        PersistentTasks.get_instance().register_timeout(params['tid'], params['hook'], params['timeout'], **kwargs)
        _return = True
    #

    return _return
#

def register_plugin():
    """
Register plugin hooks.

:since: v1.0.0
    """

    Hook.register("pas.Application.onShutdown", on_shutdown)
    Hook.register("pas.Application.onStartup", on_startup)
    Hook.register("pas.Tasks.call", call)
    Hook.register("pas.tasks.Persistent.isRegistered", is_persistent_task_registered)
    Hook.register("pas.tasks.Persistent.add", add_persistent_task)
    Hook.register("pas.tasks.Persistent.call", call_persistent_task)
    Hook.register("pas.tasks.Persistent.get", get_persistent_task)
    Hook.register("pas.tasks.Persistent.registerTimeout", register_persistent_timeout_task)
    Hook.register("pas.tasks.Persistent.remove", remove_persistent_task)
    Hook.register("pas.tasks.Persistent.reregisterTimeout", reregister_persistent_timeout_task)
    Hook.register("pas.tasks.Persistent.unregisterTimeout", unregister_persistent_timeout_task)
#

def remove_persistent_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.remove"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("tid" not in params): raise ValueException("Missing required argument")
    else: _return = PersistentTasks.get_instance().remove(params['tid'])

    return _return
#

def reregister_persistent_timeout_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.reregisterTimeout"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("tid" not in params): raise ValueException("Missing required argument")
    else: _return = PersistentTasks.get_instance().reregister_timeout(params['tid'])

    return _return
#

def unregister_persistent_timeout_task(params, last_return = None):
    """
Called for "pas.tasks.Persistent.unregisterTimeout"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (last_return is not None): _return = last_return
    elif ("tid" not in params): raise ValueException("Missing required argument")
    else: _return = PersistentTasks.get_instance().unregister_timeout(params['tid'])

    return _return
#

def unregister_plugin():
    """
Unregister plugin hooks.

:since: v1.0.0
    """

    Hook.unregister("pas.Application.onShutdown", on_shutdown)
    Hook.unregister("pas.Application.onStartup", on_startup)
    Hook.unregister("pas.Tasks.call", call)
    Hook.unregister("pas.tasks.Persistent.isRegistered", is_persistent_task_registered)
    Hook.unregister("pas.tasks.Persistent.add", add_persistent_task)
    Hook.unregister("pas.tasks.Persistent.call", call_persistent_task)
    Hook.unregister("pas.tasks.Persistent.get", get_persistent_task)
    Hook.unregister("pas.tasks.Persistent.registerTimeout", register_persistent_timeout_task)
    Hook.unregister("pas.tasks.Persistent.remove", remove_persistent_task)
    Hook.unregister("pas.tasks.Persistent.reregisterTimeout", reregister_persistent_timeout_task)
    Hook.unregister("pas.tasks.Persistent.unregisterTimeout", unregister_persistent_timeout_task)
#
