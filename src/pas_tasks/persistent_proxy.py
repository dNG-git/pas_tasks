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

from weakref import ref

from dpt_runtime.binary import Binary
from dpt_runtime.operation_not_supported_exception import OperationNotSupportedException
from dpt_runtime.type_exception import TypeException
from dpt_settings import Settings
from dpt_threading.instance_lock import InstanceLock
from pas_bus import Client as BusClient

from .abstract_persistent_proxy import AbstractPersistentProxy
from .tasks import PersistentLrtHook

class PersistentProxy(AbstractPersistentProxy):
    """
The task proxy forwards tasks to a background daemon executing it.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    __slots__ = [ "client" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
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
        """
Constructor __init__(PersistentProxy)

:since: v1.0.0
        """

        AbstractPersistentProxy.__init__(self)

        self.client = None
        """
IPC bus client
        """
    #

    def __del__(self):
        """
Destructor __del__(PersistentProxy)

:since: v1.0.0
        """

        self.disconnect()
    #

    def connect(self):
        """
Opens a bus connection.

:since: v1.0.0
        """

        if (self.client is None): self.client = BusClient("pas_tasks_daemon")
    #

    def disconnect(self):
        """
Closes an active bus connection.

:since: v1.0.0
        """

        if (self.client is not None): self.client.disconnect()
    #

    def add(self, tid, hook, timeout = None, **kwargs):
        """
Add a new task with the given TID to the storage for later activation.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        params = self._get_hook_proxy_params(hook, timeout, kwargs)

        self.connect()
        self.client.request("pas.tasks.Persistent.add", tid = tid, **params)
    #

    def call(self, params = None, last_return = None):
        """
Called to initiate a task if its known and valid.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Task result; None if not matched
:since:  v1.0.0
        """

        self.connect()
        return self.client.request("pas.tasks.Persistent.call", params = params, last_return = last_return)
    #

    def get(self, tid):
        """
Returns the task for the given TID.

:param tid: Task ID

:return: (dict) Task definition
:since:  v1.0.0
        """

        self.connect()
        return self.client.request("pas.tasks.Persistent.get", tid = tid)
    #

    def _get_hook_proxy_params(self, hook, timeout = None, kwargs = None):
        """
Returns serializable parameters for the persistent proxy.

:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout
:param kwargs: Keyword arguments

:return: (dict)
:since:  v1.0.0
        """

        _return = { }

        if (isinstance(hook, PersistentLrtHook)):
            _return['hook'] = hook.underlying_hook

            _return['kwargs'] = ({ } if (kwargs is None) else kwargs)
            _return['kwargs'].update(hook.params)
            _return['kwargs']['_lrt_hook'] = True
        else:
            hook = Binary.str(hook)
            if (type(hook) is not str): raise TypeException("Hook given is invalid")

            _return['hook'] = hook
            if (kwargs is not None): _return['kwargs'] = kwargs
        #

        if (timeout is not None): _return['timeout'] = timeout

        return _return
    #

    def is_registered(self, tid, hook = None):
        """
Checks if a given task ID is known.

:param tid: Task ID
:param hook: Task hook to be called

:return: (bool) True if defined
:since:  v1.0.0
        """

        params = ({ } if (hook is None) else self._get_hook_proxy_params(hook))

        self.connect()
        return (True if (self.client.request("pas.tasks.Persistent.isRegistered", tid = tid, **params) == True) else False)
    #

    def register_timeout(self, tid, hook, timeout = None, **kwargs):
        """
Registers a new task with the given TID to the storage for later use.

:param tid: Task ID
:param hook: Task hook to be called
:param timeout: Timeout in seconds; None to use global task timeout

:since: v1.0.0
        """

        params = self._get_hook_proxy_params(hook, timeout, kwargs)

        self.connect()
        self.client.request("pas.tasks.Persistent.registerTimeout", tid = tid, **params)
    #

    def remove(self, tid):
        """
Removes the given TID from the storage.

:param tid: Task ID

:return: (bool) True on success
:since:  v1.0.0
        """

        self.connect()
        return self.client.request("pas.tasks.Persistent.remove", tid = tid)
    #

    def reregister_timeout(self, tid):
        """
Updates the task with the given TID to push its expiration time.

:return: (bool) True on success
:since:  v1.0.0
        """

        self.connect()
        return self.client.request("pas.tasks.Persistent.reregisterTimeout", tid = tid)
    #

    def unregister_timeout(self, tid):
        """
Removes the given TID from the storage.

:return: (bool) True on success
:since:  v1.0.0
        """

        self.connect()
        return self.client.request("pas.tasks.Persistent.unregisterTimeout", tid = tid)
    #

    @staticmethod
    def get_instance():
        """
Get the PersistentProxy singleton.

:return: (PersistentProxy) Object on success
:since:  v1.0.0
        """

        _return = None

        with PersistentProxy._lock:
            if (PersistentProxy._weakref_instance is not None): _return = PersistentProxy._weakref_instance()

            if (_return is None):
                _return = PersistentProxy()
                PersistentProxy._weakref_instance = ref(_return)
            #
        #

        return _return
    #

    @staticmethod
    def is_available():
        """
True if a persistent tasks executing scheduler is available.

:return: (bool) True if available
:since:  v1.0.0
        """

        if (not Settings.is_defined("pas_tasks_daemon_listener_address")):
            Settings.read_file("{0}/settings/pas_tasks_daemon.json".format(Settings.get("path_data")))
        #

        return Settings.is_defined("pas_tasks_daemon_listener_address")
    #
#
