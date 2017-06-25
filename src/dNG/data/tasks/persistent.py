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

from dNG.data.settings import Settings
from dNG.module.named_loader import NamedLoader
from dNG.runtime.not_implemented_class import NotImplementedClass

from .abstract_persistent import AbstractPersistent
from .abstract_persistent_proxy import AbstractPersistentProxy

class Persistent(object):
    """
"Persistent" provides access to the implementation independent, persistent
task scheduler.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2
    """

    @staticmethod
    def get_class(is_not_implemented_class_aware = False):
        """
Returns the persistent tasks implementation class based on the configuration
set.

:param is_not_implemented_class_aware: True to return
       "dNG.runtime.NotImplementedClass" instead of None

:return: (object) Tasks implementation class; None if not available
:since:  v0.2.00
        """

        _return = None

        persistent_tasks = Persistent.get_executing_instance()

        if (isinstance(persistent_tasks, AbstractPersistent) and persistent_tasks.is_executing_daemon()):
            _return = persistent_tasks
        else:
            persistent_tasks_proxy = NamedLoader.get_singleton("dNG.data.tasks.PersistentProxy")

            if (isinstance(persistent_tasks_proxy, AbstractPersistentProxy)
                and persistent_tasks_proxy.is_available()
               ): _return = persistent_tasks_proxy
        #

        if (_return is None and is_not_implemented_class_aware): _return = NotImplementedClass

        return _return
    #

    @staticmethod
    def get_executing_class(is_not_implemented_class_aware = False):
        """
Returns the persistent tasks implementation class responsible for scheduling
and execution based on the configuration set.

:param is_not_implemented_class_aware: True to return
       "dNG.runtime.NotImplementedClass" instead of None

:return: (object) Tasks implementation class; None if not available
:since:  v0.2.00
        """

        if (not Settings.is_defined("pas_tasks_daemon_listener_address")):
            Settings.read_file("{0}/settings/pas_tasks_daemon.json".format(Settings.get("path_data")))
        #

        implementation_class_name = Settings.get("pas_tasks_persistent_implementation", "Database")

        _return = NamedLoader.get_class("dNG.data.tasks.{0}".format(implementation_class_name))
        if (_return is None and is_not_implemented_class_aware): _return = NotImplementedClass

        return _return
    #

    @staticmethod
    def get_executing_instance():
        """
Returns the persistent tasks implementation responsible for scheduling and
execution based on the configuration set.

:return: (object) Tasks implementation
:since:  v0.2.00
        """

        implementation_class = Persistent.get_executing_class(True)
        return implementation_class.get_instance()
    #

    @staticmethod
    def get_instance():
        """
Returns the persistent tasks implementation based on the configuration set.

:return: (object) Tasks implementation
:since:  v0.2.00
        """

        implementation_class = Persistent.get_class(True)
        return implementation_class.get_instance()
    #

    @staticmethod
    def is_available():
        """
True if a persistent tasks executing scheduler is available.

:return: (bool) True if available
:since:  v0.2.00
        """

        persistent_tasks_class = Persistent.get_class(True)
        return isinstance(persistent_tasks_class, AbstractPersistent)
    #

    @staticmethod
    def is_executing_daemon(self):
        """
True if the instance is executing scheduled tasks.

:return: (bool) True if executing scheduled tasks
:since:  v0.2.00
        """

        return Persistent.get_instance().is_executing_daemon()
    #
#
