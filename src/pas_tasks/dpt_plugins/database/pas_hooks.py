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

from dpt_module_loader import NamedClassLoader
from dpt_plugins import Hook
from pas_database.schema import Schema

def after_apply_schema(params, last_return = None):
    """
Called for "pas.Database.applySchema.after"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    task_class = NamedClassLoader.get_class("pas_tasks.orm.Task")
    Schema.apply_version(task_class)

    return last_return
#

def after_apply_task_schema(params, last_return = None):
    """
Called for "pas.database.Task.applySchema.after"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    if (params.get("current_version") < 3 and params.get("target_version") >= 3):
        task_class = NamedClassLoader.get_class("pas_tasks.instances.Task")
        task_class._regenerate_tids()
    #

    return last_return
#

def load_all(params, last_return = None):
    """
Load and register all SQLAlchemy objects to generate database tables.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    NamedClassLoader.get_class("pas_tasks.orm.Task")
    return last_return
#

def register_plugin():
    """
Register plugin hooks.

:since: v1.0.0
    """

    Hook.register("pas.Database.applySchema.after", after_apply_schema)
    Hook.register("pas.Database.loadAll", load_all)
    Hook.register("pas.database.Task.applySchema.after", after_apply_task_schema)
#

def unregister_plugin():
    """
Unregister plugin hooks.

:since: v1.0.0
    """

    Hook.unregister("pas.Database.applySchema.after", after_apply_schema)
    Hook.unregister("pas.Database.loadAll", load_all)
    Hook.unregister("pas.database.Task.applySchema.after", after_apply_task_schema)
#
