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

from random import randrange
from time import time

from sqlalchemy.sql.expression import and_, or_
from sqlalchemy.sql.functions import count as sql_count

from dNG.data.binary import Binary
from dNG.data.json_resource import JsonResource
from dNG.data.settings import Settings
from dNG.data.text.md5 import Md5
from dNG.database.condition_definition import ConditionDefinition
from dNG.database.connection import Connection
from dNG.database.instance import Instance
from dNG.database.instance_iterator import InstanceIterator
from dNG.database.instances.task import Task as _DbTask
from dNG.database.nothing_matched_exception import NothingMatchedException
from dNG.database.sort_definition import SortDefinition
from dNG.module.named_loader import NamedLoader
from dNG.runtime.io_exception import IOException
from dNG.runtime.type_exception import TypeException

class DatabaseTask(Instance):
    """
A "Database" instance stores tasks in the database.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2
    """

    _DB_INSTANCE_CLASS = _DbTask
    """
SQLAlchemy database instance class to initialize for new instances.
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
        """
Constructor __init__(Database)

:param db_instance: Encapsulated SQLAlchemy database instance

:since: v0.2.00
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

        if (db_instance is not None):
            with self:
                self.db_id = self.local.db_instance.id
                self.hook = self.local.db_instance.hook
                if (self.local.db_instance.params != ""): self.params = JsonResource().json_to_data(self.local.db_instance.params)

                if ("_tid" in self.params): self.tid = self.params['_tid']
            #
        #

        if (self.params is None): self.params = { }
    #

    def _get_hook(self):
        """
Returns the task hook to be called.

:return: (mixed) Task hook either as str or an instance of "AbstractHook"
:since:  v0.2.00
        """

        _return = (NamedLoader.get_instance("dNG.tasks.DatabaseLrtHook", hook = self.hook, **self.params)
                   if (self.params.get("_lrt_hook", False)) else
                   self.hook
                  )

        return _return
    #

    def get_params(self):
        """
Returns the task parameter used.

:return: (dict) Task parameter
:since:  v0.2.00
        """

        return self.params
    #

    get_status = Instance._wrap_getter("status")
    """
Returns the task status.

:return: (int) Task status
:since:  v0.2.00
    """

    def get_tid(self):
        """
Returns the task ID.

:return: (str) Task ID
:since:  v0.2.00
        """

        return self.tid
    #

    get_time_started = Instance._wrap_getter("time_started")
    """
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v0.2.00
    """

    get_time_scheduled = Instance._wrap_getter("time_scheduled")
    """
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v0.2.00
    """

    get_time_updated = Instance._wrap_getter("time_updated")
    """
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v0.2.00
    """

    get_timeout = Instance._wrap_getter("timeout")
    """
Returns the UNIX timestamp this task will time out.

:return: (int) UNIX timestamp
:since:  v0.2.00
    """

    def is_reloadable(self):
        """
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v0.2.00
        """

        _return = True

        if (self.db_id is None):
            # Thread safety
            with self._lock: _return = (self.db_id is not None)
        #

        return _return
    #

    def is_timed_out(self):
        """
Returns true if the task timed out.

:return: (bool) True if timed out
:sinve:  v0.2.00
        """

        timeout = self.get_timeout()
        return (timeout > 0 and timeout < int(time()))
    #

    def is_timeout_set(self):
        """
Returns true if the task has a timeout value.

:return: (bool) True if timeout set
:sinve:  v0.2.00
        """

        return (self.get_timeout() > 0)
    #

    def _reload(self):
        """
Implementation of the reloading SQLAlchemy database instance logic.

:since: v0.2.00
        """

        if (self.local.db_instance is None):
            if (self.db_id is None): raise IOException("Database instance is not reloadable.")
            self.local.db_instance = self.local.connection.query(_DbTask).filter(_DbTask.id == self.db_id).one()
        else: Instance._reload(self)
    #

    def save(self):
        """
Saves changes of the database task instance.

:since: v0.2.00
        """

        with self:
            self.local.db_instance.tid = Binary.utf8(Md5.hash(self.tid))
            self.params['_tid'] = self.tid

            if (self.local.db_instance.name == ""): self.local.db_instance.name = Binary.utf8(self.hook[-100:])
            if (self.local.db_instance.status is None): self.local.db_instance.status = DatabaseTask.STATUS_WAITING
            self.local.db_instance.hook = Binary.utf8(self.hook)
            self.local.db_instance.params = Binary.utf8(JsonResource().data_to_json(self.params))
            self.local.db_instance.time_updated = int(time())

            Instance.save(self)
        #
    #

    def set_data_attributes(self, **kwargs):
        """
Sets values given as keyword arguments to this method.

:since: v0.2.00
        """

        with self:
            if (self.db_id is None): self.db_id = self.local.db_instance.id

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

    def _set_hook(self, hook):
        """
Sets the task hook to be called.

:param hook: Task hook

:since: v0.2.00
        """

        self.hook = hook
    #

    set_name = Instance._wrap_setter("name")
    """
Sets the task name.

:param value: Task name

:since: v0.2.00
    """

    def set_params(self, params):
        """
Sets the task parameter.

:param params: Task parameter

:since: v0.2.00
        """

        if (not isinstance(params, dict)): raise TypeException("Parameter given are invalid")
        self.params = params
    #

    set_status = Instance._wrap_setter("status")
    """
Sets the task status.

:param value: Task status

:since: v0.2.00
    """

    def set_status_completed(self):
        """
Sets the task status to "completed".

:since: v0.2.00
        """

        self.set_data_attributes(status = DatabaseTask.STATUS_COMPLETED)
    #

    def set_tid(self, tid):
        """
Sets the task ID.

:param tid: Task ID

:since: v0.2.00
        """

        self.tid = tid
    #

    set_time_scheduled = Instance._wrap_setter("time_scheduled")
    """
Sets the time the task is scheduled to be executed.

:param value: UNIX timestamp

:since: v0.2.00
    """

    set_timeout = Instance._wrap_setter("timeout")
    """
Sets a timeout for the task.

:param value: UNIX timestamp

:since: v0.2.00
    """

    @staticmethod
    def _get_default_list_condition_definition():
        """
Returns the default condition definition used for listings.

:return: (object) ConditionDefinition instance
:since:  v0.2.00
        """

        _return = ConditionDefinition()

        archive_timeout = int(Settings.get("pas_tasks_database_tasks_archive_timeout", 28)) * 86400
        completed_condition_definition = ConditionDefinition(ConditionDefinition.AND)
        timestamp = int(time())
        timestamp_archive = timestamp - archive_timeout

        completed_condition_definition.add_exact_match_condition("status", DatabaseTask.STATUS_COMPLETED)
        completed_condition_definition.add_greater_than_match_condition("time_scheduled", 0)
        completed_condition_definition.add_less_than_match_condition("time_scheduled", timestamp_archive)

        _return.add_sub_condition(completed_condition_definition)
        _return.add_exact_no_match_condition("status", DatabaseTask.STATUS_COMPLETED)

        _return.add_exact_match_condition("timeout", 0)
        _return.add_greater_than_match_condition("timeout", timestamp)

        return _return
    #

    @staticmethod
    def get_list_count(condition_definition = None):
        """
Returns the count of database entries based on the given condition
definition. If no separate condition is given all non-completed will be
returned.

:param condition_definition: ConditionDefinition instance

:return: (int) Number of DatabaseTask entries
:since:  v0.2.00
        """

        with Connection.get_instance() as connection:
            db_query = connection.query(sql_count(_DbTask.id))

            if (condition_definition is None): condition_definition = DatabaseTask._get_default_list_condition_definition()

            if (condition_definition is not None):
                db_query = condition_definition.apply(_DbTask, db_query)
            #

            return db_query.scalar()
        #
    #

    @staticmethod
    def _load(cls, db_instance):
        """
Load DatabaseTask entry from database.

:param cls: Expected encapsulating database instance class
:param db_instance: SQLAlchemy database instance

:return: (object) DatabaseTask instance on success
:since:  v0.2.00
        """

        _return = None

        if (db_instance is not None):
            with Connection.get_instance() as connection:
                Instance._ensure_db_class(cls, db_instance)

                _return = DatabaseTask(db_instance)
                if (_return.is_timed_out()): _return = None

                if ((not Settings.get("pas_database_auto_maintenance", False)) and randrange(0, 3) < 1):
                    archive_timeout = int(Settings.get("pas_tasks_database_tasks_archive_timeout", 28)) * 86400
                    timestamp = int(time())
                    timestamp_archive = timestamp - archive_timeout

                    if (connection.query(_DbTask)
                        .filter(or_(and_(_DbTask.status == DatabaseTask.STATUS_COMPLETED,
                                         _DbTask.time_scheduled > 0,
                                         _DbTask.time_scheduled < timestamp_archive
                                        ),
                                    and_(_DbTask.timeout > 0, _DbTask.timeout < timestamp)
                                   )
                               )
                        .delete() > 0
                       ): connection.optimize_random(_DbTask)
                #
            #
        #

        return _return
    #

    @classmethod
    def load_id(cls, _id):
        """
Load DatabaseTask value by entry ID.

:param cls: Expected encapsulating database instance class
:param _id: Task entry ID

:return: (object) DatabaseTask instance on success
:since:  v0.2.00
        """

        if (_id is None): raise NothingMatchedException("Task entry ID is invalid")

        with Connection.get_instance(): _return = DatabaseTask._load(cls, Instance.get_db_class_query(cls).get(_id))

        if (_return is None): raise NothingMatchedException("Task entry ID '{0}' not found".format(_id))
        return _return
    #

    @classmethod
    def load_next(cls, status = None):
        """
Load DatabaseTask to be executed next.

:param cls: Expected encapsulating database instance class
:param status: Task status

:return: (object) DatabaseTask instance on success
:since:  v0.2.00
        """

        if (status is None): status = DatabaseTask.STATUS_WAITING

        with Connection.get_instance():
            db_instance = (Instance.get_db_class_query(cls)
                           .filter(_DbTask.status == status,
                                   _DbTask.time_scheduled > 0,
                                   or_(_DbTask.timeout == 0,
                                       _DbTask.timeout >= int(time())
                                      )
                                  )
                           .order_by(_DbTask.time_scheduled.asc())
                           .first()
                          )

            _return = DatabaseTask._load(cls, db_instance)
        #

        if (_return is None): raise NothingMatchedException("No scheduled task found")
        return _return
    #

    @classmethod
    def load_tid(cls, tid):
        """
Load DatabaseTask value by ID.

:param cls: Expected encapsulating database instance class
:param tid: Task ID

:return: (object) DatabaseTask instance on success
:since:  v0.2.00
        """

        if (tid is None): raise NothingMatchedException("Task ID is invalid")

        with Connection.get_instance():
            db_instance = Instance.get_db_class_query(cls).filter(_DbTask.tid == Md5.hash(tid)).limit(1).first()
            _return = DatabaseTask._load(cls, db_instance)
        #

        if (_return is None): raise NothingMatchedException("Task ID '{0}' not found".format(tid))
        return _return
    #

    @staticmethod
    def load_list(condition_definition = None, offset = 0, limit = -1, sort_definition = None):
        """
Loads a list of database instances based on the given condition definition.
If no separate condition is given all tasks will be returned that are not
completed or are within the archived time frame.

:param condition_definition: ConditionDefinition instance
:param offset: SQLAlchemy query offset
:param limit: SQLAlchemy query limit
:param sort_definition: SortDefinition instance

:return: (list) List of DatabaseTask instances on success
:since:  v0.2.00
        """

        with Connection.get_instance() as connection:
            db_query = connection.query(_DbTask)

            if (condition_definition is None): condition_definition = DatabaseTask._get_default_list_condition_definition()

            if (condition_definition is not None):
                db_query = condition_definition.apply(_DbTask, db_query)
            #

            if (sort_definition is None):
                sort_definition = SortDefinition([ ( "status", SortDefinition.DESCENDING ),
                                                   ( "time_scheduled", SortDefinition.DESCENDING ),
                                                   ( "time_updated", SortDefinition.DESCENDING ),
                                                   ( "tid", SortDefinition.ASCENDING )
                                                 ])
            #

            db_query = sort_definition.apply(_DbTask, db_query)
            if (offset > 0): db_query = db_query.offset(offset)
            if (limit > 0): db_query = db_query.limit(limit)

            return InstanceIterator(_DbTask, connection.execute(db_query), instance_class = DatabaseTask)
        #
    #

    @staticmethod
    def _reset_stale_running():
        """
Resets all stale tasks with the "running" status.

:since: v0.2.00
        """

        with Connection.get_instance() as connection:
            connection.query(_DbTask).filter(or_(_DbTask.status == DatabaseTask.STATUS_QUEUED,
                                                 _DbTask.status == DatabaseTask.STATUS_RUNNING
                                                )
                                            ).update({ "status": DatabaseTask.STATUS_WAITING })
        #
    #
#
