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

try: from hashlib import sha3_256 as sha256
except ImportError: from hashlib import sha256

from dpt_json import JsonResource
from dpt_module_loader import NamedClassLoader
from dpt_runtime.binary import Binary
from dpt_runtime.io_exception import IOException
from dpt_runtime.type_exception import TypeException
from dpt_settings import Settings
from pas_database import ConditionDefinition, Connection, Instance, NothingMatchedException, SortDefinition
from sqlalchemy.sql.expression import and_, or_
from sqlalchemy.sql.functions import count as sql_count

from ..orm.task import Task as _DbTask

class Task(Instance):
    """
A "Database" instance stores tasks in the database.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
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

:since: v1.0.0
        """

        Instance.__init__(self, db_instance)

        self._id = None
        """
Database ID used for reloading
        """
        self.hook = ""
        """
Task hook to be called
        """
        self._params = None
        """
Task parameter specified
        """
        self._tid = None
        """
Task ID
        """

        if (db_instance is not None):
            with self:
                self._id = self.local.db_instance.id
                self.hook = self.local.db_instance.hook
                if (self.local.db_instance.params != ""): self.params = JsonResource.json_to_data(self.local.db_instance.params)

                if ("_tid" in self.params): self.tid = self.params['_tid']
            #
        #

        if (self._params is None): self._params = { }
    #

    @property
    def _hook(self):
        """
Returns the task hook to be called.

:return: (mixed) Task hook either as str or an instance of "AbstractHook"
:since:  v1.0.0
        """

        return (NamedClassLoader.get_instance("pas_tasks.tasks.DatabaseLrtHook", hook = self.hook, **self.params)
                if (self.is_lrt) else
                self.hook
               )
    #

    @_hook.setter
    def _hook(self, hook):
        """
Sets the task hook to be called.

:param hook: Task hook

:since: v1.0.0
        """

        self.hook = hook
    #

    @property
    def is_reloadable(self):
        """
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v1.0.0
        """

        _return = True

        if (self._id is None):
            # Thread safety
            with self._lock: _return = (self._id is not None)
        #

        return _return
    #

    @property
    def is_lrt(self):
        """
Returns true if the task is a long running one.

:return: (bool) True if long running task
:since:  v1.0.0
        """

        return self._params.get("_lrt_hook", False)
    #

    @property
    def is_timed_out(self):
        """
Returns true if the task timed out.

:return: (bool) True if timed out
:since:  v1.0.0
        """

        timeout = self.timeout
        return (timeout > 0 and timeout < int(time()))
    #

    @property
    def is_timeout_set(self):
        """
Returns true if the task has a timeout value.

:return: (bool) True if timeout set
:since:  v1.0.0
        """

        return (self.timeout > 0)
    #

    name = Instance._data_attribute_property("name")
    """
Sets the task name.

:param value: Task name

:since: v1.0.0
    """

    @property
    def params(self):
        """
Returns the task parameter used.

:return: (dict) Task parameter
:since:  v1.0.0
        """

        return self._params
    #

    @params.setter
    def params(self, params):
        """
Sets the task parameter.

:param params: Task parameter

:since: v1.0.0
        """

        if (not isinstance(params, dict)): raise TypeException("Parameter given are invalid")
        self._params = params
    #

    status = Instance._data_attribute_property("status")
    """
Returns the task status.

:return: (int) Task status
:since:  v1.0.0
    """

    @property
    def tid(self):
        """
Returns the task ID.

:return: (str) Task ID
:since:  v1.0.0
        """

        return self._tid
    #

    @tid.setter
    def tid(self, tid):
        """
Sets the task ID.

:param tid: Task ID

:since: v1.0.0
        """

        self._tid = tid
    #

    time_scheduled = Instance._data_attribute_property("time_scheduled")
    """
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v1.0.0
    """

    time_started = Instance._data_attribute_readonly_property("time_started")
    """
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v1.0.0
    """

    time_updated = Instance._data_attribute_readonly_property("time_updated")
    """
Returns the UNIX timestamp this task will be executed.

:return: (int) UNIX timestamp
:since:  v1.0.0
    """

    timeout = Instance._data_attribute_property("timeout")
    """
Returns the UNIX timestamp this task will time out.

:return: (int) UNIX timestamp
:since:  v1.0.0
    """

    def _reload(self):
        """
Implementation of the reloading SQLAlchemy database instance logic.

:since: v1.0.0
        """

        if (self.local.db_instance is None):
            if (self._id is None): raise IOException("Database instance is not reloadable.")
            self.local.db_instance = self.local.connection.query(_DbTask).filter(_DbTask.id == self._id).one()
        else: Instance._reload(self)
    #

    def save(self):
        """
Saves changes of the database task instance.

:since: v1.0.0
        """

        with self:
            self.local.db_instance.tid = sha256(Binary.utf8_bytes(self.tid)).hexdigest()
            self._params['_tid'] = self.tid

            if (self.local.db_instance.name == ""): self.local.db_instance.name = Binary.utf8(self.hook[-100:])
            if (self.local.db_instance.status is None): self.local.db_instance.status = Task.STATUS_WAITING
            self.local.db_instance.hook = Binary.utf8(self.hook)
            self.local.db_instance.params = Binary.utf8(JsonResource().data_to_json(self.params))
            self.local.db_instance.time_updated = int(time())

            Instance.save(self)
        #
    #

    def _set_data_attribute(self, attribute, value):
        """
Sets data for the requested attribute.

:param attribute: Requested attribute
:param value: Value for the requested attribute

:since: v1.0.0
        """

        if (attribute == "tid"): self.tid = value
        elif (attribute == "hook"): self.hook = value
        elif (attribute == "params" and isinstance(value, dict)): self.params = value
        else:
            if (attribute == "name"): value = Binary.utf8(value)
            Instance._set_data_attribute(self, attribute, value)
        #
    #

    def set_status_completed(self):
        """
Sets the task status to "completed".

:since: v1.0.0
        """

        self.set_data_attributes(status = Task.STATUS_COMPLETED)
    #

    @staticmethod
    def _get_default_list_condition_definition():
        """
Returns the default condition definition used for listings.

:return: (object) ConditionDefinition instance
:since:  v1.0.0
        """

        _return = ConditionDefinition()

        archive_timeout = int(Settings.get("pas_tasks_database_tasks_archive_timeout", 28)) * 86400
        completed_condition_definition = ConditionDefinition(ConditionDefinition.AND)
        timestamp = int(time())
        timestamp_archive = timestamp - archive_timeout

        completed_condition_definition.add_exact_match_condition("status", Task.STATUS_COMPLETED)
        completed_condition_definition.add_greater_than_match_condition("time_scheduled", 0)
        completed_condition_definition.add_less_than_match_condition("time_scheduled", timestamp_archive)

        _return.add_sub_condition(completed_condition_definition)
        _return.add_exact_no_match_condition("status", Task.STATUS_COMPLETED)

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

:return: (int) Number of Task entries
:since:  v1.0.0
        """

        with Connection.get_instance() as connection:
            db_query = connection.query(sql_count(_DbTask.id))

            if (condition_definition is None): condition_definition = Task._get_default_list_condition_definition()

            if (condition_definition is not None):
                db_query = condition_definition.apply(_DbTask, db_query)
            #

            return db_query.scalar()
        #
    #

    @staticmethod
    def _load(cls, db_instance):
        """
Load Task entry from database.

:param cls: Expected encapsulating database instance class
:param db_instance: SQLAlchemy database instance

:return: (object) Task instance on success
:since:  v1.0.0
        """

        _return = None

        if (db_instance is not None):
            with Connection.get_instance() as connection:
                Instance._ensure_db_class(cls, db_instance)

                _return = Task(db_instance)
                if (_return.is_timed_out): _return = None

                if ((not Settings.get("pas_database_auto_maintenance", False)) and randrange(0, 3) < 1):
                    archive_timeout = int(Settings.get("pas_tasks_database_tasks_archive_timeout", 28)) * 86400
                    timestamp = int(time())
                    timestamp_archive = timestamp - archive_timeout

                    if (connection.query(_DbTask)
                        .filter(or_(and_(_DbTask.status == Task.STATUS_COMPLETED,
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
Load Task value by entry ID.

:param cls: Expected encapsulating database instance class
:param _id: Task entry ID

:return: (object) Task instance on success
:since:  v1.0.0
        """

        if (_id is None): raise NothingMatchedException("Task entry ID is invalid")

        with Connection.get_instance(): _return = Task._load(cls, Instance.get_db_class_query(cls).get(_id))

        if (_return is None): raise NothingMatchedException("Task entry ID '{0}' not found".format(_id))
        return _return
    #

    @classmethod
    def load_next(cls, status = None):
        """
Load Task to be executed next.

:param cls: Expected encapsulating database instance class
:param status: Task status

:return: (object) Task instance on success
:since:  v1.0.0
        """

        if (status is None): status = Task.STATUS_WAITING

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

            _return = Task._load(cls, db_instance)
        #

        if (_return is None): raise NothingMatchedException("No scheduled task found")
        return _return
    #

    @classmethod
    def load_tid(cls, tid):
        """
Load Task value by ID.

:param cls: Expected encapsulating database instance class
:param tid: Task ID

:return: (object) Task instance on success
:since:  v1.0.0
        """

        if (tid is None): raise NothingMatchedException("Task ID is invalid")
        tid = sha256(Binary.utf8_bytes(tid)).hexdigest()

        with Connection.get_instance():
            db_instance = Instance.get_db_class_query(cls).filter(_DbTask.tid == tid).limit(1).first()
            _return = Task._load(cls, db_instance)
        #

        if (_return is None): raise NothingMatchedException("Task ID '{0}' not found".format(tid))
        return _return
    #

    @classmethod
    def load_list(cls, condition_definition = None, offset = 0, limit = -1, sort_definition = None):
        """
Loads a list of database instances based on the given condition definition.
If no separate condition is given all tasks will be returned that are not
completed or are within the archived time frame.

:param cls: Encapsulating database instance class
:param condition_definition: ConditionDefinition instance
:param offset: SQLAlchemy query offset
:param limit: SQLAlchemy query limit
:param sort_definition: SortDefinition instance

:return: (list) List of Task instances on success
:since:  v1.0.0
        """

        with Connection.get_instance() as connection:
            db_query = connection.query(_DbTask)

            if (condition_definition is None): condition_definition = Task._get_default_list_condition_definition()

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

            return cls.iterator(_DbTask, connection.execute(db_query))
        #
    #

    @staticmethod
    def _regenerate_tids():
        """
Regenerates all task IDs based on the current hashing algorithm.

:since: v1.0.0
        """

        with Connection.get_instance():
            for task in Task.load_list(ConditionDefinition()): task.save()
        #
    #

    @staticmethod
    def _reset_stale_running():
        """
Resets all stale tasks with the "running" status.

:since: v1.0.0
        """

        with Connection.get_instance() as connection:
            connection.query(_DbTask).filter(or_(_DbTask.status == Task.STATUS_QUEUED,
                                                 _DbTask.status == Task.STATUS_RUNNING
                                                )
                                            ).update({ "status": Task.STATUS_WAITING })
        #
    #
#
