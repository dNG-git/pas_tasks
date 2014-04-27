# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.instances.Task
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

from sqlalchemy import BIGINT, Column, INT, TEXT, VARCHAR
#from sqlalchemy.orm import relationship
from time import time
from uuid import uuid4 as uuid

from .abstract import Abstract

class Task(Abstract):
#
	"""
SQLAlchemy database instance for Task.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	# pylint: disable=invalid-name

	__tablename__ = "{0}_task".format(Abstract.get_table_prefix())
	"""
SQLAlchemy table name
	"""

	id = Column(VARCHAR(32), primary_key = True)
	"""
tasks.id
	"""
	name = Column(VARCHAR(100), index = True, server_default = "", nullable = False)
	"""
tasks.name
	"""
	status = Column(INT, index = True, server_default = "96", nullable = False)
	"""
tasks.status = STATUS_WAITING = 96
	"""
	hook = Column(VARCHAR(255), index = True, server_default = "", nullable = False)
	"""
tasks.hook
	"""
	params = Column(TEXT)
	"""
tasks.params
	"""
	time_started = Column(BIGINT, server_default = "0", nullable = False)
	"""
tasks.time_started
	"""
	time_scheduled = Column(BIGINT, server_default = "0", nullable = False)
	"""
tasks.time_scheduled
	"""
	time_updated = Column(BIGINT, server_default = "0", nullable = False)
	"""
tasks.time_updated
	"""
	timeout = Column(BIGINT, server_default = "0", nullable = False)
	"""
tasks.timeout
	"""

	def __init__(self, *args, **kwargs):
	#
		"""
Constructor __init__(UserProfile)

:since: v0.1.00
		"""

		Abstract.__init__(self, *args, **kwargs)

		if (self.id == None): self.id = uuid().hex

		timestamp = int(time())
		if (self.time_started == None): self.time_started = timestamp
		if (self.time_updated == None): self.time_updated = timestamp
	#
#

##j## EOF