# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.tasks.Callback
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

from threading import Thread

from dNG.pas.data.logging.log_line import LogLine
from .abstract import Abstract

class Callback(Abstract):
#
	"""
The callback task can be used if the the task store is memory based.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;gpl
             GNU General Public License 2
	"""

	def __init__(self, callback):
	#
		"""
Constructor __init__(Callback)

:since: v0.1.00
		"""

		Abstract.__init__(self)

		self.callback = callback
		"""
Python callback
		"""
	#

	def run(self, task_store, tid, **kwargs):
	#
		"""
Starts the execution of this hook synchronously.

:return: (mixed) Task result
:since:  v0.1.00
		"""

		try: return self.callback(**kwargs)
		except Exception as handled_exception:
		#
			LogLine.error(handled_exception)
			raise
		#
	#

	def start(self, task_store, tid, **kwargs):
	#
		"""
Starts the execution of this hook asynchronously.

:since: v0.1.00
		"""

		thread = Thread(target = self.run, args = ( task_store, tid ), kwargs = kwargs)
		thread.start()
	#
#

##j## EOF