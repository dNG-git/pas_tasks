# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.tasks.store.database
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
pas/#echo(__FILEPATH__)#
----------------------------------------------------------------------------
NOTE_END //n"""

from time import time
from threading import RLock,Thread

from dNG.classes.pas_db import direct_db
from dNG.classes.pas_db_queue import direct_db_queue
from dNG.classes.pas_evars import direct_evars
from dNG.classes.pas_globals import direct_globals
from dNG.classes.pas_logger import direct_logger
from dNG.classes.pas_pluginmanager import direct_plugin_hooks
from dNG.thread.pas_psd_worker import direct_psd_worker

_direct_psd_lrt_counter = 0
_direct_psd_lrt_synchronized = RLock ()

class direct_psd_lrt (Thread):
#
	"""
"LRT" stands for "Long Running Tasks" and means self updating processes.

@author     direct Netware Group
@copyright  (C) direct Netware Group - All rights reserved
@package    pas_complete
@subpackage PSD.LRT
@since      v0.1.00
@license    http://www.direct-netware.de/redirect.php?licenses;gpl
            GNU General Public License 2
	"""

	database = None
	"""
Database object
	"""
	debug = None
	"""
Debug message container
	"""
	data_dict = { }
	"""
Entry data
	"""

	def __init__ (self,data_dict):
	#
		"""
Constructor __init__ (direct_db_queue)

@since v0.1.00
		"""

		Thread.__init__ (self)

		self.debug = direct_globals['debug']
		self.database = direct_db.py_get ()
		self.data_dict = data_dict

		if (self.debug != None): self.debug.append ("#echo(__FILEPATH__)# -LRT.__init__ (direct_psd_lrt)- (#echo(__LINE__)#)")
	#

	def __del__ (self):
	#
		"""
Destructor __del__ (direct_db_queue)

@since v0.1.00
		"""

		self.del_direct_psd_lrt ()
	#

	def del_direct_psd_lrt (self):
	#
		"""
Destructor del_direct_db_queue (direct_db_queue)

@since v0.1.00
		"""

		if (direct_db != None): direct_db.py_del ()
	#

	def process (self):
	#
		"""
Calls the sWG cron service.

@param  params Parameter specified calling "direct_pluginmanager".
@param  last_return The return value from the last hook called.
@return (dict) Updated entry dictionary
@since  v0.1.00
		"""

		try:
		#
			direct_logger.debug ("PSD.LRT gets active")

			if ("hook" in self.data_dict):
			#
				f_data_dict = self.data_dict.copy ()
				f_hook = f_data_dict['hook']
				del (f_data_dict['hook'])

				f_result = direct_plugin_hooks.call (f_hook,**f_data_dict)
				if (not self.update_entry (f_result)): direct_logger.debug ("PSD.LRT returned an invalid result: {0!r}".format (f_result))
			#
			else: direct_logger.error ("PSD.LRT is not configured")
		#
		except Exception as f_handled_exception: direct_logger.critical (f_handled_exception)
	#

	def run (self):
	#
		"""
Worker loop

@since v0.1.00
		"""

		global _direct_psd_lrt_counter,_direct_psd_lrt_synchronized
		if (self.debug != None): self.debug.append ("#echo(__FILEPATH__)# -LRT.run ()- (#echo(__LINE__)#)")

		f_data = direct_evars.get (self.data_dict['entry']['ddbqueue_data'])
		f_queue = direct_db_queue.py_get ()

		try:
		#
			f_thread = Thread (target = self.process)
			f_thread.start ()

			if (("auto_complete" not in self.data_dict) or (self.data_dict['auto_complete'])):
			#
				f_thread.join ()

				if (self.entry_dict['ddbqueue_status'] == direct_db_queue.COMPLETED):
				#
					if ((not f_queue.update_entry_prepare (self.data_dict['entry'])) or (not f_queue.update_entry (self.data_dict['entry']['ddbqueue_id'],status = self.data_dict['entry']['ddbqueue_status']))): f_data['psd_core_error'] = "PSD.LRT database update failed"
				#
				else: f_data['psd_core_error'] = "PSD.LRT failed"
			#
		#
		except Exception as f_handled_exception: f_data['psd_core_error'] = "{0!r}".format (f_handled_exception)

		_direct_psd_lrt_synchronized.acquire ()
		if (_direct_psd_lrt_counter > 0): _direct_psd_lrt_counter -= 1
		_direct_psd_lrt_synchronized.release ()

		if ("psd_core_error" in f_data):
		#
			self.data_dict['entry']['ddbqueue_status'] = direct_db_queue.FAILED
			self.data_dict['entry']['ddbqueue_data'] = direct_evars.write (f_data)
			if ((not f_queue.update_entry_prepare (self.data_dict['entry'])) or (not f_queue.update_entry (self.data_dict['entry']['ddbqueue_id'],data = self.data_dict['entry']['ddbqueue_data'],status = self.data_dict['entry']['ddbqueue_status']))): direct_logger.error (f_data['psd_core_error'])
		#
	#

	def update_entry (self,entry_dict):
	#
		"""
Calls the sWG cron service.

@param  params Parameter specified calling "direct_pluginmanager".
@param  last_return The return value from the last hook called.
@return (dict) Updated entry dictionary
@since  v0.1.00
		"""

		f_return = False

		try:
		#
			f_queue = direct_db_queue.py_get ()

			if ((type (entry_dict) == dict) and (f_queue.update_entry_prepare (entry_dict)) and (f_queue.update_entry (entry_dict['ddbqueue_id'],entry_dict['ddbqueue_name'],entry_dict['ddbqueue_identifier'],entry_dict['ddbqueue_data'],entry_dict['ddbqueue_status'],entry_dict['ddbqueue_started'],entry_dict['ddbqueue_update']))):
			#
				f_return = True
				self.data_dict['entry'] = entry_dict
			#
		#
		except Exception as f_handled_exception: direct_logger.critical (f_handled_exception)

		return f_return
	#

	def call (params = None):
	#
		"""
Calls the sWG cron service.

@param  params Parameter specified calling "direct_pluginmanager".
@param  last_return The return value from the last hook called.
@return (dict) Updated entry dictionary
@since  v0.1.00
		"""

		global _direct_psd_lrt_counter,_direct_psd_lrt_synchronized

		f_return = params['entry']

		try:
		#
			try: f_lrt_limit = int (direct_globals['settings'].get ("pas_psd_lrt_limit",3))
			except ValueError: f_lrt_limit = 3

			try: f_lrt_retry_interval = int (direct_globals['settings'].get ("pas_psd_lrt_retry_interval",30))
			except ValueError: f_lrt_retry_interval = 30

			_direct_psd_lrt_synchronized.acquire ()

			if (_direct_psd_lrt_counter < f_lrt_limit):
			#
				_direct_psd_lrt_counter += 1
				_direct_psd_lrt_synchronized.release ()

				direct_psd_lrt(params).start ()
				f_return['ddbqueue_status'] = direct_db_queue.RUNNING
			#
			else:
			#
				_direct_psd_lrt_synchronized.release ()

				f_return['ddbqueue_status'] = direct_db_queue.WAITING
				f_return['ddbqueue_update'] = int (time () + f_lrt_retry_interval)
			#
		#
		except Exception as f_handled_exception:
		#
			f_data = direct_evars.get (f_return['ddbqueue_data'])
			f_data['psd_core_error'] = "{0!r}".format (f_handled_exception)

			f_return['ddbqueue_data'] = direct_evars.write (f_data)
			f_return['ddbqueue_status'] = direct_db_queue.FAILED
		#

		return f_return
	#
	call = staticmethod (call)

	def queue_call (hook,entry,params = None,retry_interval = 0):
	#
		"""
Prepare the upload of an file with HTTP tasks.

@param  params Parameter specified calling "direct_pluginmanager".
@param  last_return The return value from the last hook called.
@return (bool) True on success
@since  v0.1.00
		"""

		if (len (entry['ddbqueue_data']) > 0): f_data = direct_evars.get (entry['ddbqueue_data'])
		else: f_data = { }

		f_exception_check = False
		f_return = None

		try:
		#
			if (params == None): params = f_data
			params.update ({ "hook": hook })

			f_return = direct_psd_lrt.call (params)

			if (f_return == None):
			#
				f_data['psd_core_error'] = "Exception occurred and logged"
				f_exception_check = True
			#
		#
		except Exception as f_handled_exception:
		#
			f_data['psd_core_error'] = "{0!r}".format (f_handled_exception)
			f_exception_check = True
		#

		if (f_exception_check):
		#
			if (retry_interval > 0): direct_psd_worker.py_get().add (None,entry['ddbqueue_name'],entry['ddbqueue_identifier'],entry['ddbqueue_data'],time_update = int (time () + retry_interval))

			entry['ddbqueue_data'] = direct_evars.write (f_data)
			entry['ddbqueue_status'] = direct_db_queue.FAILED
		#

		return f_return
	#
	queue_call = staticmethod (queue_call)
#

##j## EOF