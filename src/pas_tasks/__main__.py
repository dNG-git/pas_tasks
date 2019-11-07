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

from argparse import ArgumentParser
from time import time

from dpt_cli import Cli
from dpt_module_loader import NamedClassLoader
from dpt_plugins import Hook, Manager
from dpt_runtime.environment import Environment
from dpt_runtime.io_exception import IOException
from dpt_settings import Settings
from pas_bus import Client as BusClient
from pas_bus import CliMixin as BusMixin
from pas_bus import Server as BusServer

class Application(Cli, BusMixin):
    """
Daemon application to execute database tasks scheduled.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    # pylint: disable=unused-argument

    def __init__(self):
        """
Constructor __init__(Application)

:since: v1.0.0
        """

        Cli.__init__(self)
        BusMixin.__init__(self)

        self.cache_instance = None
        """
Cache instance
        """
        self.server = None
        """
Server thread
        """

        self.arg_parser = ArgumentParser()
        self.arg_parser.add_argument("--additionalSettings", action = "store", type = str, dest = "additional_settings")
        self.arg_parser.add_argument("--reloadPlugins", action = "store_true", dest = "reload_plugins")
        self.arg_parser.add_argument("--stop", action = "store_true", dest = "stop")

        Cli.register_run_callback(self._on_run)
        Cli.register_shutdown_callback(self._on_shutdown)
    #

    @Cli.log_handler.setter
    def log_handler(self, log_handler):
        """
Sets the LogHandler.

:param log_handler: LogHandler to use

:since: v1.0.0
        """

        Cli.log_handler.fset(self, log_handler)

        Hook.set_log_handler(log_handler)
        NamedClassLoader.set_log_handler(log_handler)
    #

    def _on_run(self, args):
        """
Callback for execution.

:param args: Parsed command line arguments

:since: v1.0.0
        """

        # pylint: disable=attribute-defined-outside-init

        Settings.read_file("{0}/settings/core.json".format(Settings.get("path_data")), True)
        Settings.read_file("{0}/settings/pas_tasks_daemon.json".format(Settings.get("path_data")), True)
        if (args.additional_settings is not None): Settings.read_file(args.additional_settings, True)

        if (not Settings.is_defined("pas_tasks_daemon_listener_address")): raise IOException("No listener address defined for the tasks daemon")

        if (args.reload_plugins):
            client = BusClient("pas_tasks_daemon")
            client.request("pas.Plugins.reload")
        elif (args.stop):
            client = BusClient("pas_tasks_daemon")

            pid = client.request("pas.Application.getOSPid")
            client.request("pas.Application.stop")

            self._wait_for_os_pid(pid)
        else:
            self.cache_instance = NamedClassLoader.get_singleton("pas_cache.Content", False)
            if (self.cache_instance is not None): Settings.set_cache_instance(self.cache_instance)

            log_handler = NamedClassLoader.get_singleton("dpt_logging.LogHandler", False)
            if (log_handler is not None): self.log_handler = log_handler

            Hook.load("tasks")
            Hook.register("pas.Application.getOSPid", self.get_os_pid)
            Hook.register("pas.Application.getTimeStarted", self.get_time_started)
            Hook.register("pas.Application.getUptime", self.get_uptime)
            Hook.register("pas.Application.stop", self.stop)

            self.server = BusServer("pas_tasks_daemon")
            self._time_started = time()

            if (self._log_handler is not None): self._log_handler.info("Tasks daemon starts listening", context = "pas_tasks")

            Hook.call("pas.Application.onStartup")
            Hook.call("pas.tasks.Daemon.onStartup")

            self.mainloop = self.server.run
        #
    #

    def _on_shutdown(self):
        """
Callback for shutdown.

:since: v1.0.0
        """

        Hook.call("pas.tasks.Daemon.onShutdown")
        Hook.call("pas.Application.onShutdown")

        if (self.cache_instance is not None): self.cache_instance.disable()
        Hook.free()
    #

    def _signal(self, signal_name, stack_frame):
        """
Handles an OS signal.

:param signal_name: Signal name
:param stack_frame: Stack frame

:since: v1.0.0
        """

        if (signal_name == "SIGHUP"): Manager.reload_plugins()
        else: Cli._signal(self, signal_name, stack_frame)
    #

    def stop(self, params = None, last_return = None):
        """
Stops the running server instance.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (None) None to stop communication after this call
:since:  v1.0.0
        """

        if (self.server is not None):
            self.server.stop()
            self.server = None

            if (self._log_handler is not None): self._log_handler.info("Tasks daemon stopped listening", context = "pas_tasks")
        #

        return last_return
    #
#

def main():
    """
Application main entry point.

:since: v1.0.0
    """

    application = None
    if (not Environment.is_application_short_name_defined()): Environment.set_application_short_name("pas.tasks")

    try:
        application = Application()
        application.run()
    except Exception as handled_exception:
        if (application is not None):
            application.error(handled_exception)
            application.stop()
        else: sys.stderr.write("{0!r}".format(sys.exc_info()))
    #
#

if (__name__ == "__main__"): main()
