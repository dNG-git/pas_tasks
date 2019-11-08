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

from dpt_plugins import Hook

from .abstract_lrt_hook import AbstractLrtHook

class PersistentLrtHook(AbstractLrtHook):
    """
A "PersistentLrtHook" is an persistent, long running task.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: tasks
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;gpl
             GNU General Public License 2 or later
    """

    __slots__ = [ "_hook" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def __init__(self, hook, **kwargs):
        """
Constructor __init__(PersistentLrtHook)

:since: v1.0.0
        """

        AbstractLrtHook.__init__(self)

        self._hook = hook
        """
Hook
        """

        self.context_id = hook
        self.independent_scheduling = True
        if (len(kwargs) > 0): self._params = kwargs
    #

    def __str__(self):
        """
python.org: Called by the str() built-in function and by the print statement
to compute the "informal" string representation of an object.

:return: (str) Informal string representation
:since:  v1.0.0
        """

        return (object.__str__(self) if (self._hook is None) else self.underlying_hook)
    #

    @property
    def underlying_hook(self):
        """
Returns the hook being called of this long running task instance.

:return: (str) Hook
:since:  v1.0.0
        """

        return self._hook
    #

    def _run_hook(self):
        """
Hook execution

:since: v1.0.0
        """

        Hook.call_one(self.underlying_hook, **self.params)
    #
#
