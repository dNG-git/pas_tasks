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
setup.py
"""

from os import makedirs, path

try:
    from setuptools import setup
except ImportError:
    from distutils import setup
#

try:
    from dpt_builder_suite.distutils.build_py import BuildPy
    from dpt_builder_suite.distutils.install_data import InstallData
    from dpt_builder_suite.distutils.sdist import Sdist
    from dpt_builder_suite.distutils.temporary_directory import TemporaryDirectory
except ImportError:
    raise RuntimeError("'dpt-builder-suite' prerequisite not matched")
#

def get_version():
    """
Returns the version currently in development.

:return: (str) Version string
:since:  v0.1.1
    """

    return "v1.0.0"
#

with TemporaryDirectory(dir = ".") as build_directory:
    parameters = { "pasTasksVersion": get_version(), "plain_copy_extensions": "json,sql" }

    BuildPy.set_build_target_path(build_directory)
    BuildPy.set_build_target_parameters(parameters)

    InstallData.add_install_data_callback(InstallData.plain_copy, [ "data" ])
    InstallData.set_build_target_path(build_directory)
    InstallData.set_build_target_parameters(parameters)

    Sdist.set_build_target_path(build_directory)
    Sdist.set_build_target_parameters(parameters)

    package_dir = path.join(build_directory, "src")
    makedirs(package_dir)

    _setup = { "version": get_version()[1:],
               "package_dir": { "": package_dir },
               "packages": [ "pas_tasks" ],
               "data_files": [ ( "docs", [ "LICENSE", "README" ]) ],
               "entry_points": { "console_scripts": [ "pas-tasks-daemon = pas_tasks.__main__:main" ] },
               "test_suite": "tests"
             }

    # Override build_py to first run builder.py
    _setup['cmdclass'] = { "build_py": BuildPy, "install_data": InstallData, "sdist": Sdist }

    setup(**_setup)
#
