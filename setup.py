#!/usr/bin/python
# -*- coding: utf-8 -*-
#
#
# Copyright (C) 2014 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#

from distutils.core import setup
from setuptools import find_packages
import os
import subprocess

# Fetch version from git tags,
# it will crash if you are running this outside a git clone

version_git = subprocess.check_output(["git", "describe","--tags"]).rstrip()

setup(name = "GrimoireLib",
      version="{ver}".format(ver=version_git),
      author =  "Daniel Izquierdo et al",
      author_email = "dizquierdo@bitergia.com",
      description = "Open Source projects data mining library",
      url = "https://github.com/VizGrimoire/GrimoireLib",
      packages = find_packages())
