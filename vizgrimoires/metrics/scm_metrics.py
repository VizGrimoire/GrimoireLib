#!/usr/bin/python3
## Copyright (C) 2014 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##   Alvaro del Castillo  <acs@bitergia.com>
##

from metrics.metrics import Metrics

class Commits(Metrics):
    """ Commits metric class for source code management systems """

    id = "commits"
    name = "Commits"
    desc = "Changes to the source code"
    FIELD_COUNT = 'hash' # field used to count commits
    FIELD_NAME = 'hash' # field used to list commits


class Authors(Metrics):
    """ Authors metric class for source code management systems """

    id = "authors"
    name = "Authors"
    desc = "People authoring commits (changes to source code)"
    FIELD_COUNT = 'author_uuid' # field used to count Authors
    FIELD_NAME = 'author_name' # field used to list Authors


class Committers(Metrics):
    """ Committers metric class for source code management systems """

    id = "committers"
    name = "Committers"
    desc = "Number of developers committing (merging changes to source code)"
    FIELD_COUNT = 'Commit_uuid'
    FIELD_NAME = 'Commit_name'


class ProjectsSCM(Metrics):
    """ Projects in the source code management system """

    id = "projects"
    name = "Projects"
    desc = "Projects in the source code management system"
    FIELD_NAME = 'project' # field used to list projects
