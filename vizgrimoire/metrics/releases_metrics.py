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
##


import logging
import MySQLdb

import re, sys

from vizgrimoire.GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff

from vizgrimoire.metrics.metrics import Metrics

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.metrics.query_builder import MLSQuery

from vizgrimoire.ReleasesDS import ReleasesDS

class Releases(Metrics):
    """ Number of releases done in a project """

    id = "releases"
    name = "Releases"
    desc = "Number of Releases done in a project"
    data_source = ReleasesDS

    def _get_sql(self, evolutionary):

        fields = "COUNT(DISTINCT(r.id)) AS releases"
        tables = "releases r, projects p"
        filters = "r.project_id = p.id"
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, "r.created_on", fields,
                               tables, filters, evolutionary)
        return q

class Modules(Metrics):
    """ Number of modules (projects)  """

    id = "modules"
    name = "Modules (projects)"
    desc = "Number of modules (projects)"
    data_source = ReleasesDS

    def _get_sql (self, evolutionary):
        fields = "COUNT(*) AS modules"
        tables = "projects p"
        filters = ""

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, "p.created_on", fields,
                               tables, filters, evolutionary)
        return q


class Authors(Metrics):
    """ Number of people working in modules (projects) and releases  """

    id = "authors"
    name = "Authors"
    desc = "Number of people working in modules (projects) and releases"
    data_source = ReleasesDS

    def _get_sql (self, evolutionary):
        fields = " COUNT(DISTINCT(pup.uuid)) as authors "
        # fields = "COUNT(DISTINCT(u.id)) AS authors"
        tables = "users u, releases r, projects p, people_uidentities pup"
        filters = "r.author_id = u.id AND r.project_id = p.id AND pup.people_id = u.id"
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, "r.created_on", fields,
                               tables, filters, evolutionary)
        return(q)

    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople

        # Unique identities not supported yet

        filter_bots = ''
        bots = ReleasesDS.get_bots()
        for bot in bots:
            filter_bots = filter_bots + " username<>'"+bot+"' and "
        # filter_bots = ''

        # fields = "COUNT(r.id) as releases, username, u.id"
        fields = "COUNT(r.id) as releases, pup.uuid AS id, username"
        tables = "users u, releases r, projects p, people_uidentities pup"
        filters = filter_bots + "pup.people_id = u.id AND r.author_id = u.id AND r.project_id = p.id"
        if (days > 0):
            tables += ", (SELECT MAX(r.created_on) as last_date from releases r) t"
            filters += " AND DATEDIFF (last_date, r.created_on) < %s" % (days)
        filters += " AND r.created_on >= %s and r.created_on < %s " % (startdate, enddate)
        filters += " GROUP by username"
        filters += " ORDER BY releases DESC, r.name"
        filters += " LIMIT %s" % (limit)

        q = "SELECT %s FROM %s WHERE %s" % (fields, tables, filters)
        data = self.db.ExecuteQuery(q)
#        for id in data:
#            if not isinstance(data[id], (list)): data[id] = [data[id]]
        return(data)
