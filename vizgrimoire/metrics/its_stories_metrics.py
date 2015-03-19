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
##   Alvaro del Castillo <acs@bitergia.com>

""" Metrics for stories in Story Board (and others using this table) """

import logging
import MySQLdb

from vizgrimoire.metrics.metrics import Metrics

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.metrics.query_builder import ITSQuery

from vizgrimoire.ITS import ITS

from sets import Set

class StoriesOpened(Metrics):
    """ Stories Opened metric class for issue tracking systems """

    id = "stories_opened"
    name = "Opened stories"
    desc = "Number of opened stories"
    envision =  {"y_labels" : "true", "show_markers" : "true"}
    data_source = ITS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(s.story_id)) as stories_opened")

        tables.add("stories s")
        # tables.union_update(self.db.GetSQLReportFrom(self.filters))

        # filters.union_update(self.db.GetSQLReportWhere(self.filters, "stories"))

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " created_at ", fields,
                               tables, filters, evolutionary,
                               self.filters.type_analysis)
        return q


class StoriesOpeners(Metrics):
    """ Stories Openers metric class for issue tracking systems """

    id = "stories_openers"
    name = "Stories submitters"
    desc = "Number of persons submitting new stories"
    desc = "Number of opened stories"
    data_source = ITS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(s.creator_id)) as stories_openers")

        tables.add("stories s")
        # tables.union_update(self.db.GetSQLReportFrom(self.filters))

        # filters.union_update(self.db.GetSQLReportWhere(self.filters, "stories"))

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " created_at ", fields,
                               tables, filters, evolutionary,
                               self.filters.type_analysis)
        return q

    # TODO: once stories are integrated in ITSQuery, moved this logic there
    def GetTablesOwnUniqueIds (self, table='') :
        tables = Set([])
        # TODO: The acronym "c" is already used for organizations
        tables.add("stories sto")
        tables.add("people peo")
        tables.add("people_uidentities pup")
        tables.add(self.db.identities_db+".uidentities up")

        return (tables)

    def GetFiltersOwnUniqueIds (self, table='') :
        filters = Set([])
        filters.add("pup.people_id = peo.id")
        filters.add("sto.creator_id = peo.user_id")
        filters.add("pup.uuid = up.id")

        return (filters)


    def _get_top_global(self, days = 0, metric_filters = None):

        if metric_filters == None:
            metric_filters = self.filters

        tables_set = self.GetTablesOwnUniqueIds("stories")
        filters_set = self.GetFiltersOwnUniqueIds("stories")
        tables = self.db._get_fields_query(tables_set)
        filters = self.db._get_filters_query(filters_set)

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = ''
        # filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        # if filter_bots != "": filter_bots += " AND "

        dtables = dfilters = " "
        if (days > 0):
            dtables = " , (SELECT MAX(updated_at) as last_date from stories) t "
            dfilters = " AND DATEDIFF (last_date, updated_at) < %s " % (days)

        q = "SELECT up.uuid as id, up.identifier as stories_openers, "+\
            "    count(distinct(sto.story_id)) as stories_opened "+\
            "FROM " + tables + dtables + \
            "WHERE "+filter_bots + filters +" and "+\
            "    sto.created_at >= "+ startdate+ " and "+\
            "    sto.created_at < "+ enddate + dfilters +\
            "    GROUP BY up.identifier "+\
            "    ORDER BY stories_opened desc, stories_openers "+\
            "    LIMIT " + str(limit)

        data = self.db.ExecuteQuery(q)
        return (data)

class StoriesClosed(Metrics):
    """ Stories Closed metric class for issue tracking systems """

    id = "stories_closed"
    name = "Closed stories"
    desc = "Number of closed stories"
    data_source = ITS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(s.story_id)) as stories_closed")

        tables.add("stories s")

        filters.add("(status = 'merged' or status = 'invalid')")
        # tables.union_update(self.db.GetSQLReportFrom(self.filters))

        # filters.union_update(self.db.GetSQLReportWhere(self.filters, "stories"))

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " created_at ", fields,
                               tables, filters, evolutionary,
                               self.filters.type_analysis)
        return q



class StoriesPending(Metrics):
    id = "stories_pending"
    name = "Pending stories"
    desc = "Number of pending stories"
    data_source = ITS

    def _get_metrics_for_pending(self):

        # We need to fix the same filter for all metrics
        metrics_for_pendig = {}

        metric = StoriesOpened(self.db, self.filters)
        metric.filters = self.filters
        metrics_for_pendig['stories_opened'] = metric

        metric = StoriesClosed(self.db, self.filters)
        metric.filters = self.filters
        metrics_for_pendig['stories_closed'] = metric

        return metrics_for_pendig

    def get_agg(self):
        metrics = self._get_metrics_for_pending()
        opened = metrics['stories_opened'].get_agg()
        closed = metrics['stories_closed'].get_agg()

        # GROUP BY queries
        if self.filters.type_analysis is not None and self.filters.type_analysis[1] is None:
            pending = self.get_agg_all()
        else:
            pending = opened['stories_opened']-closed['stories_closed']
            pending = {"stories_pending":pending}
        return pending

    def get_ts(self):
        metrics = self._get_metrics_for_pending()
        opened = metrics["stories_opened"].get_ts()
        closed = metrics["stories_closed"].get_ts()
        evol = dict(opened.items() + closed.items())
        pending = {"stories_pending":[]}
            # GROUP BY queries
        if self.filters.type_analysis is not None and self.filters.type_analysis[1] is None:
            pending = self.get_ts_all()
        else:
            for i in range(0, len(evol['stories_opened'])):
                pending_val = evol["stories_opened"][i] - evol["stories_closed"][i]
                pending["stories_pending"].append(pending_val)
            pending[self.filters.period] = evol[self.filters.period]
        return pending

class People(Metrics):
    """ Stories People metric class for issue tracking systems """
    id = "stories_people"
    name = "Stories people"
    desc = "People working in stories"
    data_source = ITS

    def _get_top_global (self, days = 0, metric_filters = None):
        """ Implemented using Openers """
        top = None
        openers = ITS.get_metrics("stories_openers", ITS)
        if openers is None:
            openers = StoriesOpeners(self.db, self.filters)
            top = openers._get_top(days, metric_filters)
        else:
            afilters = openers.filters
            openers.filters = self.filters
            top = openers._get_top(days, metric_filters)
            openers.filters = afilters

        top['name'] = top.pop('openers')
        return top
