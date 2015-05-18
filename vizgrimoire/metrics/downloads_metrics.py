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

from vizgrimoire.metrics.query_builder import DownloadsDSQuery

from vizgrimoire.DownloadsDS import DownloadsDS

from sets import Set

class Downloads(Metrics):
    """ Number of downloads """

    id = "downloads"
    name = "Downloads"
    desc = "Number of total downloads"
    data_source = DownloadsDS

    def _get_sql(self, evolutionary):
        fields = "count(*) as downloads"
        tables = "downloads"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                      self.filters.enddate, " date ", fields, tables,
                                      filters, evolutionary)
        return query


class Packages(Metrics):
    """ Number of downloaded packages """

    id = "packages"
    name = "Packages"
    desc = "Number of downloaded packages"
    data_source = DownloadsDS

    def _get_sql(self, evolutionary):
        fields = "count(distinct(package)) as packages"
        tables = "downloads"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                      self.filters.enddate, " date ", fields, tables,
                                      filters, evolutionary)
        return query

    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople

        query = """
                select package as packages, count(*) as downloads
                from downloads
                where date >= %s and
                      date < %s
                group by packages
                order by downloads desc
                limit %s
                """ % (startdate, enddate, str(limit))
        return self.db.ExecuteQuery(query)

class Protocols(Metrics):
    """ Number of protocols used to download packages """

    id = "protocols"
    name = "Protocols"
    desc = "Number of protocols used to download packages """
    data_source = DownloadsDS

    def _get_sql(self, evolutionary):
        fields = "count(distinct(protocol)) as protocols"
        tables = "downloads"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                      self.filters.enddate, " date ", fields, tables,
                                      filters, evolutionary)
        return query


class IPs(Metrics):
    """ Number of IPs downloading packages """

    id = "ips"
    name = "IPs"
    desc = "Number of IPs downloading packages """
    data_source = DownloadsDS

    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople

        query = """
                select ip as ips, count(*) as downloads
                from downloads
                where date >= %s and
                      date < %s
                group by ips
                order by downloads desc
                limit %s
                """ % (startdate, enddate, str(limit))
        return self.db.ExecuteQuery(query)

    def _get_sql(self, evolutionary):
        fields = "count(distinct(ip)) as ips"
        tables = "downloads"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                      self.filters.enddate, " date ", fields, tables,
                                      filters, evolutionary)
        return query


class UniqueVisitors(Metrics):
    """Number of unique visitors """

    id = "uvisitors"
    name = "Unique Visitors"
    desc = "Number of unique visitors"
    data_source = DownloadsDS

    def get_agg(self):
        # We do not have this value yet
        return {'uvisitors' : 'null'}

    def get_ts(self):
        if self.filters.period != 'month':
            msg = 'Period %s not valid. Currently, only "month" is supported' % \
                self.filters.period
            return ValueError(msg)

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("unique_visitors uvisitors")
        tables.add("visits_month v")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "v.date",
                                   fields, tables, filters, True,
                                   self.filters.type_analysis)

        ts = self.db.ExecuteQuery(query)
        ts = completePeriodIds(ts, self.filters.period,
                               self.filters.startdate, self.filters.enddate)
        return ts


class Visits(Metrics):
    """Number of visits"""

    id = "visits"
    name = "Visits"
    desc = "Number of visits"
    data_source = DownloadsDS

    def _get_sql(self, evolutionary):
        if self.filters.period != 'month':
            msg = 'Period %s not valid. Currently, only "month" is supported' % \
                self.filters.period
            return ValueError(msg)

        fields = "SUM(visits) visits"
        tables = "visits_month v"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " date ", fields, tables,
                                   filters, evolutionary)
        return query


class Bounces(Metrics):
    """Number of bounces"""

    id = "bounces"
    name = "Bounces"
    desc = "Number of bounces"
    data_source = DownloadsDS

    def _get_sql(self, evolutionary):
        if self.filters.period != 'month':
            msg = 'Period %s not valid. Currently, only "month" is supported' % \
                self.filters.period
            return ValueError(msg)

        fields = "SUM(bounce) bounces"
        tables = "visits_month v"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " date ", fields, tables,
                                   filters, evolutionary)
        return query


class Pages(Metrics):
    """Most visited pages"""

    id = "pages"
    name = "Pages"
    desc = "Most visited pages"
    data_source = DownloadsDS


    def _get_sql_generic(self, evolutionary, islist=False):
        if self.filters.period != 'month':
            msg = 'Period %s not valid. Currently, only "month" is supported' % \
                self.filters.period
            return ValueError(msg)

        fields = Set([])
        tables = Set([])
        filters = Set([])

        if not islist:
            fields.add("count(distinct(p.page)) as pages")
        else:
            fields.add("p.page as page, SUM(p.visits) visits")
        tables.add("pages_month p")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " p.date ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        if islist:
            q += " GROUP BY p.page "
            if not evolutionary: q += " ORDER BY visits DESC, page"

        return q

    def _get_sql(self, evolutionary):
        return self._get_sql_generic(evolutionary)

    def get_list(self):
        q = self._get_sql_generic(None, True)
        data = self.db.ExecuteQuery(q)
        return data


class Countries(Metrics):
    """Most visitor countries"""

    id = "countries"
    name = "Countries"
    desc = "Most visitor countries"
    data_source = DownloadsDS


    def _get_sql_generic(self, evolutionary, islist=False):
        if self.filters.period != 'month':
            msg = 'Period %s not valid. Currently, only "month" is supported' % \
                self.filters.period
            return ValueError(msg)

        fields = Set([])
        tables = Set([])
        filters = Set([])

        if not islist:
            fields.add("count(distinct(c.country)) as countries")
        else:
            fields.add("c.country as country, SUM(c.visits) visits")
        tables.add("countries_month c")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " c.date ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        if islist:
            q += " GROUP BY c.country "
            if not evolutionary: q += " ORDER BY visits DESC, country"

        return q

    def _get_sql(self, evolutionary):
        return self._get_sql_generic(evolutionary)

    def get_list(self):
        q = self._get_sql_generic(None, True)
        data = self.db.ExecuteQuery(q)
        return data
