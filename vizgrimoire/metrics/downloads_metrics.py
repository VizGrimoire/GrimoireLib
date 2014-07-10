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

from GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import DownloadsDSQuery

from DownloadsDS import DownloadsDS


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

