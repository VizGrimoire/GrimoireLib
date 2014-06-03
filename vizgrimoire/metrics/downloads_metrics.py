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

from query_builder import DownloadsQuery

from Downloads import Downloads


class Downloads(Metrics):
    """ Number of downloads """

    id = "downloads"
    name = "Downloads"
    desc = "Number of total downloads"
    data_source = Downloads

    def __get_sql__(self, evolutionary):
        fields = "count(*) as downloads"
        tables = "downloads"
        filters = ""

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate, 
                                      self.filters.enddate, " date ", fields, tables, 
                                      filters, evolutionary)
        return query


