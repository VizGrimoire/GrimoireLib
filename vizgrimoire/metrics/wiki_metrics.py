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

from query_builder import MLSQuery

from Mediawiki import Mediawiki

class Reviews(Metrics):
    """ Reviews done in the Wiki """

    id = "reviews"
    name = "Reviews"
    desc = "Reviews done in the Wiki (editions)"
    data_source = Mediawiki

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(rev_id)) as reviews "
        tables = " wiki_pages_revs " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "date", fields,
                                   tables, filters, evolutionary)
        return q

class Pages(Metrics):
    """ Pages created in the Wiki """

    id = "pages"
    name = "Pages"
    desc = "Pages created in the Wiki"
    data_source = Mediawiki

    def __get_sql__ (self, evolutionary):
        fields = "COUNT(page_id) as pages"
        tables = " ( "+\
                "SELECT wiki_pages.page_id, MIN(date) as date FROM wiki_pages, wiki_pages_revs "+\
                "WHERE wiki_pages.page_id=wiki_pages_revs.page_id  "+\
                "GROUP BY wiki_pages.page_id) t "
        filters = ''

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "date", fields,
                                   tables, filters, evolutionary)
        return q


class Authors(Metrics):
    """ People editing the Wiki """
    # Threads class is not needed here. This is thanks to the use of the
    # field is_reponse_of.

    id = "authors"
    name = "Authors"
    desc = "People editing the Wiki"
    data_source = Mediawiki

    def __get_sql__ (self, evolutionary):
        fields = " count(distinct(user)) as authors "
        tables = " wiki_pages_revs " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "date", fields,
                                   tables, filters, evolutionary)
        return(q)