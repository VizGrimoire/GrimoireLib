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

""" Authors metric for the source code management system """

import logging
import MySQLdb

import re, sys

from GrimoireUtils import completePeriodIds

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import SCMQuery

from SCM import SCM

class Commits(Metrics):
    """ Commits metric class for source code management systems """

    id = "commits"
    name = "Commits"
    desc = "Changes to the source code"
    data_source = SCM

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(s.id)) as commits "
        tables = " scmlog s, actions a " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "author") + " and s.id=a.commit_id "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate, 
                                   self.filters.enddate, " s.date ", fields, 
                                   tables, filters, evolutionary)
        return query

# Examples of use
if __name__ == '__main__':
    filters = MetricFilters("week", "'2010-01-01'", "'2014-01-01'", ["company", "'Red Hat'"])
    # dbcon = SCMQuery("dic_cvsanaly_openstack_2259", "root", "", "dic_cvsanaly_openstack_2259")
    dbcon = SCMQuery("root", "", "cp_cvsanaly_SingleProject", "cp_cvsanaly_SingleProject",)
    redhat = Commits(dbcon, filters)
    all = Commits(dbcon)
    print redhat.get_ts()
    print redhat.get_agg()
    print all.get_agg()
    print redhat.get_data_source()

