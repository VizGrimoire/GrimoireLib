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

""" Opened metric for the issue tracking system """

import logging
import MySQLdb

from GrimoireUtils import completePeriodIds

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import ITSQuery

from ITS import ITS

class Opened(Metrics):
    """ Tickets Opened metric class for issue tracking systems """

    id = "opened"
    name = "Opened tickets"
    desc = "Number of opened tickets"
    envision =  {"y_labels" : "true", "show_markers" : "true"}
    data_source = ITS

    def __get_sql__(self, evolutionary):

        fields = " count(distinct(i.id)) as opened "
        tables = " issues i "+ self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " submitted_on ", fields,
                               tables, filters, evolutionary)
        return q

# Examples of use
if __name__ == '__main__':
    filters = MetricFilters("week", "'2010-01-01'", "'2014-01-01'", ["company", "'Red Hat'"])
    dbcon = ITSQuery("root", "", "cp_bicho_SingleProject", "cp_bicho_SingleProject",)
    redhat = Opened(dbcon, filters)
    all = Opened(dbcon)
    # print redhat.get_ts()
    print redhat.get_agg()
    print all.get_agg()