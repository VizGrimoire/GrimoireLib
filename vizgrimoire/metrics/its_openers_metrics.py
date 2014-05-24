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

""" Openers metric for the issue tracking system """

import logging
import MySQLdb

from GrimoireUtils import completePeriodIds

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import ITSQuery

from ITS import ITS

class Openers(Metrics):
    """ Tickets Openers metric class for issue tracking systems """

    id = "openers"
    name = "Ticket submitters"
    desc = "Number of persons submitting new tickets"
    action = "opened"
    envision =  {"gtype" : "whiskers"}
    data_source = ITS

    def __get_openers__ (self, evolutionary):
        # This function contains basic parts of the query to count openers tickets.
        # That query is built and results returned.
        query = self.__get_sql__(evolutionary)
        return self.db.ExecuteQuery(query)


    def __get_sql__(self, evolutionary):
        """ This function returns the evolution or agg number of people opening issues """
        fields = " count(distinct(pup.upeople_id)) as openers "
        tables = " issues i " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id"
        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " submitted_on ",
                               fields, tables, filters, evolutionary)
        return q

    def get_data_source(self):
        return self.data_source

    def get_ts (self):
        # Returns the evolution of commits through the time
        data = self.__get_openers__(True)
        return completePeriodIds(data, self.filters.period, self.filters.startdate, self.filters.enddate)

    def get_agg(self):
        return self.__get_openers__(False)

    def get_list(self):
        #to be implemented
        pass

# Examples of use
if __name__ == '__main__':
    filters = MetricFilters("week", "'2010-01-01'", "'2014-01-01'", ["company", "'Red Hat'"])
    dbcon = ITSQuery("root", "", "cp_bicho_SingleProject", "cp_bicho_SingleProject",)
    redhat = Openers(dbcon, filters)
    all = Openers(dbcon)
    # print redhat.get_ts()
    print redhat.get_agg()
    print all.get_agg()