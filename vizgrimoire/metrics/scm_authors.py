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

from metric import MetricDomain

from GrimoireUtils import completePeriodIds

from metrics_filter import MetricFilters

from query_builder import SCMQuery


class Author(MetricDomain):
    """ Authors metric class for source code management systems """

    def __init__(self, dbcon, filters):
        self.db = dbcon
        self.filters = filters
        self.id = "authors"
        self.name = "Authors"
        self.desc = "People authoring commits (changes to source code)"
        self.data_source = "SCM"

    def __get_authors__ (self, evolutionary):
        # This function contains basic parts of the query to count authors
        # That query is later built and executed

        fields = " count(distinct(pup.upeople_id)) AS authors "
        tables = " scmlog s "
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2):
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ",  "+self.filters.identities_db+".people_upeople pup"
            filters += " and s.author_id = pup.people_id"

        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ",  "+self.filters.identities_db+".people_upeople pup"
            filters += " and s.author_id = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate, 
                               self.filters.enddate, " s.date ", fields, 
                               tables, filters, evolutionary)
        print q

        return(self.db.ExecuteQuery(q))


    def get_ts (self):
        # returns the evolution of authors through the time
        data = self.__get_authors__(True)
        return completePeriodIds(data, self.filters.period, self.filters.startdate, self.filters.enddate)


    def get_agg (self):
        # returns the aggregated number of authors in the specified timeperiod (enddate - startdate)
        return (self.__get_authors__(False))

    def get_list(self):
        #to be implemented
        pass

#example of use

filters = MetricFilters("week", "'2010-01-01'", "'2014-01-01'", ["company", "'Red Hat'"])
dbcon = SCMQuery("dic_cvsanaly_openstack_2259", "root", "", "dic_cvsanaly_openstack_2259")
redhat = Author(dbcon, filters)
print redhat.get_ts()
print redhat.get_agg()
print redhat.get_data_source()

