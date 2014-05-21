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


class Filter(object):

    """ Specific filters for each analysis """
    def __init__(self, period, startdate, enddate, type_analysis):
        self.period = period
        self.startdate = startdate
        self.enddate = enddate
        self.type_analysis = type_analysis

class Author(MetricDomain):
    """ Authors metric class for source code management systems """

    def __init__(self, db, filters):
        self.db = db
        self.filters = filters
        self.id = "authors"
        self.name = "Authors"
        self.desc = "People authoring commits (changes to source code)"
        self.data_source = "SCM"

    def __get_authors__ (self, evolutionary):
        # This function contains basic parts of the query to count authors
        # That query is later built and executed
        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        type_analysis = self.filters.type_analysis

        fields = " count(distinct(pup.upeople_id)) AS authors "
        tables = " scmlog s "
        filters = self.db.GetSQLReportWhere(type_analysis, "author")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(type_analysis)

        if (type_analysis is None or len (type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ",  "+identities_db+".people_upeople pup"
            filters += " and s.author_id = pup.people_id"

        elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ",  "+identities_db+".people_upeople pup"
            filters += " and s.author_id = pup.people_id "

        q = self.db.BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)
        print q

        return(self.db.ExecuteQuery(q))


    def authors_ts (self):
        # returns the evolution of authors through the time
        return (self.__get_authors__(True))


    def authors_agg (self):
        # returns the aggregated number of authors in the specified timeperiod (enddate - startdate)
        return (self.__get_authors__(False))

    def top_authors(self):
        #to be implemented
        pass

#################

#Example of use

from query_builder import SCMQuery

user = "root"
password = ""
database = "dic_cvsanaly_openstack_2259"
identities_db = "dic_cvsanaly_openstack_2259"

openstack_db = SCMQuery(identities_db, user, password, database)
redhat_filters = Filter("week", "'2010-01-01'", "'2014-01-01'", ["company", "'Red Hat'"])

authors_redhat = Author(openstack_db, redhat_filters)
print authors_redhat.authors_ts()
print authors_redhat.authors_agg()

rackspace_filters = Filter("month", "'2010-01-01'", "'2014-01-01'", ["company", "'Rackspace'"])
authors_rackspace = Author(openstack_db, rackspace_filters)
print authors_rackspace.authors_ts()
print authors_rackspace.authors_agg()

# Changing filters
redhat_filters.period = "month"
authors_redhat.filters = redhat_filters
print authors_redhat.authors_ts()
print authors_redhat.authors_agg()


