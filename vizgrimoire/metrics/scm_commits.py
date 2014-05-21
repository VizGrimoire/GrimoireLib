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

class Commits(MetricDomain):
    """ Commits metric class for source code management systems """

    def __init__(self, db, filters):
        self.db = db
        self.filters = filters
        self.id = "commits"
        self.name = "Commits"
        self.desc = "Changes to the source code"
        self.data_source = "SCM"

    def __get_commits__ (self, evolutionary):
        # This function contains basic parts of the query to count commits.
        # That query is built and results returned.
        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        type_analysis = self.filters.type_analysis

        fields = " count(distinct(s.id)) as commits "
        tables = " scmlog s, actions a " + self.db.GetSQLReportFrom(type_analysis)
        filters = self.db.GetSQLReportWhere(type_analysis, "author") + " and s.id=a.commit_id "

        q = self.db.BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

        return self.db.ExecuteQuery(q)


    def commits_ts (self):
        # Returns the evolution of commits through the time

        return self.__get_commits__(True)
    
    def commits_agg(self):
        return self.__get_commits__(False)        

    def list_commits(self):
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

commits_redhat = Commits(openstack_db, redhat_filters)
print commits_redhat.commits_ts()
print commits_redhat.commits_agg()

rackspace_filters = Filter("month", "'2010-01-01'", "'2014-01-01'", ["company", "'Rackspace'"])
commits_rackspace = Commits(openstack_db, rackspace_filters)
print commits_rackspace.commits_ts()
print commits_rackspace.commits_agg()

# Changing filters
redhat_filters.period = "month"
commits_redhat.filters = redhat_filters
print commits_redhat.commits_ts()
print commits_redhat.commits_agg()


