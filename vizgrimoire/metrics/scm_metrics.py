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


class Authors(Metrics):
    """ Authors metric class for source code management systems """

    id = "authors"
    name = "Authors"
    desc = "People authoring commits (changes to source code)"
    data_source = SCM

    def __get_sql__ (self, evolutionary):
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
            tables += ",  "+self.db.identities_db+".people_upeople pup"
            filters += " and s.author_id = pup.people_id"

        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ",  "+self.db.identities_db+".people_upeople pup"
            filters += " and s.author_id = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q


class Committers(Metrics):
    """ Committers metric class for source code management system """

    id = "committers"
    name = "Committers"
    desc = "Number of developers committing (merging changes to source code)"
    data_source = SCM

    def __get_sql__(self, evolutionary):
        # This function contains basic parts of the query to count committers
        fields = 'count(distinct(pup.upeople_id)) AS committers '
        tables = "scmlog s "
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "committer")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += " ,  "+self.db.identities_db+".people_upeople pup "
            filters += " and s.committer_id = pup.people_id"

        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ",  "+self.db.identities_db+".people_upeople pup"
            filters += " and s.committer_id = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate, 
                               self.filters.enddate, " s.date ", fields, 
                               tables, filters, evolutionary)

        return q


class Files(Metrics):
    """ Files metric class for source code management system """

    id = "files"
    name = "Files"
    desc = "Number of files 'touched' (added, modified, removed, ) by at least one commit"
    data_source = SCM

    def __get_sql__(self, evolutionary):
        # This function contains basic parts of the query to count files
        fields = " count(distinct(a.file_id)) as files "
        tables = " scmlog s, actions a "
        filters = " a.commit_id = s.id "

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)       
 
        return q


class Lines(Metrics):
    """ Added and Removed lines for source code management system """

    id = "lines"
    name = "Lines"
    desc = "Number of added and/or removed lines"
    data_source = SCM

    def __get_sql__(self, evolutionary):
        # This function contains basic parts of the query to count added and removed lines
        fields = "sum(cl.added) as added_lines, sum(cl.removed) as removed_lines"
        tables = "scmlog s, commits_lines cl "
        filters = "cl.commit_id = s.id "

        # specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)

        return q

    def get_ts(self):
        #Specific needs for Added and Removed lines not considered in meta class Metrics
        query = self.__get_sql__(True)
        data = self.db.ExecuteQuery(query)

        if not (isinstance(data['removed_lines'], list)): data['removed_lines'] = [data['removed_lines']]
        if not (isinstance(data['added_lines'], list)): data['added_lines'] = [data['added_lines']]

        data['removed_lines'] = [float(lines)  for lines in data['removed_lines']]
        data['added_lines'] = [float(lines)  for lines in data['added_lines']]

        return completePeriodIds(data, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)


