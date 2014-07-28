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
##   Alvaro del Castillo  <acs@bitergia.com>
##


import logging
import MySQLdb

import re, sys

from GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import SCMQuery

from SCM import SCM


class Commits(Metrics):
    """ Commits metric class for source code management systems """

    id = "commits"
    name = "Commits"
    desc = "Changes to the source code"
    envision = {"y_labels" : "true",
                "show_markers" : "true" }
    data_source = SCM

    def _get_sql(self, evolutionary):
        q_actions = " AND s.id IN (select distinct(a.commit_id) from actions a)"

        fields = " count(distinct(s.id)) as commits "
        tables = " scmlog s " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "author") + q_actions
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " s.date ", fields,
                                   tables, filters, evolutionary)
        return query

    def _get_sqlslow(self, evolutionary):
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
    envision = {"gtype" : "whiskers"}
    action = "commits"
    data_source = SCM

    def _get_sql (self, evolutionary):
        # This function contains basic parts of the query to count authors
        # That query is later built and executed

        fields = " count(distinct(pup.upeople_id)) AS authors "
        tables = " scmlog s "
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)

        # Hack to cover SCMQuery probs
        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2):
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ",  people_upeople pup"
            filters += " and s.author_id = pup.people_id"

        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            tables += ",  people_upeople pup"
            filters += " and s.author_id = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q


    def _get_top_repository (self, metric_filters = None, days = None):
        if metric_filters == None:
            metric_filters = self.filters
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        repo = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)

        repos_from = self.db.GetSQLRepositoriesFrom()
        # Remove first and
        repos_where = " WHERE  " + self.db.GetSQLRepositoriesWhere(repo)[4:]

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(date) as last_date from scmlog) dt"
            dfilters = " AND DATEDIFF (last_date, date) < %s " % (days)

        fields =  "SELECT COUNT(DISTINCT(s.id)) as commits, u.id, u.identifier as authors "
        fields += "FROM actions a, scmlog s, people_upeople pup, upeople u " + dtables
        q = fields + repos_from + repos_where
        q += dfilters
        if filter_bots != "": q += " AND "+ filter_bots
        q += " AND pup.people_id = s.author_id AND u.id = pup.upeople_id "
        q += " and s.id = a.commit_id "
        q += " AND s.date >= " + startdate + " and s.date < " + enddate
        q += " GROUP by u.id ORDER BY commits DESC, authors"
        q += " limit " + str(self.filters.npeople)

        res = self.db.ExecuteQuery(q)

        return res

    def _get_top_company (self, metric_filters = None, days = None):
        if metric_filters == None:
            metric_filters = self.filters
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        company = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        if filter_bots != '': filter_bots += " AND "

        q = """
        SELECT id, authors, count(logid) AS commits FROM (
        SELECT DISTINCT u.id AS id, u.identifier AS authors, s.id as logid
        FROM people p,  scmlog s,  actions a, people_upeople pup, upeople u,
             upeople_companies upc,  companies c
        WHERE  %s s.id = a.commit_id AND p.id = s.author_id AND s.author_id = pup.people_id  AND
          pup.upeople_id = upc.upeople_id AND pup.upeople_id = u.id AND  s.date >= upc.init AND
          s.date < upc.end AND upc.company_id = c.id AND
          s.date >=%s AND s.date < %s AND c.name =%s) t
        GROUP BY id
        ORDER BY commits DESC, authors
        LIMIT %s
        """ % (filter_bots, startdate, enddate, company, limit)

        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_project(self, metric_filters = None, days = None):
        if metric_filters == None:
            metric_filters = self.filters
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        project = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)

        projects_from = self.db.GetSQLProjectFrom()

        # Remove first and
        projects_where = " WHERE  " + self.db.GetSQLProjectWhere(project)[3:]

        fields =  "SELECT COUNT(DISTINCT(s.id)) as commits, u.id, u.identifier as authors "
        fields += "FROM actions a, scmlog s, people_upeople pup, upeople u "
        q = fields + projects_from + projects_where
        if filter_bots != "": q += " AND "+ filter_bots
        q += " AND pup.people_id = s.author_id AND u.id = pup.upeople_id "
        q += " and a.commit_id = s.id "
        q += " AND s.date >= " + startdate + " and s.date < " + enddate
        q += " GROUP by u.id ORDER BY commits DESC, authors"
        q += " limit " + str(self.filters.npeople)

        res = self.db.ExecuteQuery(q)

        return res

    def _get_top_global (self, days = 0, metric_filters = None, role = "author") :
        # This function returns the top people participating in the source code.
        # In addition, the number of days allows to limit the study to the last
        # X days specified in that parameter

        if metric_filters == None:
            metric_filters = self.filters
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        if filter_bots != "": filter_bots = "WHERE " + filter_bots

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(date) as last_date from scmlog) dt"
            dfilters = " DATEDIFF (last_date, date) < %s AND " % (days)

        q = """
        SELECT u.id, u.identifier as %ss, SUM(total) AS commits FROM
        (
         SELECT s.%s_id, COUNT(DISTINCT(s.id)) as total
         FROM scmlog s, actions a %s
         WHERE %s s.id = a.commit_id AND
            s.date >= %s AND  s.date < %s
         GROUP BY  s.%s_id ORDER by total DESC, s.%s_id
        ) t
        JOIN people_upeople pup ON pup.people_id = %s_id
        JOIN upeople u ON pup.upeople_id = u.id
        %s
        GROUP BY u.identifier ORDER BY commits desc, %ss  limit %s
        """ % (role, role, dtables, dfilters, startdate, enddate, role, role, role, filter_bots, role, limit)

        data = self.db.ExecuteQuery(q)
        for id in data:
            if not isinstance(data[id], (list)): data[id] = [data[id]]
        return (data)

    def _get_top_supported_filters(self):
        return ['repository','company','project']

class People(Metrics):
    """ People filter metric class for source code management systems """

    id = "people2" # people is used yet for all partial filter
    name = "People"
    desc = "People authoring commits (changes to source code)"
    envision = {"gtype" : "whiskers"}
    action = "commits"
    data_source = SCM

    def _get_sql (self, evolutionary):
        """ Implemented using Authors """
        authors = SCM.get_metrics("authors", SCM)
        if authors is None:
            authors = Authors(self.db, self.filters)
            q = authors._get_sql(evolutionary)
        else:
            afilters = authors.filters
            authors.filters = self.filters
            q = authors._get_sql(evolutionary)
            authors.filters = afilters
        return q

    def _get_top_global (self, days = 0, metric_filters = None):
        """ Implemented using Authors """
        top = None
        authors = SCM.get_metrics("authors", SCM)
        if authors is None:
            authors = Authors(self.db, self.filters)
            top = authors._get_top_global(days, metric_filters)
        else:
            afilters = authors.filters
            authors.filters = self.filters
            top = authors._get_top_global(days, metric_filters)
            authors.filters = afilters
        top['name'] = top.pop('authors')
        return top

class Committers(Metrics):
    """ Committers metric class for source code management system """

    id = "committers"
    name = "Committers"
    desc = "Number of developers committing (merging changes to source code)"
    envision = {"gtype" : "whiskers"}
    action = "commits"
    data_source = SCM

    def _get_sql(self, evolutionary):
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

    def _get_sql_filter_all(self, evolutionary):
        if self.filters.type_analysis is None:
            return None

        fields = "count(distinct(a.file_id)) as files "
        tables = " scmlog s, actions a "
        filters = " a.commit_id = s.id "

        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis)

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary, self.filters)
        print(q)

        return q


    def _get_sql(self, evolutionary):
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

    def _get_sql(self, evolutionary):
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
        query = self._get_sql(True)
        data = self.db.ExecuteQuery(query)

        if not (isinstance(data['removed_lines'], list)): data['removed_lines'] = [data['removed_lines']]
        if not (isinstance(data['added_lines'], list)): data['added_lines'] = [data['added_lines']]

        data['removed_lines'] = [float(lines)  for lines in data['removed_lines']]
        data['added_lines'] = [float(lines)  for lines in data['added_lines']]

        return completePeriodIds(data, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)

    def get_agg_diff_days(self, date, days):
        #Specific needs for Added and Removed lines not considered in meta class Metrics
        filters = self.filters

        chardates = GetDates(date, days)

        self.filters = MetricFilters(Metrics.default_period,
                                     chardates[1], chardates[0], None)        
        last = self.get_agg()
        last_added = int(last['added_lines'])
        last_removed = int(last['removed_lines'])

        self.filters = MetricFilters(Metrics.default_period,
                                     chardates[2], chardates[1], None)
        prev = self.get_agg()
        prev_added = int(prev['added_lines'])
        prev_removed = int(prev['removed_lines'])

        data = {}
        data['diff_netadded_lines_'+str(days)] = last_added - prev_added
        data['percentage_added_lines_'+str(days)] = GetPercentageDiff(prev_added, last_added)
        data['diff_netremoved_lines_'+str(days)] = last_removed - prev_removed
        data['percentage_removed_lines_'+str(days)] = GetPercentageDiff(prev_removed, last_removed)
        data['added_lines_'+str(days)] = last_added
        data['removed_lines_'+str(days)] = last_removed

        #Returning filters to their original value
        self.filters = filters
        return (data) 


class Branches(Metrics):
    """ Branches metric class for source code management system """

    id = "branches"
    name = "Branches"
    desc = "Number of active branches"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating branches
        fields = "count(distinct(a.branch_id)) as branches "
        tables = " scmlog s, actions a "
        filters = " a.commit_id = s.id "

        # specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q


class Actions(Metrics):
    """ Actions metrics class for source code management system """

    id = "actions"
    name = "Actions"
    desc = "Actions performed on several files (add, remove, copy, ... each file)"
    data_source = SCM

    def _get_sql (self, evolutionary):
        # Basic parts of the query needed when calculating actions
        fields = " count(distinct(a.id)) as actions "
        tables = " scmlog s, actions a "
        filters = " a.commit_id = s.id "

        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q

class CommitsPeriod(Metrics):
    """ Commits per period class for source code management system """

    id = "avg_commits"
    name = "Average Commits per period"
    desc = "Average number of commits per period"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating commits per period

        fields = " count(distinct(s.id))/timestampdiff("+self.filters.period+",min(s.date),max(s.date)) as avg_commits_"+self.filters.period
        tables = " scmlog s "
        filters = " s.id IN (SELECT DISTINCT(a.commit_id) from actions a) "

        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q

    def get_ts(self):
        # WARNING: This function should provide same information as Commits.get_ts(), do not use this.
        return {}


class FilesPeriod(Metrics):
    """ Files per period class for source code management system  """

    id = "avg_files"
    name = "Average Files per period"
    desc = "Average number of files per period"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating commits per period

        fields = " count(distinct(a.file_id))/timestampdiff("+self.filters.period+",min(s.date),max(s.date)) as avg_files_"+self.filters.period
        tables = " scmlog s, actions a "
        filters = " s.id = a.commit_id "

        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q

    def get_ts(self):
        # WARNING: This function should provide same information as Files.get_ts(), do not use this.
        return {}


class CommitsAuthor(Metrics):
    """ Commits per author class for source code management system """

    id = "avg_commits_author"
    name = "Average Commits per Author"
    desc = "Average number of commits per author"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating commits per author
  
        fields = " count(distinct(s.id))/count(distinct(pup.upeople_id)) as avg_commits_author "
        tables = " scmlog s, actions a "
        filters = " s.id = a.commit_id "

        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
 
        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
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


class AuthorsPeriod(Metrics):
    """ Authors per period class for source code management system """

    id = "avg_authors_period"
    name = "Average Authors per period"
    desc = "Average number of authors per period"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating commits per period

        fields = " count(distinct(pup.upeople_id))/timestampdiff("+self.filters.period+",min(s.date),max(s.date)) as avg_authors_"+self.filters.period
        tables = " scmlog s "
        # filters = ""

        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
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


    def get_ts(self):
        # WARNING, this function should return same information as Authors.get_ts(), do not use this
        return {}


class CommittersPeriod(Metrics):
    """ Committers per period class for source code management system """

    id = "avg_committers_period"
    name = "Average Committers per period"
    desc = "Average number of committers per period"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating commits per period

        #TODO: the following three lines should be initialize in a __init__ method.
        self.id = "avg_committers_" + self.filters.period
        self.name = "Average Committers per " + self.filters.period
        self.desc = "Average number of committers per " + self.filters.period

        fields = " count(distinct(pup.upeople_id))/timestampdiff("+self.filters.period+",min(s.date),max(s.date)) as avg_committers_"+self.filters.period
        tables = " scmlog s "
        # filters = ""

        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "committer")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ",  "+self.db.identities_db+".people_upeople pup"
            filters += " and s.committer_id = pup.people_id"

        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ",  "+self.db.identities_db+".people_upeople pup"
            filters += " and s.committer_id = pup.people_id "
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q

    def get_ts(self):
        # WARNING, this function should return same information as Committers.get_ts(), do not use this
        return {}


class FilesAuthor(Metrics):
    """ Files per author class for source code management system """

    id = "avg_files_author"
    name = "Average Files per Author"
    desc = "Average number of files per author"
    data_source = SCM

    def _get_sql(self, evolutionary):
        # Basic parts of the query needed when calculating files per author

        fields = " count(distinct(a.file_id))/count(distinct(pup.upeople_id)) as avg_files_author "
        tables = " scmlog s, actions a "
        filters = " s.id = a.commit_id "

        filters += self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        #specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
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

class Repositories(Metrics):
    """ Number of repositories in the source code management system """
    #TO BE REFACTORED

    id = "repositories"
    name = "Repositories"
    desc = "Number of repositories in the source code management system"
    envision = {"gtype" : "whiskers"}
    data_source = SCM

    def _get_sql(self, evolutionary):
        fields = "count(distinct(s.repository_id)) AS repositories "
        tables = "scmlog s "

        # specific parts of the query depending on the report needed
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, "author")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evolutionary)
        return q

    def get_list(self):
        """Repositories list ordered by number of commits"""
        q = """
            select count(distinct(sid)) as total, name
            from repositories r, (
              select distinct(s.id) as sid, repository_id from actions a, scmlog s
              where s.id = a.commit_id  and s.date >=%s and s.date < %s) t
            WHERE repository_id = r.id
            group by repository_id   
            order by total desc,name
            """ % (self.filters.startdate, self.filters.enddate)

        return self.db.ExecuteQuery(q)

class Companies(Metrics):
    """ Companies participating in the source code management system """
    #TO BE REFACTORED

    id = "companies"
    name = "Companies"
    desc = "Companies participating in the source code management system"
    data_source = SCM

    def _get_sql(self, evol):
        fields = "count(distinct(upc.company_id)) as companies"
        tables = " scmlog s, people_upeople pup, upeople_companies upc"
        filters = "s.author_id = pup.people_id and "+\
               "pup.upeople_id = upc.upeople_id and "+\
               "s.date >= upc.init and  "+\
               "s.date < upc.end"
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields, 
                               tables, filters, evol)
        return q

    def _get_top_project(self, fbots = None, days = None):
        if fbots is not None and fbots != '': fbots += " AND "

        startdate = self.filters.startdate
        enddate = self.filters.enddate
        project = self.filters.type_analysis[1]
        limit = self.filters.npeople

        projects_from = self.db.GetSQLProjectFrom()
        # Remove first and
        projects_where = " WHERE  " + self.db.GetSQLProjectWhere(project)[3:]

        fields =  "SELECT COUNT(DISTINCT(s.id)) as company_commits, c.name as companies "
        fields += "FROM scmlog s, people_upeople pup, upeople u, upeople_companies upc, companies c "
        q = fields + projects_from + projects_where
        q += " AND " + fbots + " pup.people_id = s.author_id AND u.id = pup.upeople_id "
        q += " AND u.id = upc.upeople_id AND c.id = upc.company_id "
        q += " AND s.date >= " + startdate + " and s.date < " + enddate
        q += " AND s.date >= upc.init AND s.date < upc.end "
        q += " GROUP by c.name ORDER BY company_commits DESC, c.name"
        q += " limit " + str(self.filters.npeople)

        return q

    def _get_top(self, fbots = None):
        if fbots is not None and fbots !='': fbots += " AND "
        q = """
            select c.name, count(distinct(t.s_id)) as total
            from companies c,  (
              select distinct(s.id) as s_id, company_id
              from companies c, people_upeople pup, upeople_companies upc,
                   scmlog s,  actions a
              where c.id = upc.company_id and  upc.upeople_id = pup.upeople_id
                and  s.date >= upc.init and s.date < upc.end
                and pup.people_id = s.author_id
                and s.id = a.commit_id and
                %s s.date >=%s and s.date < %s) t
            where c.id = t.company_id
            group by c.name
            order by count(distinct(t.s_id)) desc
        """ % (fbots, self.filters.startdate, self.filters.enddate)

        return q

    def get_list(self, metric_filters = None):
        from data_source import DataSource
        from filter import Filter
        # bots = DataSource.get_filter_bots(Filter("company"))

        if metric_filters == None:
            metric_filters = self.filters

        # Store current filter to restore it
        metric_filters_orig = self.filters
        items_out = self.get_items_out_filter_sql("company", metric_filters)

        if metric_filters is not None and metric_filters.type_analysis is not None:
            self.filters = metric_filters

            if metric_filters.type_analysis[0] == "project":
                q = self._get_top_project(items_out)

        else:
            q = self._get_top(items_out)

        # Restore original filter for the metric
        self.filters = metric_filters_orig

        return self.db.ExecuteQuery(q)

class Countries(Metrics):
    """ Countries participating in the source code management system """
    #TO BE REFACTORED

    id = "countries"
    name = "Countries"
    desc = "Countries participating in the source code management system"
    data_source = SCM

    def _get_sql(self, evol):
        fields = "count(distinct(upc.country_id)) as countries"
        tables = "scmlog s, people_upeople pup, upeople_countries upc"
        filters = "s.author_id = pup.people_id and pup.upeople_id = upc.upeople_id"
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evol)
        return q

    def get_list(self): 
        rol = "author" #committer
        identities_db = self.db.identities_db
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        q = "SELECT count(s.id) as commits, c.name as name "+\
            "FROM scmlog s,  "+\
            "     people_upeople pup, "+\
            "     "+identities_db+".countries c, "+\
            "     "+identities_db+".upeople_countries upc "+\
            "WHERE pup.people_id = s."+rol+"_id AND "+\
            "      pup.upeople_id  = upc.upeople_id and "+\
            "      upc.country_id = c.id and "+\
            "      s.date >="+startdate+ " and "+\
            "      s.date < "+enddate+ " "+\
            "group by c.name "+\
            "order by commits desc"

        return self.db.ExecuteQuery(q)

class Domains(Metrics):
    """ Domains participating in the source code management system """
    #TO BE REFACTORED

    id = "domains"
    name = "Domains"
    desc = "Domains participating in the source code management system"
    data_source = SCM

    def _get_sql(self, evol):
        fields = "COUNT(DISTINCT(upd.domain_id)) AS domains"
        tables = "scmlog s, people_upeople pup, upeople_domains upd"
        filters = "s.author_id = pup.people_id and pup.upeople_id = upd.upeople_id "
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " s.date ", fields,
                               tables, filters, evol)
        return q

    def get_list(self):
        rol = "author" #committer
        identities_db = self.db.identities_db
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        q = "SELECT count(s.id) as commits, d.name as name "+\
            "FROM scmlog s, "+\
            "  people_upeople pup, "+\
            "  "+identities_db+".domains d, "+\
            "  "+identities_db+".upeople_domains upd "+\
            "WHERE pup.people_id = s."+rol+"_id AND "+\
            "  pup.upeople_id  = upd.upeople_id and "+\
            "  upd.domain_id = d.id and "+\
            "  s.date >="+ startdate+ " and "+\
            "  s.date < "+ enddate+ " "+\
            "GROUP BY d.name "+\
            "ORDER BY commits desc  LIMIT " + str(Metrics.domains_limit)

        return self.db.ExecuteQuery(q)

class Projects(Metrics):
    """ Projects in the source code management system """
    #TO BE COMPLETED

    id = "projects"
    name = "Projects"
    desc = "Projects in the source code management system"
    data_source = SCM

    def get_list(self):
        from data_source import DataSource
        from metrics_filter import MetricFilters

        identities_db = self.db.identities_db
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        # Get all projects list
        q = "SELECT p.id AS name FROM  %s.projects p" % (identities_db)
        projects = self.db.ExecuteQuery(q)
        data = []

        # Loop all projects getting reviews
        for project in projects['name']:
            type_analysis = ['project', project]
            period = None
            evol = False
            mcommits = DataSource.get_metrics("commits", SCM)
            mfilter = MetricFilters(period, startdate, enddate, type_analysis)
            mfilter_orig = mcommits.filters
            mcommits.filters = mfilter
            commits = mcommits.get_agg()
            mcommits.filters = mfilter_orig
            commits = commits['commits']
            if (commits > 0):
                data.append([commits,project])

        # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
        from operator import itemgetter
        data_sort = sorted(data, key=itemgetter(0),reverse=True)
        names = [name[1] for name in data_sort]

        return({"name":names})


if __name__ == '__main__':
    filters = MetricFilters("week", "'2014-04-01'", "'2014-07-01'", ["project", "'nova'"])
    dbcon = SCMQuery("root", "", "dic_cvsanaly_openstack_2259_tm", "dic_cvsanaly_openstack_2259_tm",)
    os_sw = Companies(dbcon, filters)
    print os_sw.get_list()
