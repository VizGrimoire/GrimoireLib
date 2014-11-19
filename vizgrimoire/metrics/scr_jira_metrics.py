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
##   Daniel Izquierdo <dizquierdo@bitergia.com>

"""This list of classes directly work with the database provided by Bicho.
   The specific schema is the one provided by Jira.
   
   So far, this list of classes need to work on a Jira database only with
   Pull Requests and not other type of issue.
"""

import logging
import MySQLdb
import numpy
from sets import Set

from GrimoireUtils import completePeriodIds, checkListArray, medianAndAvgByPeriod, check_array_values
from query_builder import DSQuery
from metrics import Metrics

from metrics_filter import MetricFilters
from query_builder import ITSQuery

from ITS import ITS

class PullRequests(Metrics):
    """This class calculates the number of pull requests (submitted reviews) 

    This class is based on the database schema provided by Bicho when analyzing Jira.
    Jira provides support to manage code review process through the use of Pull Requests
    that are stored in the database. 

    This type of issues are special ones. Those are identified when there is a field with
    the specific literal "Git Pull Request". (field = 'Git Pull Request').

    Once this flag is activated, the activity in such issue is detected in the following
    as a code review submission.

    This class needs to work on a database only containing Pull Requests.
    """

    id = "jira_pull_requests"
    name = "Submitted reviews"
    desc = "Number of submitted reviews"
    data_source = ITS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(i.id)) as submitted")

        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters.type_analysis))

        filters.union_update(self.db.GetSQLReportWhere(self.filters.type_analysis, "issues"))

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " submitted_on ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        return q
        

class Submitters(Metrics):
    """ Submitters are those that initially send the pull request.
        
        Those are identified as the developers that opened the ticket in the Jira system.
        Another approach would be to use the developer that added the pull request 
        to the system. Although this is not taking into account in this analysis

    """

    id = "jira_submitters_pull_requests"
    name = "Jira submitters pull requests"
    desc = "Jira developers submitting pull requests"
    data_source = ITS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(pup.upeople_id)) as submitters")

        tables.add("issues i")
        tables.add("people_upeople pup")
        tables.union_update(self.db.GetSQLReportFrom(self.filters.type_analysis))

        filters.union_update(self.db.GetSQLReportWhere(self.filters.type_analysis, "issues"))
        filters.add("i.submitted_by = pup.people_id")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " submitted_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        return query

class Abandoned(Metrics):
    """ An abandoned pull request is found when this was 'Closed' and not 'Fixed'

    """

    id = "jira_abandoned_pull_requests"
    name = "Jira abandoned pull requests"
    desc = "Jira pull requests that were abandoned at some point"
    data_source = ITS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(i.id)) as abandoned")

        tables.add("issues i")
        tables.add("changes ch")
        tables.union_update(self.db.GetSQLReportFrom(self.filters.type_analysis))

        filters.union_update(self.db.GetSQLReportWhere(self.filters.type_analysis, "issues"))
        filters.add("ch.issue_id = i.id")
        filters.add("i.status = 'Closed'")
        filters.add("ch.field = 'Resolution'")
        filters.add("ch.new_value <> 'Fixed'") 
        filters.add("ch.new_value <> 'Fixed in LPS'")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        return query



class Merged(Metrics):
    """ A merged pull request is detected when the final status of the issues is 'Closed' and
        the resolution is 'Fixed'.

    """

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(i.id)) as merged")

        tables.add("issues i")
        tables.add("changes ch")
        tables.union_update(self.db.GetSQLReportFrom(self.filters.type_analysis))

        filters.union_update(self.db.GetSQLReportWhere(self.filters.type_analysis, "issues"))
        filters.add("ch.issue_id = i.id")
        filters.add("i.status = 'Closed'")
        filters.add("ch.field = 'Resolution'")
        filters.add("(ch.new_value <> 'Fixed' or ch.new_value <> 'Fixed in LPS')")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        print query
        return query

import its_metrics as its

class Trackers(its.Trackers):

    """ List of trackers
    """

    id = "jira_trackers"
    name = "Jira Trackers"
    desc = "Jira Trackers"
    data_source = ITS


if __name__ == '__main__':

    # PYTHONPATH=./:../../:../analysis/:../ python scr_jira_metrics.py
    filters = MetricFilters("month", "'2014-04-01'", "'2014-07-01'", [])
    dbcon = ITSQuery("root", "", "example")

    print "Pull requests"
    example = PullRequests(dbcon, filters)
    print example.get_agg()
    print example.get_ts()

    print "submitters"
    example = Submitters(dbcon, filters)
    print example.get_agg()
    print example.get_ts()

    print "abandoned"
    example = Abandoned(dbcon, filters)
    print example.get_agg()
    print example.get_ts()

    print "merged"
    example = Merged(dbcon, filters)
    print example.get_agg()
    print example.get_ts()

    print "trackers"
    example = Trackers(dbcon, filters)
    print example.get_agg()
    print example.get_ts()  

