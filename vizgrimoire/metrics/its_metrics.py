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

""" Metrics for the issue tracking system """

import logging
import MySQLdb

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

class Openers(Metrics):
    """ Tickets Openers metric class for issue tracking systems """

    id = "openers"
    name = "Ticket submitters"
    desc = "Number of persons submitting new tickets"
    action = "opened"
    envision =  {"gtype" : "whiskers"}
    data_source = ITS

    def __get_sql_trk_prj__(self, evolutionary):

        tpeople_sql  = "SELECT  distinct(submitted_by) as submitted_by, submitted_on  "
        tpeople_sql += " FROM issues i " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            tpeople_sql += " WHERE " + filters_ext


        fields = " count(distinct(upeople_id)) as openers "
        tables = " people_upeople pup, (%s) tpeople " % (tpeople_sql)
        filters = " tpeople.submitted_by = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " tpeople.submitted_on ",
                               fields, tables, filters, evolutionary)
        return q


    def __get_sql_default__(self, evolutionary):
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

    def __get_sql__(self, evolutionary):
        if (self.filters.type_analysis is not None and (self.filters.type_analysis[0] in  ["repository","project"])):
            return self.__get_sql_trk_prj__(evolutionary)
        else:
            return self.__get_sql_default__(evolutionary)


#closed
class Closed(Metrics):
    """ Tickets Closed metric class for issue tracking systems """
    id = "closed"
    name = "Ticket closed"
    desc = "Number of closed tickets"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        """ Implemented using Changed """
        close = True
        changed = ITS.get_metrics("changed", ITS)
        cfilters = changed.filters
        changed.filters = self.filters
        q = changed.__get_sql__(evolutionary, close)
        changed.filters = cfilters
        return q

#closers
class Closers(Metrics):
    """ Tickets Closers metric class for issue tracking systems """
    id = "closers"
    name = "Tickets closers"
    desc = "Number of persons closing tickets"
    data_source = ITS
    envision = {"gtype" : "whiskers"}

    def __get_sql__(self, evolutionary):
        """ Implemented using Changers """
        close = True
        changers = ITS.get_metrics("changers", ITS)
        cfilters = changers.filters
        changers.filters = self.filters
        q = changers.__get_sql__(evolutionary, close)
        changers.filters = cfilters
        return q

class Changed(Metrics):
    """ Tickets Changed metric class for issue tracking systems. Also supports closed metric. """
    id = "changed"
    name = "Tickets changed"
    desc = "Number of changes to the state of tickets"
    data_source = ITS

    def __get_sql_trk_prj__(self, evolutionary, close = False):
        """ First get the issues filtered and then join with changes. Optimization for projects and trackers """

        issues_sql = "SELECT  i.id as id "
        issues_sql += "FROM issues i " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            issues_sql += " WHERE " + filters_ext
                    #Action needed to replace issues filters by changes one
        issues_sql = issues_sql.replace("i.submitted", "ch.changed")


        # closed_condition =  ITS._get_closed_condition()
        fields = " count(distinct(t.id)) as changed "
        tables = " changes ch LEFT JOIN (%s) t ON t.id = ch.issue_id" % (issues_sql)

        filters = ""
        if close:
            closed_condition =  ITS._get_closed_condition()
            fields = " count(distinct(t.id)) as closed "
            filters += closed_condition

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary)
        return q


    def __get_sql_default__(self, evolutionary, close = False):
        """ Default SQL for changed. Valid for all filters """
        fields = " count(distinct(i.id)) as changed "
        tables = " issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters = " i.id = ch.issue_id "

        if close:
            closed_condition =  ITS._get_closed_condition()
            fields = " count(distinct(i.id)) as closed "
            filters += " AND " + closed_condition

        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            filters += " and " + filters_ext
        #Action needed to replace issues filters by changes one
        filters = filters.replace("i.submitted", "ch.changed")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary)
        return q

    def __get_sql__(self, evolutionary, close = False):
        if (self.filters.type_analysis is not None and (self.filters.type_analysis[0] in  ["repository","project"])):
            return self.__get_sql_trk_prj__(evolutionary, close)
        else:
            return self.__get_sql_default__(evolutionary, close)

class Changers(Metrics):
    """ Tickets Changers metric class for issue tracking systems """
    id = "changers"
    name = "Tickets changers"
    desc = "Number of persons changing the state of tickets"
    data_source = ITS

    def __get_sql_trk_prj__(self, evolutionary, close = False):
        # First get changers and then join with people_upeople

        tpeople_sql  = "SELECT  distinct(changed_by) as cpeople, changed_on  "
        tpeople_sql += " FROM issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        tpeople_sql += " WHERE i.id = ch.issue_id "
        if close:
            closed_condition =  ITS._get_closed_condition()
            tpeople_sql += " AND " + closed_condition


        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            tpeople_sql += " and " + filters_ext


        fields = " count(distinct(upeople_id)) as changers "
        tables = " people_upeople, (%s) tpeople " % (tpeople_sql)
        filters = " tpeople.cpeople = people_upeople.people_id "
        if close:
            fields = " count(distinct(upeople_id)) as closers "


        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " tpeople.changed_on ",
                               fields, tables, filters, evolutionary)
        return q


    def __get_sql_default__(self, evolutionary, close = False):
        # closed_condition =  ITS._get_closed_condition()

        fields = " count(distinct(pup.upeople_id)) as changers "
        tables = " issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis) 
        if close:
            fields = " count(distinct(pup.upeople_id)) as closers "
            closed_condition =  ITS._get_closed_condition()
            filters += " AND " + closed_condition
        filters = " i.id = ch.issue_id and "
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            filters += " and " + filters_ext
        #unique identities filters
        if (self.filters.type_analysis is None or len(self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id"
        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id "

        #Action needed to replace issues filters by changes one
        filters = filters.replace("i.submitted", "ch.changed")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary)
        return q

    def __get_sql__(self, evolutionary, close = False):
        if (self.filters.type_analysis is not None and (self.filters.type_analysis[0] in  ["repository","project"])):
            return self.__get_sql_trk_prj__(evolutionary, close)
        else:
            return self.__get_sql_default__(evolutionary, close)

    def __get_sql_old__(self, evolutionary):
        #This function returns the evolution or agg number of changed issues
        #This function can be also reproduced using the Backlog function.
        #However this function is less time expensive.
        fields = " count(distinct(pup.upeople_id)) as changers "
        tables = " issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)

        filters = " i.id = ch.issue_id "
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            filters += " and " + filters_ext

        #unique identities filters
        if (self.filters.type_analysis is None or len(self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id"

        elif (self.filters.type_analysis[0] == "repository"or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id "

        #Action needed to replace issues filters by changes one
        filters = filters.replace("i.submitted", "ch.changed")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary)

        return q

class Trackers(Metrics):
    """ Trackers metric class for issue tracking systems """
    id = "trackers"
    name = "Trackers"
    desc = "Number of active trackers"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        fields = " COUNT(DISTINCT(tracker_id)) AS trackers  "
        tables = " issues i " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " i.submitted_on ",
                               fields, tables, filters, evolutionary)

        return q

class Companies(Metrics):
    """ Companies metric class for issue tracking systems """
    id = "companies"
    name = "Organizations"
    desc = "Number of organizations (companies, etc.) with persons active in the ticketing system"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        q = self.db.GetSQLIssuesStudies(self.filters.period, self.filters.startdate, 
                                           self.filters.enddate, self.db.identities_db, 
                                           ['company', ''], evolutionary, 'companies')
        return q

class Countries(Metrics):
    """ Countries metric class for issue tracking systems """
    id = "countries"
    name = "Countries"
    desc = "Number of countries with persons active in the ticketing system"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        q = self.db.GetSQLIssuesStudies(self.filters.period, self.filters.startdate, 
                                           self.filters.enddate, self.db.identities_db, 
                                           ['country', ''], evolutionary, 'countries')
        return q

class Domains(Metrics):
    """ Domains metric class for issue tracking systems """
    id = "domains"
    name = "Domains"
    desc = "Number of distinct email domains with persons active in the ticketing system"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        q = self.db.GetSQLIssuesStudies(self.filters.period, self.filters.startdate, 
                                           self.filters.enddate, self.db.identities_db, 
                                           ['domain', ''], evolutionary, 'domains')
        return q

class Projects(Metrics):
    """ Projects metric class for issue tracking systems """
    id = "projects"
    name = "Projects"
    desc = "Number of distinct projects active in the ticketing system"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        # Not yet working
        return None
        q = self.db.GetSQLIssuesStudies(self.filters.period, self.filters.startdate, 
                                           self.filters.enddate, self.db.identities_db, 
                                           ['project', ''], evolutionary, 'projects')
        return q

# Only agg metrics
class AllParticipants(Metrics):
    """ Total number of participants metric class for issue tracking systems """
    id = "allhistory_participants"
    name = "All Participants"
    desc = "Number of participants in all history in the ticketing system"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        q = "SELECT count(distinct(pup.upeople_id)) as allhistory_participants from people_upeople pup"
        return q

    def get_ts (self):
        """ It is an aggregate only metric """
        return {}

    def get_agg(self):
        if self.filters.type_analysis is None:
            query = self.__get_sql__(False)
            return self.db.ExecuteQuery(query)
        else: return {}
