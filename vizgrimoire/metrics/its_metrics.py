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

#closed
class Closed(Metrics):
    """ Tickets Closed metric class for issue tracking systems """
    id = "closed"
    name = "Ticket closed"
    desc = "Number of closed tickets"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        closed_condition =  ITS._get_closed_condition()
        fields = " count(distinct(i.id)) as closed "
        tables = " issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        filters = " i.id = ch.issue_id and " + closed_condition
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            filters += " and " + filters_ext
        #Action needed to replace issues filters by changes one
        filters = filters.replace("i.submitted", "ch.changed")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary)
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
        closed_condition =  ITS._get_closed_condition()

        fields = " count(distinct(pup.upeople_id)) as closers "
        tables = " issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)
        #closed condition filters
        filters = " i.id = ch.issue_id and " + closed_condition
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


class Changed(Metrics):
    """ Tickets Changed metric class for issue tracking systems """
    id = "changed"
    name = "Tickets changed"
    desc = "Number of changes to the state of tickets"
    data_source = ITS

    def __get_sql__(self, evolutionary):
        #This function returns the evolution or agg number of changed issues
        #This function can be also reproduced using the Backlog function.
        #However this function is less time expensive.
        fields = " count(distinct(ch.issue_id)) as changed "
        tables = " issues i, changes ch " + self.db.GetSQLReportFrom(self.db.identities_db, self.filters.type_analysis)

        filters = " i.id = ch.issue_id "
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis, self.db.identities_db)
        if (filters_ext != ""):
            filters += " and " + filters_ext

        #Action needed to replace issues filters by changes one
        filters = filters.replace("i.submitted", "ch.changed")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " ch.changed_on ",
                               fields, tables, filters, evolutionary)

        return q

class Changers(Metrics):
    """ Tickets Changers metric class for issue tracking systems """
    id = "changers"
    name = "Tickets changers"
    desc = "Number of persons changing the state of tickets"
    data_source = ITS

    def __get_sql__(self, evolutionary):
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