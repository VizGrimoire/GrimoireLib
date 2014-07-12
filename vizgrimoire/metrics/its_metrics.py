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

    def _get_sql(self, evolutionary):

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
        """ First we get the submitters then join with unique identities """
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

    def _get_top_global(self, days = 0, metric_filters = None):

        if metric_filters == None:
            metric_filters = self.filters

        tables = self.db.GetTablesOwnUniqueIds("issues")
        filters = self.db.GetFiltersOwnUniqueIds("issues")

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        if filter_bots != "": filter_bots += " AND "

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(submitted_on) as last_date from issues) t "
            dfilters = " AND DATEDIFF (last_date, submitted_on) < %s " % (days)

        q = "SELECT u.id as id, u.identifier as openers, "+\
            "    count(distinct(i.id)) as opened "+\
            "FROM " +tables +\
            " ,   "+self.db.identities_db+".upeople u "+ dtables + \
            "WHERE "+filter_bots + filters +" and "+\
            "    pup.upeople_id = u.id and "+\
            "    i.submitted_on >= "+ startdate+ " and "+\
            "    i.submitted_on < "+ enddate + dfilters +\
            "    GROUP BY u.identifier "+\
            "    ORDER BY opened desc, openers "+\
            "    LIMIT " + str(limit)

        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_sql(self, evolutionary):
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

    def _get_sql(self, evolutionary):
        """ Implemented using Changed """
        close = True
        changed = ITS.get_metrics("changed", ITS)
        if changed is None:
            # We need to create changers metric
            changed = Changed(self.db, self.filters)
            q = changed._get_sql(evolutionary, close)
        else:
            cfilters = changed.filters
            changed.filters = self.filters
            q = changed._get_sql(evolutionary, close)
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

    def _get_top_company (self, metric_filters) :
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        company_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        closed_condition =  ITS._get_closed_condition()

        if filter_bots != '': filter_bots = " AND " + filter_bots

        q = "SELECT u.id as id, u.identifier as closers, "+\
            "       COUNT(DISTINCT(c.id)) as closed "+\
            "FROM "+self.db.GetTablesCompanies(self.db.identities_db)+", "+\
            "     "+self.db.identities_db+".companies com, "+\
            "     "+self.db.identities_db+".upeople u "+\
            "WHERE "+self.db.GetFiltersCompanies()+" AND " + closed_condition + " "+\
            "      AND pup.upeople_id = u.id "+\
            "      AND upc.company_id = com.id "+\
            "      AND com.name = "+ company_name +" "+\
            "      AND changed_on >= "+startdate+" AND changed_on < "+enddate+\
            "      " + filter_bots +\
            " GROUP BY u.identifier ORDER BY closed DESC, closers LIMIT " + str(limit)

        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_domain (self, metric_filters):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        domain_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        closed_condition =  ITS._get_closed_condition()
        if filter_bots != '': filter_bots = " AND " + filter_bots

        q = "SELECT u.id as id, u.identifier as closers, "+\
            "COUNT(DISTINCT(c.id)) as closed "+\
            "FROM "+self.db.GetTablesDomains(self.db.identities_db)+", "+\
            "     "+self.db.identities_db+".domains dom, "+\
            "     "+self.db.identities_db+".upeople u "+\
            "WHERE "+ self.db.GetFiltersDomains()+" AND "+closed_condition+" "+\
            "      AND pup.upeople_id = u.id "+\
            "      AND upd.domain_id = dom.id "+\
            "      AND dom.name = "+domain_name+" "+\
            "      AND changed_on >= "+startdate+" AND changed_on < " +enddate +\
            "      " + filter_bots +\
            " GROUP BY u.identifier ORDER BY closed DESC, closers LIMIT " + str(limit)

        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_repository (self, metric_filters):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        repo_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        closed_condition =  ITS._get_closed_condition()
        if filter_bots != '': filter_bots = " AND " + filter_bots

        q = "SELECT u.id as id, u.identifier as closers, "+\
            "COUNT(DISTINCT(i.id)) as closed "+\
            "FROM issues i, changes c, trackers t, people_upeople pup, " +\
            "     "+self.db.identities_db+".upeople u "+\
            "WHERE "+closed_condition+" "+\
            "      AND pup.upeople_id = u.id "+\
            "      AND c.changed_by = pup.people_id "+\
            "      AND c.issue_id = i.id "+\
            "      AND i.tracker_id = t.id "+\
            "      AND t.url = "+repo_name+" "+\
            "      AND changed_on >= "+startdate+" AND changed_on < " +enddate +\
            "      " + filter_bots +\
            " GROUP BY u.identifier ORDER BY closed DESC, closers LIMIT " + str(limit)

        data = self.db.ExecuteQuery(q)
        return (data)


    def _get_top(self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        tables = self.db.GetTablesOwnUniqueIds("changes")
        filters = self.db.GetFiltersOwnUniqueIds("changes")

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        if filter_bots != "": filter_bots += " AND "
        closed_condition =  ITS._get_closed_condition()

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(changed_on) as last_date from changes) t "
            dfilters = " AND DATEDIFF (last_date, changed_on) < %s " % (days)

        q = "SELECT u.id as id, u.identifier as closers, "+\
            "       count(distinct(c.id)) as closed "+\
            "FROM  "+tables+\
            ",     "+self.db.identities_db+".upeople u "+ dtables +\
            "WHERE "+filter_bots + filters + " and "+\
            "      c.changed_by = pup.people_id and "+\
            "      pup.upeople_id = u.id and "+\
            "      c.changed_on >= "+ startdate+ " and "+\
            "      c.changed_on < "+ enddate+ " and " +\
            "      " + closed_condition + " " + dfilters+ " "+\
            "GROUP BY u.identifier "+\
            "ORDER BY closed desc, closers "+\
            "LIMIT "+ str(limit)

        data = self.db.ExecuteQuery(q)

        if not isinstance(data['id'], list):
            data = {item: [data[item]] for item in data}

        return (data)

    def get_list(self, metric_filters = None, days = 0):
        alist = {}

        if metric_filters is not None:
            metric_filters_orig = self.filters
            self.filters = metric_filters

        if metric_filters.type_analysis and metric_filters.type_analysis is not None:
            if metric_filters.type_analysis[0] == "repository":
                alist = self._get_top_repository(metric_filters)
            if metric_filters.type_analysis[0] == "company":
                alist = self._get_top_company(metric_filters)
            if metric_filters.type_analysis[0] == "domain":
                alist = self._get_top_domain(metric_filters)
        else:
            alist = self._get_top(days)

        if metric_filters is not None: self.filters = metric_filters_orig
        return alist

    def _get_sql(self, evolutionary):
        """ Implemented using Changers (changed metric should exists first) """
        close = True
        changers = ITS.get_metrics("changers", ITS)
        if changers is None:
            # We need to create changers metric
            changers = Changers(self.db, self.filters)
            q = changers._get_sql(evolutionary, close)
        else:
            cfilters = changers.filters
            changers.filters = self.filters
            q = changers._get_sql(evolutionary, close)
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

    def _get_sql(self, evolutionary, close = False):
        if (self.filters.type_analysis is not None
            and len(self.filters.type_analysis) == 2
            and (self.filters.type_analysis[0] in  ["repository","project"])):
            q = self.__get_sql_trk_prj__(evolutionary, close)
        else:
            q = self.__get_sql_default__(evolutionary, close)
        return q

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
        filters = " i.id = ch.issue_id "
        if close:
            fields = " count(distinct(pup.upeople_id)) as closers "
            closed_condition =  ITS._get_closed_condition()
            filters += " AND " + closed_condition
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

    def _get_sql(self, evolutionary, close = False):
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

    def get_list(self):
        # List the url of each of the repositories analyzed
        # Those are order by the number of opened issues (desc order)
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        q = " SELECT t.url as name "+\
                   "   FROM issues i, "+\
                   "        trackers t "+\
                   "   WHERE i.tracker_id=t.id and "+\
                   "         i.submitted_on >= "+ startdate+ " and "+\
                   "         i.submitted_on < "+ enddate+\
                   "   GROUP BY t.url  "+\
                   "   ORDER BY count(distinct(i.id)) DESC "
        data = self.db.ExecuteQuery(q)
        return (data)


    def _get_sql(self, evolutionary):
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

    def get_list(self):
        from data_source import DataSource
        from filter import Filter
        bots = DataSource.get_filter_bots(Filter("company"))
        fbots = ''
        for bot in bots:
            fbots += " c.name<>'"+bot+"' and "
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        closed_condition = ITS._get_closed_condition()

        # list each of the companies analyzed
        # those are order by number of closed issues
        q = "select c.name "+\
            "from issues i, "+\
            "     changes ch, "+\
            "     people_upeople pup, "+\
            "     "+ self.db.identities_db+ ".upeople_companies upc, "+\
            "     "+ self.db.identities_db+ ".companies c "+\
            "where i.id = ch.issue_id and "+\
            "      ch.changed_by = pup.people_id and "+\
            "      pup.upeople_id = upc.upeople_id and "+\
            "      upc.company_id = c.id and "+\
            "      ch.changed_on >= "+ startdate+ " and "+\
            "      ch.changed_on < "+ enddate+" and "+\
            "      i.submitted_on >= upc.init and "+\
            "      i.submitted_on < upc.end and "+\
            "      "+ fbots  +\
                   closed_condition +\
            "      group by c.name  "+\
            "      order by count(distinct(i.id)) desc"

        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_sql(self, evolutionary):
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

    def _get_sql(self, evolutionary):
        q = self.db.GetSQLIssuesStudies(self.filters.period, self.filters.startdate, 
                                           self.filters.enddate, self.db.identities_db, 
                                           ['country', ''], evolutionary, 'countries')
        return q

    def get_list(self):
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        closed_condition = ITS._get_closed_condition()

        q = "select cou.name "+\
            "from issues i, "+\
            "     changes ch, "+\
            "     people_upeople pup, "+\
            "     "+ self.db.identities_db+ ".upeople_countries upc, "+\
            "     "+ self.db.identities_db+ ".countries cou "+\
            "where i.id = ch.issue_id and "+\
            "      ch.changed_by = pup.people_id and "+\
            "      pup.upeople_id = upc.upeople_id and "+\
            "      upc.country_id = cou.id and "+\
            "      ch.changed_on >= "+ startdate+ " and "+\
            "      ch.changed_on < "+ enddate+" and "+\
            "      "+ closed_condition+ " "+\
            "      group by cou.name  "+\
            "      order by count(distinct(i.id)) desc"

        data = self.db.ExecuteQuery(q)
        return (data)

class Domains(Metrics):
    """ Domains metric class for issue tracking systems """
    id = "domains"
    name = "Domains"
    desc = "Number of distinct email domains with persons active in the ticketing system"
    data_source = ITS

    def _get_sql(self, evolutionary):
        q = self.db.GetSQLIssuesStudies(self.filters.period, self.filters.startdate, 
                                           self.filters.enddate, self.db.identities_db, 
                                           ['domain', ''], evolutionary, 'domains')
        return q

    def get_list(self):
        from data_source import DataSource
        from filter import Filter
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        closed_condition = ITS._get_closed_condition()
        bots = DataSource.get_filter_bots(Filter("domain"))
        fbots = ''
        for bot in bots:
            fbots += " dom.name<>'"+bot+"' and "

        tables = self.db.GetTablesDomains(self.db.identities_db)
        tables += ","+self.db.identities_db+".domains dom"

        q = "SELECT dom.name "+\
            "FROM "+ tables + " "+\
            "WHERE " + self.db.GetFiltersDomains() +" AND "+\
            "       dom.id = upd.domain_id and "+\
            "       "+ fbots +" "+\
            "       c.changed_on >= "+ startdate+ " AND "+\
            "       c.changed_on < "+ enddate+ " AND "+\
            "       "+ closed_condition+" "+\
            "GROUP BY dom.name "+\
            "ORDER BY COUNT(DISTINCT(c.issue_id)) DESC LIMIT " + str(Metrics.domains_limit)
        data = self.db.ExecuteQuery(q)
        return (data)

class Projects(Metrics):
    """ Projects metric class for issue tracking systems """
    id = "projects"
    name = "Projects"
    desc = "Number of distinct projects active in the ticketing system"
    data_source = ITS

    def get_list (self):
        # Projects activity needs to include subprojects also
        logging.info ("Getting projects list for ITS")
        from metrics_filter import MetricFilters

        q = "SELECT p.id AS name FROM  %s.projects p" % (self.db.identities_db)
        projects = self.db.ExecuteQuery(q)
        data = []

        # Loop all projects getting reviews
        for project in projects['name']:
            type_analysis = ['project', project]

            period = None
            filter_com = MetricFilters(period, self.filters.startdate,
                                       self.filters.enddate, type_analysis)
            mclosed = ITS.get_metrics("closed", ITS)
            mclosed.filters = filter_com
            issues = mclosed.get_agg()

            issues = issues['closed']
            if (issues > 0):
                data.append([issues,project])

        # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
        from operator import itemgetter
        data_sort = sorted(data, key=itemgetter(0),reverse=True)
        names = [name[1] for name in data_sort]

        return({"name":names})

    def _get_sql(self, evolutionary):
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

    def _get_sql(self, evolutionary):
        q = "SELECT count(distinct(pup.upeople_id)) as allhistory_participants from people_upeople pup"
        return q

    def get_ts (self):
        """ It is an aggregate only metric """
        return {}

    def get_agg(self):
        if self.filters.type_analysis is None:
            query = self._get_sql(False)
            return self.db.ExecuteQuery(query)
        else: return {}
