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
##   Alvaro del Castillo <acs@bitergia.com>
##


import logging

from datetime import datetime

from data_source import DataSource
from GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff
from GrimoireSQL import ExecuteQuery
from filter import Filter
from metrics import Metrics
from metrics_filter import MetricFilters

from MLS import MLS


class EmailsSent(Metrics):
    """ Emails metric class for mailing lists analysis """

    id = "sent"
    name = "Emails Sent"
    desc = "Emails sent to mailing lists"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = " count(distinct(m.message_ID)) as sent "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class EmailsSenders(Metrics):
    """ Emails Senders class for mailing list analysis """

    id = "senders"
    name = "Email Senders"
    desc = "People sending emails"
    data_source = MLS

    def _get_top_repository (self, metric_filters):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        repo = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        rfield = MLS.get_repo_field()

        q = "SELECT up.id as id, up.identifier as senders, "+\
                " COUNT(m.message_id) as sent "+\
                " FROM "+ self.db.GetTablesOwnUniqueIds()+ ","+self.db.identities_db+".upeople up "+\
                " WHERE "+ self.db.GetFiltersOwnUniqueIds()+ " AND "+\
                "  pup.upeople_id = up.id AND "+\
                "  m.first_date >= "+startdate+" AND "+\
                "  m.first_date < "+enddate+" AND "+\
                "  m."+rfield+"="+ repo +\
                " GROUP BY up.identifier "+\
                " ORDER BY sent desc, senders "+\
                " LIMIT " + str(limit)
        data = ExecuteQuery(q)
        return (data)

    def _get_top_country (self, metric_filters):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        country_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)

        q = "SELECT up.id as id, up.identifier as senders, "+\
            " COUNT(DISTINCT(m.message_id)) as sent "+\
            " FROM messages m "+ self.db.GetSQLCountriesFrom(self.db.identities_db)+ \
            "  , "+self.db.identities_db+".upeople up "+\
            " WHERE "+ self.db.GetSQLCountriesWhere(country_name)+ " AND "+\
            "  up.id = upc.upeople_id AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+\
            " GROUP BY up.identifier "+\
            " ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT " + str(limit)
        data = ExecuteQuery(q)
        return (data)

    def _get_top_company (self, metric_filters):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        company_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)

        q = "SELECT up.id as id, up.identifier as senders, "+\
            " COUNT(DISTINCT(m.message_id)) as sent "+\
            " FROM messages m, "+self.db.identities_db+".upeople up "+\
             self.db.GetSQLCompaniesFrom(self.db.identities_db) +\
            " WHERE "+self.db.GetSQLCompaniesWhere(company_name)+" AND "+\
            "  up.id = upc.upeople_id AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+\
            " GROUP BY up.identifier "+\
            " ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT " + str(limit)
        data = ExecuteQuery(q)
        return (data)

    def _get_top_domain (self, metric_filters):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        domain_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)

        q = "SELECT up.identifier as senders, "+\
            " COUNT(DISTINCT(m.message_id)) as sent "+\
            " FROM messages m "+self.db.GetSQLDomainsFrom(self.db.identities_db) +\
            " , "+self.db.identities_db+".upeople up "+\
            " WHERE "+self.db.GetSQLDomainsWhere(domain_name)+ " AND "+\
            "  up.id = upd.upeople_id AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+\
            " GROUP BY up.identifier "+\
            " ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT "+ str(limit)
        data = ExecuteQuery(q)
        return (data)


    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        tables = self.db.GetTablesOwnUniqueIds()
        filters = self.db.GetFiltersOwnUniqueIds()

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        if filter_bots != "": filter_bots += " AND "

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(first_date) as last_date from messages) t"
            dfilters = " AND DATEDIFF (last_date, first_date) < %s " % (days)

        q = "SELECT u.id as id, u.identifier as senders, "+\
                "COUNT(distinct(m.message_id)) as sent "+\
                "FROM "+ tables + dtables +\
                " ,"+self.db.identities_db+".upeople u "+\
                "WHERE "+ filter_bots + filters + " AND "+\
                "  pup.upeople_id = u.id AND "+\
                "  m.first_date >= "+startdate+" AND "+\
                "  m.first_date < "+enddate +\
                dfilters+ " "+\
                "GROUP BY u.identifier "+\
                "ORDER BY sent desc, senders "+\
                "LIMIT " + str(limit)
        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_supported_filters(self):
        return ['repository','company','country','domain']

    def _get_sql (self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables = tables + ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            #not sure if this line is useful anymore...
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       "mp.email_address = pup.people_id and "+\
                       "mp.type_of_recipient=\'From\' "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class SendersResponse(Metrics):
    """ People answering in a thread """
    # Threads class is not needed here. This is thanks to the use of the
    # field is_reponse_of.

    id = "senders_response"
    name = "Senders Response"
    desc = "People answering in a thread"
    data_source = MLS

    def _get_sql (self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders_response "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables += ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       "mp.email_address = pup.people_id and "+\
                       "mp.type_of_recipient=\'From\' "
        filters += " and m.is_response_of is not null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class SendersInit(Metrics):
    """ People initiating threads """

    id = "senders_init"
    name = "SendersInit"
    desc = "People initiating threads"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders_init "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables += ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       " mp.email_address = pup.people_id and "+\
                       " mp.type_of_recipient=\'From\' "
        filters += " and m.is_response_of is null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class EmailsSentResponse(Metrics):
    """ Emails sent as response """

    id = "sent_response"
    name = "SentResponse"
    desc = "Emails sent as response"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = " count(distinct(m.message_ID)) as sent_response "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis) + " and m.is_response_of is not null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class EmailsSentInit(Metrics):
    """ Emails sent as initiating a thread """

    id = "sent_init"
    name = "EmailsSentInit"
    desc = "Emails sent to start a thread"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = " count(distinct(m.message_ID)) as sent_init"
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis) + " and m.is_response_of is null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class Threads(Metrics):
    """ Number of threads """

    id = "threads"
    name = "Threads"
    desc = "Number of threads"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = " count(distinct(m.is_response_of)) as threads"
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class Repositories(Metrics):
    """ Mailing lists repositories """

    id = "repositories"
    name = "Mailing Lists"
    desc = "Mailing lists with activity"
    data_source = MLS

    def _get_sql(self, evolutionary):
        #fields = " COUNT(DISTINCT(m."+rfield+")) AS repositories  "
        fields = " COUNT(DISTINCT(m.mailing_list_url)) AS repositories "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

    def get_list (self) :
        rfield = MLS.get_repo_field()
        names = ""
        if (rfield == "mailing_list_url") :
            q = "SELECT ml.mailing_list_url, COUNT(message_ID) AS total "+\
                   "FROM messages m, mailing_lists ml "+\
                   "WHERE m.mailing_list_url = ml.mailing_list_url AND "+\
                   "m.first_date >= "+self.filters.startdate+" AND "+\
                   "m.first_date < "+self.filters.enddate+" "+\
                   "GROUP BY ml.mailing_list_url ORDER by total desc"
            mailing_lists = self.db.ExecuteQuery(q)
            mailing_lists_files = self.db.ExecuteQuery(q)
            names = mailing_lists_files[rfield]
        else:
            # TODO: not ordered yet by total messages
            q = "SELECT DISTINCT(mailing_list) FROM messages m "+\
                "WHERE m.first_date >= "+startdate+" AND "+\
                "m.first_date < "+enddate
            mailing_lists = ExecuteQuery(q)
            names = mailing_lists
        return (names)


class Companies(Metrics):
    """ Companies participating in mailing lists """

    id = "companies"
    name = "Companies"
    desc = "Companies participating in mailing lists"
    data_source = MLS

    def _get_sql(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate, 
                                  self.filters.enddate, ['company', ''], evolutionary, 'companies')

    def get_list (self):
        filter_ = DataSource.get_filter_bots(Filter("company"))
        filter_companies = ''
        for company in filter_:
            filter_companies += " c.name<>'"+company+"' AND "

        q = "SELECT c.name as name, COUNT(DISTINCT(m.message_ID)) as sent "+\
            "    FROM "+ self.db.GetTablesCompanies()+ " "+\
            "    WHERE "+ self.db.GetFiltersCompanies()+ " AND "+\
            "      "+ filter_companies+ " "+\
            "      m.first_date >= "+self.filters.startdate+" AND "+\
            "      m.first_date < "+self.filters.enddate+" "+\
            "    GROUP BY c.name "+\
            "    ORDER BY COUNT(DISTINCT(m.message_ID)) DESC"

        data = self.db.ExecuteQuery(q)
        return (data['name'])

class Domains(Metrics):
    """ Domains found in the analysis of mailing lists """

    id = "domains"
    name = "Domains"
    desc = "Domains found in the analysis of mailing lists """
    data_source = MLS

    def _get_sql(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, ['domain', ''], evolutionary, 'domains')

    def get_list  (self) :
        filter_ = DataSource.get_filter_bots(Filter("domain"))
        filter_domains = ""
        for domain in filter_:
            filter_domains += " d.name<>'"+ domain + "' AND "

        q = "SELECT d.name as name, COUNT(DISTINCT(m.message_ID)) as sent "+\
            "    FROM "+self.db.GetTablesDomains()+ " "+\
            "    WHERE "+ self.db.GetFiltersDomains()+ " AND "+\
            "    "+ filter_domains+ " "+\
            "    m.first_date >= "+self.filters.startdate+" AND "+\
            "    m.first_date < "+self.filters.enddate+\
            "    GROUP BY d.name "+\
            "    ORDER BY COUNT(DISTINCT(m.message_ID)) DESC, d.name LIMIT " + str(Metrics.domains_limit)
        data = self.db.ExecuteQuery(q)
        return (data['name'])

class Countries(Metrics):
    """ Countries participating in mailing lists """

    id = "countries"
    name = "Countries"
    desc = "Countries participating in mailing lists"
    data_source = MLS

    def _get_sql(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, ['country', ''], evolutionary, 'countries')

    def get_list  (self):
        filter_ = DataSource.get_filter_bots(Filter("country"))
        filter_countries = ''
        for country in filter_:
            filter_countries += " c.name<>'"+country+"' AND "

        q = "SELECT c.name as name, COUNT(m.message_ID) as sent "+\
                "FROM "+ self.db.GetTablesCountries()+ " "+\
                "WHERE "+ self.db.GetFiltersCountries()+ " AND "+\
                "  "+ filter_countries+ " "+\
                "  m.first_date >= "+self.filters.startdate+" AND "+\
                "  m.first_date < "+self.filters.enddate+" "+\
                "GROUP BY c.name "+\
                "ORDER BY COUNT((m.message_ID)) DESC, name "
        data = self.db.ExecuteQuery(q)
        return(data['name'])


class Projects(Metrics):
    """ Projects existing in mailing lists """

    id = "projects"
    name = "Projects"
    desc = "Projects existing in mailing lists"
    data_source = MLS

    def get_list(self):
        # Projects activity needs to include subprojects also
        logging.info ("Getting projects list for MLS")

        # Get all projects list
        q = "SELECT p.id AS name FROM  %s.projects p" % (self.db.identities_db)
        projects = self.db.ExecuteQuery(q)
        data = []

        # Loop all projects getting reviews
        for project in projects['name']:
            type_analysis = ['project', project]
            period = None
            filter_com = MetricFilters(period, self.filters.startdate,
                                       self.filters.enddate, type_analysis)
            msent = MLS.get_metrics("sent", MLS)
            # TODO: we should restore original filter
            msent.filters = filter_com
            sent = msent.get_agg()

            sent = sent['sent']
            if (sent > 0):
                data.append([sent,project])

        # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
        from operator import itemgetter
        data_sort = sorted(data, key=itemgetter(0),reverse=True)
        names = [name[1] for name in data_sort]

        # if (limit > 0): names = names[:limit]
        return names

class UnansweredPosts(Metrics):
    """ Unanswered posts in mailing lists """

    id = "unanswered_posts"
    name = "Unanswered Posts"
    desc = "Unanswered posts in mailing lists"""
    data_source = MLS

    def __get_date_from_month(self, monthid):
        # month format: year*12+month
        year = (monthid-1) / 12
        month = monthid - year*12
        day = 1
        current = str(year) + "-" + str(month) + "-" + str(day)
        return (current)

    def __get_messages(self, from_date, to_date):
        select = "SELECT message_ID, is_response_of "
        tables = "FROM messages m "
        where = "WHERE m.first_date >= '" + str(from_date) + "' AND m.first_date < '" + str(to_date) + "' "
        where += "AND m.first_date >= " + str(self.filters.startdate) + " AND m.first_date < " + str(self.filters.enddate) + " "

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository")):
            tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
            where += "AND " + self.db.GetSQLReportWhere(self.filters.type_analysis)

        where += "ORDER BY m.first_date"

        query = select + tables + where

        results = self.db.ExecuteQuery(query)

        if isinstance(results['message_ID'], list):
            return [(results['message_ID'][i], results['is_response_of'][i])\
                    for i in range(len(results['message_ID']))]
        else:
            return [(results['message_ID'], results['is_response_of'])]

    def get_agg(self):
        return {}

    def get_ts(self):
        # Get all posts for each month and determine which from those
        # are still unanswered. Returns the number of unanswered
        # posts on each month.
        period = self.filters.period

        if (self.filters.type_analysis and self.filters.type_analysis[0] not in ("repository")):
            return {}

        if (period != "month"):
            logging.error("Period not supported in " + self.id + " " + period)
            return None

        startdate = self.filters.startdate
        enddate = self.filters.enddate

        start = datetime.strptime(startdate, "'%Y-%m-%d'")
        end = datetime.strptime(enddate, "'%Y-%m-%d'")

        start_month = (start.year * 12 + start.month) - 1
        end_month = (end.year * 12 + end.month) - 1
        months = end_month - start_month + 2
        num_unanswered = {'month' : [],
                          'unanswered_posts' : []}

        for i in range(0, months):
            unanswered = []
            current_month = start_month + i
            from_date = self.__get_date_from_month(current_month)
            to_date = self.__get_date_from_month(current_month + 1)
            messages = self.__get_messages(from_date, to_date)

            for message in messages:
                message_id = message[0]
                response_of = message[1]

                if response_of is None:
                    unanswered.append(message_id)
                    continue

                if response_of in unanswered:
                    unanswered.remove(response_of)

            num_unanswered['month'].append(current_month)
            num_unanswered['unanswered_posts'].append(len(unanswered))

        return completePeriodIds(num_unanswered, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)
