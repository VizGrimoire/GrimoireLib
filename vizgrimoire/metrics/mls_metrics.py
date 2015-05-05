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

from vizgrimoire.data_source import DataSource
from vizgrimoire.GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff
from vizgrimoire.filter import Filter
from vizgrimoire.metrics.metrics import Metrics
from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.MLS import MLS

from sets import Set

class InitialActivity(Metrics):
    """ For the given dates of activity, this returns the first trace found
    """

    id = "first_date"
    name = "First activity date"
    desc = "First message sent the two provided dates"
    data_source = MLS

    def get_agg(self):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("DATE_FORMAT(MIN(m.first_date),'%Y-%m-%d') AS first_date")

        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "m.first_date", fields,
                                   tables, filters, False,
                                   self.filters.type_analysis)
        return self.db.ExecuteQuery(query)

class EndOfActivity(Metrics):
    """ For the given dates of activity, this returns the last trace found
    """
    id = "last_date"
    name = "Last activity date"
    desc = "Last messages sent between the two provided dates"
    data_source = MLS

    def get_agg(self):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("DATE_FORMAT(MAX(m.first_date),'%Y-%m-%d') AS last_date")

        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "m.first_date", fields,
                                   tables, filters, False,
                                   self.filters.type_analysis)

        return self.db.ExecuteQuery(query)


class EmailsSent(Metrics):
    """ Emails metric class for mailing lists analysis """

    id = "sent"
    name = "Emails Sent"
    desc = "Emails sent to mailing lists"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(m.message_ID)) as sent")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query


class EmailsSenders(Metrics):
    """ Emails Senders class for mailing list analysis """

    id = "senders"
    name = "Email Senders"
    desc = "People sending emails"
    data_source = MLS

    def _get_top_repository (self, metric_filters, days = None):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        repo = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        rfield = MLS.get_repo_field()

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(first_date) as last_date from messages) t"
            dfilters = " and DATEDIFF (last_date, first_date) < %s " % (days)

        #TODO: instead of directly using the private method for building 
        # from or where clauses, this code should use GetSQLReportFrom/Where
        # and sets where strings are added.
        uniqueids_tables = self.db._get_tables_query(self.db.GetTablesOwnUniqueIds())
        uniqueids_filters = self.db._get_filters_query(self.db.GetFiltersOwnUniqueIds())

        q = "SELECT up.uuid as id, up.identifier as senders, "+\
                " COUNT(m.message_id) as sent "+\
                " FROM "+ uniqueids_tables+ ","+self.db.identities_db+".uidentities up "+ dtables + \
                " WHERE "+ uniqueids_filters + " AND "+\
                "  pup.uuid = up.uuid AND "+\
                "  m.first_date >= "+startdate+" AND "+\
                "  m.first_date < "+enddate+" AND "+\
                "  m."+rfield+"="+ repo + " " + dfilters + \
                " GROUP BY up.identifier "+\
                " ORDER BY sent desc, senders "+\
                " LIMIT " + str(limit)
        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_country (self, metric_filters, days = None):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        country_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)

        #TODO: instead of directly using the private method for building 
        # from or where clauses, this code should use GetSQLReportFrom/Where
        # and sets where strings are added.
        countries_tables = self.db._get_tables_query(self.db.GetSQLCountriesFrom())
        countries_filters = self.db._get_filters_query(self.db.GetSQLCountriesWhere(country_name))

        q = "SELECT up.uuid as id, up.identifier as senders, "+\
            " COUNT(DISTINCT(m.message_id)) as sent "+\
            " FROM messages m, "+ countries_tables+ \
            "  , "+self.db.identities_db+".uidentities up "+\
            " WHERE "+ countries_filters + " AND "+\
            "  up.uuid = pro.uuid AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+\
            " GROUP BY up.identifier "+\
            " ORDER BY COUNT(DISTINCT(m.message_ID)) DESC, senders LIMIT " + str(limit)
        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_company (self, metric_filters, days = None):
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        company_name = metric_filters.type_analysis[1]
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)

        #TODO: instead of directly using the private method for building 
        # from or where clauses, this code should use GetSQLReportFrom/Where
        # and sets where strings are added.
        organizations_tables = self.db._get_tables_query(self.db.GetSQLCompaniesFrom())
        organizations_filters = self.db._get_filters_query(self.db.GetSQLCompaniesWhere(company_name))

        q = "SELECT up.uuid as id, up.identifier as senders, "+\
            " COUNT(DISTINCT(m.message_id)) as sent "+\
            " FROM messages m, "+self.db.identities_db+".uidentities up , "+\
             organizations_tables +\
            " WHERE "+ organizations_filters  +" AND "+\
            "  up.uuid = enr.uuid AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+\
            " GROUP BY up.identifier "+\
            " ORDER BY COUNT(DISTINCT(m.message_ID)) DESC, senders LIMIT " + str(limit)
        data = self.db.ExecuteQuery(q)
        return (data)


    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        tables = self.db._get_tables_query(self.db.GetTablesOwnUniqueIds())
        filters = self.db._get_filters_query(self.db.GetFiltersOwnUniqueIds())

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        if filter_bots != "": filter_bots += " AND "

        dtables = dfilters = ""
        if (days > 0):
            dtables = ", (SELECT MAX(first_date) as last_date from messages) t"
            dfilters = " AND DATEDIFF (last_date, first_date) < %s " % (days)

        q = "SELECT up.uuid as id, up.identifier as senders, "+\
                "COUNT(distinct(m.message_id)) as sent "+\
                "FROM "+ tables + dtables +\
                " ,"+self.db.identities_db+".uidentities up "+\
                "WHERE "+ filter_bots + filters + " AND "+\
                "  pup.uuid = up.uuid AND "+\
                "  m.first_date >= "+startdate+" AND "+\
                "  m.first_date < "+enddate +\
                dfilters+ " "+\
                "GROUP BY up.identifier "+\
                "ORDER BY sent desc, senders "+\
                "LIMIT " + str(limit)
        data = self.db.ExecuteQuery(q)
        return (data)

    def _get_top_supported_filters(self):
        return ['repository','company','country','domain']

    def _get_sql (self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(pup.uuid)) as senders")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        # Adding unique ids filters (just in case)
        tables.add("messages_people mp")
        tables.add("people_uidentities pup")
        # TODO: ownuniqueids and reportwhere should be merged all in reportwhere
        filters.union_update(self.db.GetFiltersOwnUniqueIds())
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables.add("messages_people mp")
            tables.add("people_uidentities pup")
            filters.add("m.message_ID = mp.message_id")
            filters.add("mp.email_address = pup.people_id")
            filters.add("mp.type_of_recipient = \'From\'")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)

        return query

class People(Metrics):
    """ People metric class for mailing lists (senders) """
    id = "people2"
    name = "Mailing lists people"
    desc = "People sending messages"
    data_source = MLS

    def _get_top_global (self, days = 0, metric_filters = None):
        """ Implemented using EmailsSenders """
        top = None
        senders = MLS.get_metrics("senders", MLS)
        if senders is None:
            senders = EmailsSenders(self.db, self.filters)
            top = senders._get_top_global(days, metric_filters)
        else:
            afilters = senders.filters
            senders.filters = self.filters
            top = senders._get_top_global(days, metric_filters)
            senders.filters = afilters

        top['name'] = top.pop('senders')
        return top

class SendersResponse(Metrics):
    """ People answering in a thread """
    # Threads class is not needed here. This is thanks to the use of the
    # field is_reponse_of.

    id = "senders_response"
    name = "Senders Response"
    desc = "People answering in a thread"
    data_source = MLS

    def _get_sql (self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(pup.uuid)) as senders_response")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        tables.add("messages_people mp")
        tables.add("people_uidentities pup")
        filters.union_update(self.db.GetFiltersOwnUniqueIds())
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables.add("messages_people mp")
            tables.add("people_uidentities pup")
            filters.add("m.message_ID = mp.message_id")
            filters.add("mp.email_address = pup.people_id")
            filters.add("mp.type_of_recipient = \'From\'")
        filters.add("m.is_response_of is not null")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query

class TimeToFirstReply(Metrics):
    """ Statistical numbers to the time to first reply in threads that take
        place in the specified period

        In first place all of the emails are retrieved using the specific filters
        found in self.filters. For those, their father is checked and if the
        father is the root of a thread, the time between both emails is calculated.
    """

    id = "timeto_attention"
    name = "Time to Attention"
    desc = "Time to first reply in a new thread"
    data_source = MLS

    def get_agg(self):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        # If the TZ is positive that means that we need to substract that date to the original date (eg - (+3600))
        # And  if the TZ is negative, we need to substract that date from the original date (eg - (-3600))
        fields.add("(UNIX_TIMESTAMP(t.first_date) - IFNULL(t.first_date_tz, 0)) - (UNIX_TIMESTAMP(m.first_date) - IFNULL(m.first_date_tz, 0)) as diffdate")

        tables.add("messages m")
        subquery = """(select distinct message_id,
                              is_response_of,
                              first_date,
                              first_date_tz
                       from messages
                       where first_date>= %s and
                             first_date< %s and
                             is_response_of is not NULL) t
                   """ % (self.filters.startdate, self.filters.enddate)
        tables.add(subquery)
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.add("m.message_ID = t.is_response_of")
        filters.add("m.is_response_of is NULL")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        fields_str = "select " + self.db._get_fields_query(fields)
        tables_str = " from " + self.db._get_tables_query(tables)
        filters_str = " where " + self.db._get_filters_query(filters)

        query = fields_str + tables_str + filters_str

        timeframes = self.db.ExecuteQuery(query)

        return timeframes["diffdate"]


class SendersInit(Metrics):
    """ People initiating threads """

    id = "senders_init"
    name = "SendersInit"
    desc = "People initiating threads"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(pup.uuid)) as senders_init")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        tables.add("messages_people mp")
        tables.add("people_uidentities pup")
        filters.union_update(self.db.GetFiltersOwnUniqueIds())

        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables.add("messages_people mp")
            tables.add("people_uidentities pup")
            filters.add("m.message_ID = mp.message_id")
            filters.add("mp.email_address = pup.people_id")
            filters.add("mp.type_of_recipient = \'From\'")

        filters.add("m.is_response_of is null")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query


class EmailsSentResponse(Metrics):
    """ Emails sent as response """

    id = "sent_response"
    name = "SentResponse"
    desc = "Emails sent as response"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(m.message_ID)) as sent_response")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))
        filters.add("m.is_response_of is not null")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query

class EmailsSentInit(Metrics):
    """ Emails sent as initiating a thread """

    id = "sent_init"
    name = "EmailsSentInit"
    desc = "Emails sent to start a thread"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(m.message_ID)) as sent_init")
        tables.add("messages m") 
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))
        filters.add("m.is_response_of is null")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query

class Threads(Metrics):
    """ Number of threads """

    id = "threads"
    name = "Threads"
    desc = "Number of threads"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(m.message_ID)) as threads")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))
        filters.add("m.is_response_of is null")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query

class Repositories(Metrics):
    """ Mailing lists repositories """

    id = "repositories"
    name = "Mailing Lists"
    desc = "Mailing lists with activity"
    data_source = MLS

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("COUNT(DISTINCT(m.mailing_list_url)) AS repositories")
        tables.add("messages m")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
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
            mailing_lists = self.db.ExecuteQuery(q)
            names = mailing_lists
        return (names)


class Companies(Metrics):
    """ Organizations participating in mailing lists """

    id = "organizations"
    name = "Organizations"
    desc = "Organizations participating in mailing lists"
    data_source = MLS

    def _get_sql(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate, 
                                  self.filters.enddate, ['company', ''], evolutionary, 'organizations')

    def get_list (self):
        filter_ = DataSource.get_filter_bots(Filter("company"))
        filter_organizations = ''
        for company in filter_:
            filter_organizations += " org.name<>'"+company+"' AND "

        organizations_tables = self.db._get_tables_query(self.db.GetTablesCompanies())
        organizations_filters = self.db._get_filters_query(self.db.GetFiltersCompanies())
        q = "SELECT org.name as name, COUNT(DISTINCT(m.message_ID)) as sent "+\
            "    FROM "+ organizations_tables + " "+\
            "    WHERE "+ organizations_filters + " AND "+\
            "      "+ filter_organizations+ " "+\
            "      m.first_date >= "+self.filters.startdate+" AND "+\
            "      m.first_date < "+self.filters.enddate+" "+\
            "    GROUP BY org.name "+\
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
        domains_tables = self.db._get_tables_query(self.db.GetTablesDomains())
        domains_filters = self.db._get_filters_query(self.db.GetFiltersDomains())

        q = "SELECT DISTINCT(SUBSTR(email_address,LOCATE('@',email_address)+1)) as domain, COUNT(DISTINCT(m.message_ID)) as sent "+\
            "    FROM "+ domains_tables + " "+\
            "    WHERE "+ domains_filters + " AND "+\
            "    m.first_date >= "+self.filters.startdate+" AND "+\
            "    m.first_date < "+self.filters.enddate+\
            "    GROUP BY domain "+\
            "    ORDER BY COUNT(DISTINCT(m.message_ID)) DESC, domain LIMIT " + str(Metrics.domains_limit)
        data = self.db.ExecuteQuery(q)
        data['name'] = data.pop('domain')
        return (data['name'])

class Countries(Metrics):
    """ Countries participating in mailing lists """

    id = "countries"
    name = "Countries"
    desc = "Countries participating in mailing lists"
    data_source = MLS

    def _get_sql(self, evolutionary):
        query = self.db.GetStudies(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, ['country', ''], evolutionary, 'countries')
        return query

    def get_list  (self):
        filter_ = DataSource.get_filter_bots(Filter("country"))
        filter_countries = ''
        for country in filter_:
            filter_countries += " cou.name<>'"+country+"' AND "

        countries_tables = self.db._get_tables_query(self.db.GetTablesCountries())
        countries_filters = self.db._get_filters_query(self.db.GetFiltersCountries())
        q = "SELECT cou.name as name, COUNT(m.message_ID) as sent "+\
                "FROM "+ countries_tables + " "+\
                "WHERE "+ countries_filters + " AND "+\
                "  "+ filter_countries+ " "+\
                "  m.first_date >= "+self.filters.startdate+" AND "+\
                "  m.first_date < "+self.filters.enddate+" "+\
                "GROUP BY cou.name "+\
                "ORDER BY COUNT((m.message_ID)) DESC, cou.name "
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
        q = "SELECT p.id AS name FROM  %s.projects p" % (self.db.projects_db)
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
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("m.message_ID as message_ID")
        fields.add("m.is_response_of as is_response_of")
        tables.add("messages m")
        filters.add("m.first_date >= '" + str(from_date) + "' ") 
        filters.add("m.first_date < '" + str(to_date) + "' ")
        filters.add("m.first_date >= " + str(self.filters.startdate))
        filters.add("m.first_date < " + str(self.filters.enddate))

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository")):
            tables.union_update(self.db.GetSQLReportFrom(self.filters))
            filters.union_update(self.db.GetSQLReportWhere(self.filters))

        select_str = "select " + self.db._get_fields_query(fields)
        from_str = " from " + self.db._get_tables_query(tables)
        where_str = " where " + self.db._get_filters_query(filters)

        where_str += " ORDER BY m.first_date "

        query = select_str + from_str + where_str

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

