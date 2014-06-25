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
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acso@bitergia.com>

#############
# TODO: missing functions wrt 
#       evolution and agg values of countries and companies
#############

import logging
import os
import re
import sys
import datetime

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from GrimoireSQL import ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, getPeriod, createJSON, get_subprojects
from metrics_filter import MetricFilters

from data_source import DataSource
import report
from filter import Filter


class MLS(DataSource):
    _metrics_set = []

    @staticmethod
    def get_repo_field():
        return "mailing_list_url"

    @staticmethod
    def get_db_name():
        return "db_mlstats"

    @staticmethod
    def get_name(): return "mls"

    @staticmethod
    def get_date_init(startdate, enddate, identities_db = None, type_analysis = None):
        fields = "DATE_FORMAT(MIN(m.first_date),'%Y-%m-%d') AS first_date"
        tables = "messages m"
        filters = ""
        q = GetSQLGlobal('m.first_date',fields, tables, filters, startdate, enddate)
        return ExecuteQuery(q)

    @staticmethod
    def get_date_end(startdate, enddate,  identities_db = None, type_analysis = None):
        fields = "DATE_FORMAT(MAX(m.first_date),'%Y-%m-%d') AS last_date"
        tables = "messages m"
        filters = ""
        q = GetSQLGlobal('m.first_date',fields, tables, filters, startdate, enddate)
        return ExecuteQuery(q)

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        rfield = MLS.get_repo_field()
        evol = {}

        rfield = MLS.get_repo_field()

        type_analysis = None
        if (filter_ is not None):
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]
        evol = EvolMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis)

        return evol

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  MLS.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = MLS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        rfield = MLS.get_repo_field()

        type_analysis = None
        if (filter_ is not None):
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]
        agg = StaticMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis)

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data = MLS.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = MLS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def getLongestThreads(startdate, enddate, identities_db, npeople):
        # This function builds a coherent data structure according
        # to other simila structures. The Class Threads only returns
        # the head of the threads (the first message) and the message_id
        # of each of its children.

        main_topics = Threads(startdate, enddate, identities_db)

        longest_threads = main_topics.topLongestThread(npeople)
        l_threads = {}
        l_threads['message_id'] = []
        l_threads['length'] = []
        l_threads['subject'] = []
        l_threads['date'] = []
        l_threads['initiator_name'] = []
        l_threads['initiator_id'] = []
        l_threads['url'] = []
        for email in longest_threads:
            l_threads['message_id'].append(email.message_id)
            l_threads['length'].append(main_topics.lenThread(email.message_id))
            l_threads['subject'].append(email.subject)
            l_threads['date'].append(email.date.strftime("%Y-%m-%d"))
            l_threads['initiator_name'].append(email.initiator_name)
            l_threads['initiator_id'].append(email.initiator_id)
            l_threads['url'].append(email.url)

        return l_threads


    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = MLS.get_bots()
        if filter_ is None:
            top = {}
            top['senders.']=top_senders(0, startdate, enddate,identities_db,bots, npeople)
            top['senders.last year']=top_senders(365, startdate, enddate,identities_db, bots, npeople)
            top['senders.last month']=top_senders(31, startdate, enddate,identities_db,bots, npeople)

            top_senders_data['threads.'] = MLS.getLongestThreads(startdate, enddate, identities_db, npeople)
            startdate = datetime.date.today() - datetime.timedelta(days=365)
            startdate =  "'" + str(startdate) + "'"
            top_senders_data['threads.last year'] = MLS.getLongestThreads(startdate, enddate, identities_db, npeople)
            startdate = datetime.date.today() - datetime.timedelta(days=30)
            startdate =  "'" + str(startdate) + "'"
            top_senders_data['threads.last month'] = MLS.getLongestThreads(startdate, enddate, identities_db, npeople) 

        else:
            filter_name = filter_.get_name()
            item = filter_.get_item()

            if (filter_name == "company"):
                top = companyTopSenders (item, identities_db, startdate, enddate, npeople)
            elif (filter_name == "country"):
                top = countryTopSenders (item, identities_db, startdate, enddate, npeople)
            elif (filter_name == "domain"):
                top = domainTopSenders(item, identities_db, startdate, enddate, npeople)
            elif (filter_name == "repository"):
                rfield = MLS.get_repo_field()
                top = repoTopSenders (item, identities_db, startdate, enddate, rfield, npeople)
            else:
                top = None

        return top

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = MLS.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+MLS().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        rfield = MLS.get_repo_field()
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            items  = reposNames(rfield, startdate, enddate)  
        elif (filter_name == "company"):
            items  = companiesNames(identities_db, startdate, enddate)
        elif (filter_name == "country"):
            items = countriesNames(identities_db, startdate, enddate)
        elif (filter_name == "domain"):
            items = domainsNames(identities_db, startdate, enddate)
        elif (filter_name == "project"):
            items = get_projects_mls_name(startdate, enddate, identities_db)
        else:
            logging.error(filter_name + " not supported")
        return items

    @staticmethod
    def get_filter_summary(filter_, period, startdate, enddate, identities_db, limit):
        summary = None
        filter_name = filter_.get_name()

        if (filter_name == "company"):
            summary =  GetSentSummaryCompanies(period, startdate, enddate, identities_db, limit)
        return summary

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items = MLS.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        items_files = [item.replace('/', '_').replace("<","__").replace(">","___")
            for item in items]

        fn = os.path.join(destdir, filter_.get_filename(MLS()))
        createJSON(items_files, fn)

        if filter_name in ("domain", "company", "repository"):
            items_list = {'name' : [], 'sent_365' : [], 'senders_365' : []}
        else:
            items_list = items

        for item in items :
            item_name = "'"+ item+ "'"
            logging.info (item_name)
            filter_item = Filter(filter_.get_name(), item)

            evol_data = MLS.get_evolutionary_data(period, startdate, enddate, 
                                                  identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(MLS()))
            createJSON(evol_data, fn)

            agg = MLS.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(MLS()))
            createJSON(agg, fn)

            if filter_name in ("domain", "company", "repository"):
                items_list['name'].append(item.replace('/', '_').replace("<","__").replace(">","___"))
                items_list['sent_365'].append(agg['sent_365'])
                items_list['senders_365'].append(agg['senders_365'])

            top_senders = MLS.get_top_data(startdate, enddate, identities_db, filter_item, npeople)
            createJSON(top_senders, destdir+"/"+filter_item.get_top_filename(MLS()))

        fn = os.path.join(destdir, filter_.get_filename(MLS()))
        createJSON(items_list, fn)

        if (filter_name == "company"):
            sent = MLS.get_filter_summary(filter_, period, startdate, enddate, identities_db, 10)
            createJSON (sent, destdir+"/"+filter_.get_summary_filename(MLS))

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        top_data = MLS.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['senders.']["id"]
        top += top_data['senders.last year']["id"]
        top += top_data['senders.last month']["id"]
        # remove duplicates
        people = list(set(top))
        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        evol = GetEvolPeopleMLS(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        return GetStaticPeopleMLS(upeople_id, startdate, enddate)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        unique_ids = True
        vizr.ReportDemographicsAgingMLS(enddate, destdir, unique_ids)
        vizr.ReportDemographicsBirthMLS(enddate, destdir, unique_ids)

        ## Which quantiles we're interested in
        # quantiles_spec = [0.99,0.95,0.5,0.25]

        ## Yearly quantiles of time to attention (minutes)
        ## Monthly quantiles of time to attention (hours)
        ## JSON files generated from VizR
        vizr.ReportTimeToAttendMLS(destdir)

    @staticmethod
    def get_query_builder():
        from query_builder import MLSQuery
        return MLSQuery

    @staticmethod
    def get_metrics_core_agg():
        m  = ['sent','senders','threads','sent_response','senders_response','senders_init','repositories']
        return m


    @staticmethod
    def get_metrics_core_ts():
        m  = ['sent','senders','threads','sent_response','senders_response','senders_init','repositories']
        return m

    @staticmethod
    def get_metrics_core_trends():
        return ['sent','senders']

##############
# Specific FROM and WHERE clauses per type of report
##############

def GetMLSSQLRepositoriesFrom ():
    # tables necessary for repositories
    #return (" messages m ") 
    return (" ")


def GetMLSSQLRepositoriesWhere (repository):
    # fields necessary to match info among tables
    return (" m.mailing_list_url = "+repository+" ")



def GetMLSSQLCompaniesFrom (i_db):
    # fields necessary for the companies analysis

    return(" , messages_people mp, "+\
                   "people_upeople pup, "+\
                   i_db+".companies c, "+\
                   i_db+".upeople_companies upc")


def GetMLSSQLCompaniesWhere (name):
    # filters for the companies analysis
    return(" m.message_ID = mp.message_id and "+\
               "mp.email_address = pup.people_id and "+\
               "mp.type_of_recipient=\'From\' and "+\
               "pup.upeople_id = upc.upeople_id and "+\
               "upc.company_id = c.id and "+\
               "m.first_date >= upc.init and "+\
               "m.first_date < upc.end and "+\
               "c.name = "+name)


def GetMLSSQLCountriesFrom (i_db):
    # fields necessary for the countries analysis
    return(" , messages_people mp, "+\
               "people_upeople pup, "+\
               i_db+".countries c, "+\
               i_db+".upeople_countries upc ")


def GetMLSSQLCountriesWhere (name):
    # filters necessary for the countries analysis

    return(" m.message_ID = mp.message_id and "+\
               "mp.email_address = pup.people_id and "+\
               "mp.type_of_recipient=\'From\' and "+\
               "pup.upeople_id = upc.upeople_id and "+\
               "upc.country_id = c.id and "+\
               "c.name="+name)

def GetMLSSQLDomainsFrom (i_db) :
    return (" , messages_people mp, "+\
               "people_upeople pup, "+\
              i_db+".domains d, "+\
              i_db+".upeople_domains upd")


def GetMLSSQLDomainsWhere (name) :
    return (" m.message_ID = mp.message_id and "+\
                "mp.email_address = pup.people_id and "+\
                "mp.type_of_recipient=\'From\' and "+\
                "pup.upeople_id = upd.upeople_id AND "+\
                "upd.domain_id = d.id AND "+\
                "m.first_date >= upd.init AND "+\
                "m.first_date < upd.end and "+\
                "d.name="+ name)


def GetSQLProjectsFromMLS():
    return (" , mailing_lists ml")


def GetSQLProjectsWhereMLS(project, identities_db):
    # include all repositories for a project and its subprojects
    p = project.replace("'", "") # FIXME: why is "'" needed in the name?

    repos = """and ml.mailing_list_url IN (
           SELECT repository_name
           FROM   %s.projects p, %s.project_repositories pr
           WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
               AND pr.data_source='mls'
    )""" % (identities_db, identities_db, get_subprojects(p, identities_db))

    return (repos  + " and ml.mailing_list_url = m.mailing_list_url")

# Using senders only here!
def GetMLSFiltersOwnUniqueIdsMLS  () :
    return ('m.message_ID = mp.message_id AND '+\
            ' mp.email_address = pup.people_id AND '+\
            ' mp.type_of_recipient=\'From\'')

##########
#Generic functions to obtain FROM and WHERE clauses per type of report
##########

def GetMLSSQLReportFrom (identities_db, type_analysis):
    #generic function to generate 'from' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    From = ""

    if (type_analysis is None or len(type_analysis) != 2): return From

    analysis = type_analysis[0]

    if analysis == 'repository': From = GetMLSSQLRepositoriesFrom()
    elif analysis == 'company': From = GetMLSSQLCompaniesFrom(identities_db)
    elif analysis == 'country': From = GetMLSSQLCountriesFrom(identities_db)
    elif analysis == 'domain': From = GetMLSSQLDomainsFrom(identities_db)
    elif analysis == 'project': From = GetSQLProjectsFromMLS()

    return (From)


def GetMLSSQLReportWhere (type_analysis, identities_db=None):
    #generic function to generate 'where' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    where = ""

    if (type_analysis is None or len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': where = GetMLSSQLRepositoriesWhere(value)
    elif analysis == 'company': where = GetMLSSQLCompaniesWhere(value)
    elif analysis == 'country': where = GetMLSSQLCountriesWhere(value)
    elif analysis == 'domain': where = GetMLSSQLDomainsWhere(value)
    elif analysis == 'project':
        if (identities_db is None):
            logging.error("project filter not supported without identities_db")
            sys.exit(0)
        else:
            where = GetSQLProjectsWhereMLS(value, identities_db)

    return (where)


#########
# Other generic functions
#########

def get_projects_mls_name(startdate, enddate, identities_db, limit=0):
    # Projects activity needs to include subprojects also
    logging.info ("Getting projects list for MLS")

    # Get all projects list
    q = "SELECT p.id AS name FROM  %s.projects p" % (identities_db)
    projects = ExecuteQuery(q)
    data = []

    # Loop all projects getting reviews
    for project in projects['name']:
        type_analysis = ['project', project]
        period = None
        evol = False

        sent = GetEmailsSent(period, startdate, enddate, identities_db,
                             type_analysis, evol)

        sent = sent['sent']
        if (sent > 0):
            data.append([sent,project])

    # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
    from operator import itemgetter
    data_sort = sorted(data, key=itemgetter(0),reverse=True)
    names = [name[1] for name in data_sort]

    if (limit > 0): names = names[:limit]
    return names


def reposField () :
    # Depending on the mailing list, the field to be
    # used is mailing_list or mailing_list_url
    rfield = 'mailing_list'
    sql = "select count(distinct(mailing_list)) from messages"
    mailing_lists = ExecuteQuery(sql)
    if (len(mailing_lists) == 0) :
        rfield = "mailing_list_url"

    return (rfield);


def GetMLSFiltersResponse () :
    filters = GetMLSFiltersOwnUniqueIdsMLS()
    filters_response = filters + " AND m.is_response_of IS NOT NULL"
    return filters_response

##########
# Meta functions that aggregate all evolutionary or static data in one call
##########


def GetMLSInfo (period, startdate, enddate, identities_db, rfield, type_analysis, evolutionary):
    filter_ = None
    if type_analysis is not None:
        filter_ = Filter(type_analysis[0],type_analysis[1])
    metrics = DataSource.get_metrics_data(MLS, period, startdate, enddate, identities_db, filter_, evolutionary)
    if filter_ is not None: studies = {}
    else:
        studies = DataSource.get_studies_data(MLS, period, startdate, enddate, evolutionary)
    return dict(metrics.items()+studies.items())

def EvolMLSInfo (period, startdate, enddate, identities_db, rfield, type_analysis = []):
    #Evolutionary info all merged in a dataframe
    return(GetMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis, True))

def StaticMLSInfo (period, startdate, enddate, identities_db, rfield, type_analysis = []):
    #Agg info all merged in a dataframe
    return(GetMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis, False))


def GetActiveSendersMLS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    #Gets people sending messages during last days
    q0 = "SELECT distinct(pup.upeople_id) as active_senders" +\
    " FROM messages m,  messages_people mp, people_upeople pup" +\
    " WHERE m.message_ID = mp.message_id AND" +\
    " mp.email_address = pup.people_id AND" +\
    " mp.type_of_recipient='From' AND "+\
    " m.first_date >= (%s - INTERVAL %s day)"    
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActivePeopleMLS(days, enddate):
    #Gets list of IDs of people active during last days until enddate
    senders = GetActiveSendersMLS(days, enddate)
    aux = senders['active_senders']
    if not isinstance(aux, list):
        active_people = [aux]
    else:
        active_people = aux
    return(active_people)

def GetEmailsSent (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts emails sent

    if (evolutionary):
        fields = " count(distinct(m.message_ID)) as sent "
    else:
        fields = " count(distinct(m.message_ID)) as sent, "+\
                  " DATE_FORMAT (min(m.first_date), '%Y-%m-%d') as first_date, "+\
                  " DATE_FORMAT (max(m.first_date), '%Y-%m-%d') as last_date "

    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis, identities_db)

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolEmailsSent (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of emails sent
    return(GetEmailsSent(period, startdate, enddate, identities_db, type_analysis , True))


def AggEmailsSent (period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails sent
    return(GetEmailsSent(period, startdate, enddate, identities_db, type_analysis, False))


####################
# Lists of repositories, companies, countries, etc
# Functions to obtain list of names (of repositories) per type of analysis
####################


# WARNING: Functions directly copied from old MLS.R

def reposNames  (rfield, startdate, enddate) :
    names = ""
    if (rfield == "mailing_list_url") :
        q = "SELECT ml.mailing_list_url, COUNT(message_ID) AS total "+\
               "FROM messages m, mailing_lists ml "+\
               "WHERE m.mailing_list_url = ml.mailing_list_url AND "+\
               "m.first_date >= "+startdate+" AND "+\
               "m.first_date < "+enddate+" "+\
               "GROUP BY ml.mailing_list_url ORDER by total desc"
        mailing_lists = ExecuteQuery(q)
        mailing_lists_files = ExecuteQuery(q)
        names = mailing_lists_files[rfield]
    else:
        # TODO: not ordered yet by total messages
        q = "SELECT DISTINCT(mailing_list) FROM messages m "+\
            "WHERE m.first_date >= "+startdate+" AND "+\
            "m.first_date < "+enddate
        mailing_lists = ExecuteQuery(q)
        names = mailing_lists
    return (names)

def countriesNames  (identities_db, startdate, enddate, filter_=[]) :
    countries_limit = 30

    filter_countries = ""
    for country in filter_:
        filter_countries += " c.name<>'"+country+"' AND "

    q = "SELECT c.name as name, COUNT(m.message_ID) as sent "+\
            "FROM "+ GetTablesCountries(identities_db)+ " "+\
            "WHERE "+ GetFiltersCountries()+ " AND "+\
            "  "+ filter_countries+ " "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+" "+\
            "GROUP BY c.name "+\
            "ORDER BY COUNT((m.message_ID)) DESC LIMIT "+\
            str(countries_limit)
    data = ExecuteQuery(q)
    return(data['name'])


def companiesNames  (i_db, startdate, enddate, filter_=[]) :
    companies_limit = 30
    filter_companies = ""

    for company in filter_:
        filter_companies += " c.name<>'"+company+"' AND "

    q = "SELECT c.name as name, COUNT(DISTINCT(m.message_ID)) as sent "+\
        "    FROM "+ GetTablesCompanies(i_db)+ " "+\
        "    WHERE "+ GetFiltersCompanies()+ " AND "+\
        "      "+ filter_companies+ " "+\
        "      m.first_date >= "+startdate+" AND "+\
        "      m.first_date < "+enddate+" "+\
        "    GROUP BY c.name "+\
        "    ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT " +\
        str(companies_limit)

    data = ExecuteQuery(q)
    return (data['name'])


def domainsNames  (i_db, startdate, enddate, filter_=[]) :
    domains_limit = 30
    filter_domains = ""

    for domain in filter_:
        filter_domains += " d.name<>'"+ domain + "' AND "

    q = "SELECT d.name as name, COUNT(DISTINCT(m.message_ID)) as sent "+\
        "    FROM "+GetTablesDomains(i_db)+ " "+\
        "    WHERE "+ GetFiltersDomains()+ " AND "+\
        "    "+ filter_domains+ " "+\
        "    m.first_date >= "+startdate+" AND "+\
        "    m.first_date < "+enddate+\
        "    GROUP BY d.name "+\
        "    ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT "+\
            str(domains_limit)
    data = ExecuteQuery(q)
    return (data['name'])

########################
# People functions as in the old version, still to be refactored!
########################

def GetTablesOwnUniqueIdsMLS () :
    return ('messages m, messages_people mp, people_upeople pup')


# Using senders only here!
def GetFiltersOwnUniqueIdsMLS  () :
    return ('m.message_ID = mp.message_id AND '+\
             "mp.email_address = pup.people_id AND "+\
             'mp.type_of_recipient=\'From\'')


def GetTablesCountries (i_db) :
    return (GetTablesOwnUniqueIdsMLS()+', '+\
                  i_db+'.countries c, '+\
                  i_db+'.upeople_countries upc')


def GetFiltersCountries () :
    return (GetFiltersOwnUniqueIdsMLS()+' AND '+\
              "pup.upeople_id = upc.upeople_id AND "+\
              'upc.country_id = c.id')


def GetTablesCompanies (i_db) :
    return (GetTablesOwnUniqueIdsMLS()+', '+\
                  i_db+'.companies c, '+\
                  i_db+'.upeople_companies upc')


def GetFiltersCompanies () :
    return (GetFiltersOwnUniqueIdsMLS()+' AND '+\
                  "pup.upeople_id = upc.upeople_id AND "+\
                  "upc.company_id = c.id AND "+\
                  "m.first_date >= upc.init AND "+\
                  'm.first_date < upc.end')


def GetTablesDomains (i_db) :
    return (GetTablesOwnUniqueIdsMLS()+', '+\
                  i_db+'.domains d, '+\
                  i_db+'.upeople_domains upd')


def GetFiltersDomains () :
    return (GetFiltersOwnUniqueIdsMLS()+' AND '+\
                  "pup.upeople_id = upd.upeople_id AND "+\
                  "upd.domain_id = d.id AND "+\
                  "m.first_date >= upd.init AND "+\
                  'm.first_date < upd.end')

def GetFiltersInit () :
    filters = GetFiltersOwnUniqueIdsMLS()
    filters_init = filters + " AND m.is_response_of IS NULL"
    return filters_init

def GetFiltersResponse () :
    filters = GetFiltersOwnUniqueIdsMLS()
    filters_response = filters + " AND m.is_response_of IS NOT NULL"
    return filters_response

def GetListPeopleMLS (startdate, enddate) :
    fields = "DISTINCT(pup.upeople_id) as id, count(m.message_ID) total"
    tables = GetTablesOwnUniqueIdsMLS()
    filters = GetFiltersOwnUniqueIdsMLS()
    filters += " GROUP BY id ORDER BY total desc"
    q = GetSQLGlobal('first_date',fields,tables, filters, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)

def GetQueryPeopleMLS (developer_id, period, startdate, enddate, evol) :
    fields = "COUNT(m.message_ID) AS sent"
    tables = GetTablesOwnUniqueIdsMLS()
    filters = GetFiltersOwnUniqueIdsMLS() + "AND pup.upeople_id = " + str(developer_id)

    if (evol) :
        q = GetSQLPeriod(period,'first_date', fields, tables, filters,
                startdate, enddate)
    else:
        fields = fields +\
                ",DATE_FORMAT (min(first_date),'%Y-%m-%d') as first_date, "+\
                "DATE_FORMAT (max(first_date),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('first_date', fields, tables, filters,
                startdate, enddate)
    return (q)


def GetEvolPeopleMLS (developer_id, period, startdate, enddate) :
    q = GetQueryPeopleMLS(developer_id, period, startdate, enddate, True)

    data = ExecuteQuery(q)
    return (data)


def GetStaticPeopleMLS (developer_id, startdate, enddate) :
    q = GetQueryPeopleMLS(developer_id, None, startdate, enddate, False)

    data = ExecuteQuery(q)
    return (data)


#########################
# Top activity developers
#########################


def top_senders (days, startdate, enddate, identities_db, bots, limit) :

    filter_bots = ''
    for bot in bots:
        filter_bots = filter_bots + " up.identifier<>'"+bot+"' and "

    dtables = dfilters = ""
    if (days > 0):
        dtables = ", (SELECT MAX(first_date) as last_date from messages) t"
        dfilters = " AND DATEDIFF (last_date, first_date) < %s " % (days)

    tables = GetTablesOwnUniqueIdsMLS()
    filters = GetFiltersOwnUniqueIdsMLS()

    q = "SELECT up.id as id, up.identifier as senders, "+\
            "COUNT(distinct(m.message_id)) as sent "+\
            "FROM "+ tables + dtables +\
            " ,"+identities_db+".upeople up "+\
            "WHERE "+ filter_bots + filters + " AND "+\
            "  pup.upeople_id = up.id AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate +\
            dfilters+ " "+\
            "GROUP BY up.identifier "+\
            "ORDER BY sent desc, senders "+\
            "LIMIT " + limit
    data = ExecuteQuery(q)
    return (data)

def repoTopSenders (repo, identities_db, startdate, enddate, rfield, limit):
    q = "SELECT up.id as id, up.identifier as senders, "+\
            "COUNT(m.message_id) as sent "+\
            "FROM "+ GetTablesOwnUniqueIdsMLS()+ ","+identities_db+".upeople up "+\
            "WHERE "+ GetFiltersOwnUniqueIdsMLS()+ " AND "+\
            "  pup.upeople_id = up.id AND "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate+" AND "+\
            "  m."+rfield+"='"+repo+"' "+\
            "GROUP BY up.identifier "+\
            "ORDER BY sent desc "+\
            "LIMIT " + limit
    data = ExecuteQuery(q)
    return (data)

def countryTopSenders (country_name, identities_db, startdate, enddate, limit):
    q = "SELECT up.id as id, up.identifier as senders, "+\
        "COUNT(DISTINCT(m.message_id)) as sent "+\
        "FROM "+ GetTablesCountries(identities_db)+ \
        "  , "+identities_db+".upeople up "+\
        "WHERE "+ GetFiltersCountries()+ " AND "+\
        "  up.id = upc.upeople_id AND "+\
        "  m.first_date >= "+startdate+" AND "+\
        "  m.first_date < "+enddate+" AND "+\
        "  c.name = '"+country_name+"' "+\
        "GROUP BY up.identifier "+\
        "ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT " + limit
    data = ExecuteQuery(q)
    return (data)

def companyTopSenders (company_name, identities_db, startdate, enddate, limit):
    q = "SELECT up.id as id, up.identifier as senders, "+\
        "COUNT(DISTINCT(m.message_id)) as sent "+\
        "FROM "+GetTablesCompanies(identities_db)+\
        ", "+identities_db+".upeople up "+\
        "WHERE "+GetFiltersCompanies()+" AND "+\
        "  up.id = upc.upeople_id AND "+\
        "  m.first_date >= "+startdate+" AND "+\
        "  m.first_date < "+enddate+" AND "+\
        "  c.name = '"+company_name+"' "+\
        "GROUP BY up.identifier "+\
        "ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT " + limit
    data = ExecuteQuery(q)
    return (data)

def domainTopSenders (domain_name, identities_db, startdate, enddate, limit):
    q = "SELECT up.identifier as senders, "+\
        "COUNT(DISTINCT(m.message_id)) as sent "+\
        "FROM "+GetTablesDomains(identities_db) +\
        " , "+identities_db+".upeople up "+\
        "WHERE "+GetFiltersDomains()+ " AND "+\
        "  up.id = upd.upeople_id AND "+\
        "  m.first_date >= "+startdate+" AND "+\
        "  m.first_date < "+enddate+" AND "+\
        "  d.name = '"+domain_name+"' "+\
        "GROUP BY up.identifier "+\
        "ORDER BY COUNT(DISTINCT(m.message_ID)) DESC LIMIT "+ limit
    data = ExecuteQuery(q)
    return (data)


#######################
# Functions to analyze last activity
#######################

def lastActivity (days) :
    days = str(days)
    #commits
    q = "select count(distinct(message_ID)) as sent_"+days+" "+\
        "    from messages "+\
        "    where first_date >= ( "+\
        "      select (max(first_date) - INTERVAL "+days+" day) "+\
        "      from messages)"

    data1 = ExecuteQuery(q)

    q = "select count(distinct(pup.upeople_id)) as senders_"+days+" "+\
        "    from messages m, "+\
        "      people_upeople pup, "+\
        "      messages_people mp "+\
        "    where pup.people_id = mp.email_address  and "+\
        "      m.message_ID = mp.message_id and "+\
        "      m.first_date >= (select (max(first_date) - INTERVAL "+days+" day) "+\
        "        from messages)"

    data2 = ExecuteQuery(q)

    agg_data = dict(data1.items() + data2.items())
    return(agg_data)

#####################
# MICRO STUDIES
#####################

def StaticNumSent (startdate, enddate):
    fields = " COUNT(*) as sent "
    tables = GetTablesOwnUniqueIdsMLS()
    filters = GetFiltersOwnUniqueIdsMLS()
    q = GetSQLGlobal('first_date', fields, tables, filters,
            startdate, enddate)
    sent = ExecuteQuery(q)
    return(sent)


def StaticNumSenders (startdate, enddate):
    fields = " COUNT(DISTINCT(pup.upeople_id)) as senders "
    tables = GetTablesOwnUniqueIdsMLS()
    filters = GetFiltersOwnUniqueIdsMLS()
    q = GetSQLGlobal('first_date', fields, tables, filters,
            startdate, enddate)
    senders = ExecuteQuery(q)
    return(senders)

def GetDiffSentDays (period, init_date, days):
    chardates = GetDates(init_date, days)
    last = StaticNumSent(chardates[1], chardates[0])
    last = int(last['sent'])
    prev = StaticNumSent(chardates[2], chardates[1])
    prev = int(prev['sent'])

    data = {}
    data['diff_netsent_'+str(days)] = last - prev
    data['percentage_sent_'+str(days)] = GetPercentageDiff(prev, last)
    data['sent_'+str(days)] = last
    return (data)

def GetDiffSendersDays (period, init_date, days):
    # This function provides the percentage in activity between two periods

    chardates = GetDates(init_date, days)
    last = StaticNumSenders(chardates[1], chardates[0])
    last = int(last['senders'])
    prev = StaticNumSenders(chardates[2], chardates[1])
    prev = int(prev['senders'])

    data = {}
    data['diff_netsenders_'+str(days)] = last - prev
    data['percentage_senders_'+str(days)] = GetPercentageDiff(prev, last)
    data['senders_'+str(days)] = last
    return (data)

def GetSentSummaryCompanies (period, startdate, enddate, identities_db, num_companies):
    count = 1
    first_companies = {}

    companies  = companiesNames(identities_db, startdate, enddate, ["-Bot", "-Individual", "-Unknown"])

    for company in companies:
        type_analysis = ["company", "'"+company+"'"]
        sent = EvolEmailsSent(period, startdate, enddate, identities_db, type_analysis)
        sent = completePeriodIds(sent, period, startdate, enddate)
        # Rename field sent to company name
        sent[company] = sent["sent"]
        del sent['sent']

        if (count <= num_companies):
            #Case of companies with entity in the dataset
            first_companies = dict(first_companies.items() + sent.items())
        else :
            #Case of companies that are aggregated in the field Others
            if 'Others' not in first_companies:
                first_companies['Others'] = sent[company]
            else:
                first_companies['Others'] = [a+b for a, b in zip(first_companies['Others'],sent[company])]
        count = count + 1

    first_companies = completePeriodIds(first_companies, period, startdate, enddate)

    return(first_companies)
