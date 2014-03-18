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
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acso@bitergia.com>

#############
# TODO: missing functions wrt 
#       evolution and agg values of countries and companies
#############

import logging, re

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from GrimoireSQL import ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, read_options, getPeriod, createJSON

from data_source import DataSource

class MLS(DataSource):

    @staticmethod
    def get_repo_field():
        return "mailing_list_url"

    @staticmethod
    def get_db_name():
        return "db_mlstats"

    @staticmethod
    def get_name(): return "MLS"

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, type_analysis):
        rfield = MLS.get_repo_field()
        return completePeriodIds(EvolMLSInfo (period, startdate, enddate, i_db, rfield, type_analysis))

    @staticmethod
    def get_agg_data (period, startdate, enddate, i_db, type_analysis):
        rfield = MLS.get_repo_field()
        return StaticMLSInfo (period, startdate, enddate, i_db, rfield, type_analysis)

    @staticmethod
    def get_top_data (period, startdate, enddate, identities_db, npeople):
        bots = MLS.get_bots()
        opts = read_options()

        top_senders_data = {}
        top_senders_data['senders.']=top_senders(0, startdate, enddate,identities_db,bots, npeople)
        top_senders_data['senders.last year']=top_senders(365, startdate, enddate,identities_db, bots, npeople)
        top_senders_data['senders.last month']=top_senders(31, startdate, enddate,identities_db,bots, npeople)
        createJSON (top_senders_data, opts.destdir+"/mls-top.json")

        return top_senders_data

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
        else:
            logging.error(filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, startdate, enddate, identities_db, bots):
        opts = read_options()
        period = getPeriod(opts.granularity)

        items = MLS.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return

        filter_name = filter_.get_name()
        filter_name_short = filter_.get_name_short()
        if (filter_name == "repository"): filter_name = "repos"

        if not isinstance(items, (list)):
            items = [items]

        items_files = [item.replace('/', '_').replace("<","__").replace(">","___")
            for item in items]

        createJSON(items_files, opts.destdir+"/mls-"+filter_name+".json")

        for item in items :
            item_name = "'"+ item+ "'"
            item_file = item.replace("/","_").replace("<","__").replace(">","___")
            logging.info (item_name)
            type_analysis = [filter_.get_name(), item_name]

            evol_data = MLS.get_evolutionary_data (period, startdate, enddate, identities_db, type_analysis)
            createJSON(evol_data, opts.destdir+"/"+item_file+"-mls-"+filter_name_short+"-evolutionary.json")

            agg = MLS.get_agg_data (period, startdate, enddate, identities_db, type_analysis)
            createJSON(agg, opts.destdir+"/"+item_file+"-mls-"+filter_name_short+"-static.json")

            if (filter_name == "company"):
                top_senders = companyTopSenders (item, identities_db, startdate, enddate, opts.npeople)
                createJSON(top_senders, opts.destdir+"/"+item+"-mls-"+filter_name_short+"-top-senders.json")

        if (filter_name == "company"):
            sent = GetSentSummaryCompanies(period, startdate, enddate, opts.identities_db, 10)
            createJSON (sent, opts.destdir+"/mls-sent-companies-summary.json")

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

    if (len(type_analysis) != 2): return From

    analysis = type_analysis[0]

    if analysis == 'repository': From = GetMLSSQLRepositoriesFrom()
    elif analysis == 'company': From = GetMLSSQLCompaniesFrom(identities_db)
    elif analysis == 'country': From = GetMLSSQLCountriesFrom(identities_db)
    elif analysis == 'domain': From = GetMLSSQLDomainsFrom(identities_db)

    return (From)



def GetMLSSQLReportWhere (type_analysis):
    #generic function to generate 'where' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    where = ""

    if (len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': where = GetMLSSQLRepositoriesWhere(value)
    elif analysis == 'company': where = GetMLSSQLCompaniesWhere(value)
    elif analysis == 'country': where = GetMLSSQLCountriesWhere(value)
    elif analysis == 'domain': where = GetMLSSQLDomainsWhere(value)

    return (where)


#########
# Other generic functions
#########

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

    if (evolutionary == True):
        sent = EvolEmailsSent(period, startdate, enddate, identities_db, type_analysis)
        sent = completePeriodIds(sent)
        senders = EvolMLSSenders(period, startdate, enddate, identities_db, type_analysis)
        senders = completePeriodIds(senders)
        repositories = EvolMLSRepositories(rfield, period, startdate, enddate, identities_db, type_analysis)
        repositories = completePeriodIds(repositories)
        threads = EvolThreads(period, startdate, enddate, identities_db, type_analysis)
        threads = completePeriodIds(threads)
        sent_response = EvolMLSResponses(period, startdate, enddate, identities_db, type_analysis)
        sent_response = completePeriodIds(sent_response)
        senders_response = EvolMLSSendersResponse(period, startdate, enddate, identities_db, type_analysis)
        senders_response = completePeriodIds(senders_response)
        senders_init = EvolMLSSendersInit(period, startdate, enddate, identities_db, type_analysis)
        senders_init = completePeriodIds(senders_init)
    else:
        sent = AggEmailsSent(period, startdate, enddate, identities_db, type_analysis)
        senders = AggMLSSenders(period, startdate, enddate, identities_db, type_analysis)
        repositories = AggMLSRepositories(rfield, period, startdate, enddate, identities_db, type_analysis)
        threads = AggThreads(period, startdate, enddate, identities_db, type_analysis)
        sent_response = AggMLSResponses(period, startdate, enddate, identities_db, type_analysis)
        senders_response = AggMLSSendersResponse(period, startdate, enddate, identities_db, type_analysis)
        senders_init = AggMLSSendersInit(period, startdate, enddate, identities_db, type_analysis)

    data = dict(sent.items() + senders.items()+ repositories.items())
    data = dict(data.items() + threads.items()+ sent_response.items())
    data = dict(data.items() + senders_response.items() + senders_init.items())

    return (data)

def EvolMLSInfo (period, startdate, enddate, identities_db, rfield, type_analysis = []):
    #Evolutionary info all merged in a dataframe
    return(GetMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis, True))

def StaticMLSInfo (period, startdate, enddate, identities_db, rfield, type_analysis = []):
    #Agg info all merged in a dataframe
    return(GetMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis, False))

#########
#Functions to obtain info per type of basic piece of data
#########

# All of the EvolXXX or StaticXXX contains the same parameters:
#    period:
#    startdate:
#    enddate:
#    identities_db: MySQL database name
#    type_analysis: tuple with two values: typeof and value
#                   typeof = 'companies', 'countries', 'repositories' or ''
#                   value = any value that corresponds with the type of analysis


# Emails Sent
def GetEmailsSent (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts emails sent

    if (evolutionary):
        fields = " count(distinct(m.message_ID)) as sent "
    else:
        fields = " count(distinct(m.message_ID)) as sent, "+\
                  " DATE_FORMAT (min(m.first_date), '%Y-%m-%d') as first_date, "+\
                  " DATE_FORMAT (max(m.first_date), '%Y-%m-%d') as last_date "

    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis)

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolEmailsSent (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of emails sent
    return(GetEmailsSent(period, startdate, enddate, identities_db, type_analysis , True))


def AggEmailsSent (period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails sent
    return(GetEmailsSent(period, startdate, enddate, identities_db, type_analysis, False))


# People sending emails
def GetMLSSenders (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    #Generic function that counts people sending messages

    fields = " count(distinct(pup.upeople_id)) as senders "
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    if (tables == " messages m "):
        # basic case: it's needed to add unique ids filters
        tables = tables + ", messages_people mp, people_upeople pup "
        filters = GetMLSFiltersOwnUniqueIdsMLS()
    else:
        #not sure if this line is useful anymore...
        filters = GetMLSSQLReportWhere(type_analysis)

    if (type_analysis and type_analysis[0] == "repository"):
        #Adding people_upeople table
        tables += ",  messages_people mp, people_upeople pup "
        filters += " and m.message_ID = mp.message_id and "+\
                   "mp.email_address = pup.people_id and "+\
                   "mp.type_of_recipient=\'From\' "

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


def EvolMLSSenders (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of people sending emails
    return(GetMLSSenders(period, startdate, enddate, identities_db, type_analysis , True))

def AggMLSSenders (period, startdate, enddate, identities_db, type_analysis = []):
    # Agg of people sending emails
    return(GetMLSSenders(period, startdate, enddate, identities_db, type_analysis , False))

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

# People answering in a thread

def GetMLSSendersResponse (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    #Generic function that counts people sending messages

    fields = " count(distinct(pup.upeople_id)) as senders_response "
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    if (tables == " messages m "):
        # basic case: it's needed to add unique ids filters
        tables += ", messages_people mp, people_upeople pup "
        filters = GetMLSFiltersOwnUniqueIdsMLS()
    else:
        filters = GetMLSSQLReportWhere(type_analysis)

    if (type_analysis and type_analysis[0] == "repository"):
        #Adding people_upeople table
        tables += ",  messages_people mp, people_upeople pup "
        filters += " and m.message_ID = mp.message_id and "+\
                   "mp.email_address = pup.people_id and "+\
                   "mp.type_of_recipient=\'From\' "
    filters += " and m.is_response_of is not null "

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


def EvolMLSSendersResponse (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of people sending emails
    return(GetMLSSendersResponse(period, startdate, enddate, identities_db, type_analysis , True))


def AggMLSSendersResponse (period, startdate, enddate, identities_db, type_analysis = []):
    # Agg of people sending emails
    return(GetMLSSendersResponse(period, startdate, enddate, identities_db, type_analysis , False))


# People starting threads

def GetMLSSendersInit (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    #Generic function that counts people sending messages

    fields = " count(distinct(pup.upeople_id)) as senders_init "
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    if (tables == " messages m "):
        # basic case: it's needed to add unique ids filters
        tables += ", messages_people mp, people_upeople pup "
        filters = GetMLSFiltersOwnUniqueIdsMLS()
    else:
        filters = GetMLSSQLReportWhere(type_analysis)

    if (type_analysis and type_analysis[0] == "repository"):
        #Adding people_upeople table
        tables += ",  messages_people mp, people_upeople pup "
        filters += " and m.message_ID = mp.message_id and "+\
                   " mp.email_address = pup.people_id and "+\
                   " mp.type_of_recipient=\'From\' "
    filters += " and m.is_response_of is null "

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


def EvolMLSSendersInit (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of people sending emails
    return(GetMLSSendersInit(period, startdate, enddate, identities_db, type_analysis , True))


def AggMLSSendersInit (period, startdate, enddate, identities_db, type_analysis = []):
    # Agg of people sending emails
    return(GetMLSSendersInit(period, startdate, enddate, identities_db, type_analysis , False))

# Threads
def GetThreads (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts threads

    fields = " count(distinct(m.is_response_of)) as threads"
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis)

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


def EvolThreads (period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails sent
    return(GetThreads(period, startdate, enddate, identities_db, type_analysis, True))


def AggThreads (period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails sent
    return(GetThreads(period, startdate, enddate, identities_db, type_analysis, False))

# Repositories
def GetMLSRepositories (rfield, period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts threads

    fields = " COUNT(DISTINCT("+rfield+")) AS repositories  "
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis)

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


def EvolMLSRepositories (rfield, period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails sent
    return(GetMLSRepositories(rfield, period, startdate, enddate, identities_db, type_analysis, True))


def AggMLSRepositories (rfield, period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails sent
    return(GetMLSRepositories(rfield, period, startdate, enddate, identities_db, type_analysis, False))

# Messages replying a thread
def GetMLSResponses (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts replies

    fields = " count(distinct(m.message_ID)) as sent_response"
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis) + " and m.is_response_of is not null "

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolMLSResponses (period, startdate, enddate, identities_db, type_analysis = []):
    # Evol number of replies
    return(GetMLSResponses(period, startdate, enddate, identities_db, type_analysis, True))

def AggMLSResponses (period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails replied
    return(GetMLSResponses(period, startdate, enddate, identities_db, type_analysis, False))

# Messages starting threads
def GetMLSInit (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts replies

    fields = " count(distinct(m.message_ID)) as sent_init"
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis) + " m.is_response_of is null "

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolMLSInit (period, startdate, enddate, identities_db, type_analysis = []):
    # Evol number of messages starting a thread
    return(GetMLSInit(period, startdate, enddate, identities_db, type_analysis, True))

def AggMLSInit (period, startdate, enddate, identities_db, type_analysis = []):
    # Aggregated number of emails starting a thread
    return(GetMLSInit(period, startdate, enddate, identities_db, type_analysis, False))


def GetMLSStudies (period, startdate, enddate, identities_db, type_analysis, evolutionary, study):
    # Generic function that counts evolution/agg number of specific studies with similar
    # database schema such as domains, companies and countries

    fields = ' count(distinct(name)) as ' + study
    tables = " messages m " + GetMLSSQLReportFrom(identities_db, type_analysis)
    filters = GetMLSSQLReportWhere(type_analysis) + " and m.is_response_of is null "

    #Filtering last part of the query, not used in this case
    #filters = gsub("and\n( )+(d|c|cou|com).name =.*$", "", filters)

    q = BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
    q = re.sub(r'(d|c|cou|com).name.*and', "", q)

    data = ExecuteQuery(q)
    return(data)


def EvolMLSDomains (period, startdate, enddate, identities_db):
    # Evol number of domains used
    return(GetMLSStudies(period, startdate, enddate, identities_db, ['domain', ''], True, 'domains'))


def EvolMLSCountries (period, startdate, enddate, identities_db):
    # Evol number of countries
    return(GetMLSStudies(period, startdate, enddate, identities_db, ['country', ''], True, 'countries'))


def EvolMLSCompanies (period, startdate, enddate, identities_db):
    # Evol number of companies
    data = GetMLSStudies(period, startdate, enddate, identities_db, ['company', ''], True, 'companies')
    return(data)

def AggMLSDomains (period, startdate, enddate, identities_db):
    # Agg number of domains
    return(GetMLSStudies(period, startdate, enddate, identities_db, ['domain', ''], False, 'domains'))

def AggMLSCountries (period, startdate, enddate, identities_db):
    # Agg number of countries
    return(GetMLSStudies(period, startdate, enddate, identities_db, ['country', ''], False, 'countries'))

def AggMLSCompanies (period, startdate, enddate, identities_db):
    # Agg number of companies
    return(GetMLSStudies(period, startdate, enddate, identities_db, ['company', ''], False, 'companies'))


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


def top_senders (days, startdate, enddate, identities_db, filter_, limit) :

    affiliations = ""
    if (not filter_): filter_ = []
    for aff in filter_:
        affiliations = affiliations+ " c.name<>'"+ aff +"' and "

    date_limit = ""
    if (days != 0 ) :
        sql = "SELECT @maxdate:=max(first_date) from messages limit 1"
        ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate,first_date)<"+str(days)

    q = "SELECT up.id as id, up.identifier as senders, "+\
            "COUNT(distinct(m.message_id)) as sent "+\
            "FROM "+ GetTablesCompanies(identities_db)+\
            " ,"+identities_db+".upeople up "+\
            "WHERE "+ GetFiltersCompanies()+ " AND "+\
            "  pup.upeople_id = up.id AND "+\
            "  "+ affiliations + " "+\
            "  m.first_date >= "+startdate+" AND "+\
            "  m.first_date < "+enddate +\
            date_limit+ " "+\
            "GROUP BY up.identifier "+\
            "ORDER BY sent desc "+\
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
            "  "+rfield+"='"+repo+"' "+\
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
        sent = completePeriodIds(sent)
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

    first_companies = completePeriodIds(first_companies)

    return(first_companies)
