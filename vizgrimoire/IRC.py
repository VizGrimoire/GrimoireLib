#!/usr/bin/env python

# Copyright (C) 2014 Bitergia
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>
#     Daniel Izquierdo <dizquierdo@bitergia.com>

import logging, os

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod, ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, getPeriod, createJSON, completePeriodIds
from data_source import DataSource
from filter import Filter
from metrics_filter import MetricFilters


class IRC(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_irc"

    @staticmethod
    def get_name(): return "irc"

    @staticmethod
    def get_date_init(startdate, enddate, identities_db, type_analysis):
        """Get the date of the first activity in the data source"""
        return GetInitDate (startdate, enddate, identities_db, type_analysis)

    @staticmethod
    def get_date_end(startdate, enddate, identities_db, type_analysis):
        """Get the date of the last activity in the data source"""
        return GetEndDate (startdate, enddate, identities_db, type_analysis)

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        metrics = DataSource.get_metrics_data(IRC, period, startdate, enddate, identities_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(IRC, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, identities_db, filter_ = None):
        data =  IRC.get_evolutionary_data (period, startdate, enddate, identities_db, filter_)
        filename = IRC().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        metrics = DataSource.get_metrics_data(IRC, period, startdate, enddate, identities_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_metrics_data(IRC, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data = IRC.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = IRC().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        top = {}
        msenders = DataSource.get_metrics("senders", IRC)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top['senders.'] = msenders.get_list(mfilter, 0)
            top['senders.last month'] = msenders.get_list(mfilter, 31)
            top['senders.last year'] = msenders.get_list(mfilter, 365)
        else:
            logging.info("IRC does not support yet top for filters.")

        return(top)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = IRC.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+IRC().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            metric = DataSource.get_metrics("repositories", IRC)
            items = metric.get_list()
            # items = GetReposNameIRC()
        else:
            logging.error("IRC " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        items = IRC.get_filter_items(filter_, startdate, enddate, identities_db)
        if (items == None): return

        if not isinstance(items, (list)):
            items = [items]

        fn = os.path.join(destdir, filter_.get_filename(IRC()))
        createJSON(items, fn)

        for item in items :
            # item_name = "'"+ item+ "'"
            logging.info (item)

            filter_item = Filter(filter_.get_name(), item)

            evol_data = IRC.get_evolutionary_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(IRC()))
            createJSON(completePeriodIds(evol_data, period, startdate, enddate), fn)

            agg = IRC.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(IRC()))
            createJSON(agg, fn)

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        top_data = IRC.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['senders.']["id"]
        top += top_data['senders.last year']["id"]
        top += top_data['senders.last month']["id"]
        # remove duplicates
        people = list(set(top)) 
        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        evol = GetEvolPeopleIRC(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol


    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        return GetStaticPeopleIRC(upeople_id, startdate, enddate)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_query_builder():
        from query_builder import IRCQuery
        return IRCQuery

    @staticmethod
    def get_metrics_core_agg():
        return ['sent', 'senders', 'repositories']

    @staticmethod
    def get_metrics_core_ts():
        return ['sent', 'senders', 'repositories']

    @staticmethod
    def get_metrics_core_trends():
        return ['sent', 'senders']


def GetDate (startdate, enddate, identities_db, type_analysis, type):
    # date of submmitted issues (type= max or min)
    if (type=="max"):
        fields = " DATE_FORMAT (max(date), '%Y-%m-%d') as last_date"
    else :
        fields = " DATE_FORMAT (min(date), '%Y-%m-%d') as first_date"

    tables = " irclog i " + GetIRCSQLReportFrom(identities_db, type_analysis)
    filters = GetIRCSQLReportWhere(type_analysis)

    q = BuildQuery(None, startdate, enddate, " i.date ", fields, tables, filters, False)
    data = ExecuteQuery(q)
    return(data)

def GetInitDate (startdate, enddate, identities_db, type_analysis):
    #Initial date of submitted issues
    return(GetDate(startdate, enddate, identities_db, type_analysis, "min"))

def GetEndDate (startdate, enddate, identities_db, type_analysis):
    #End date of submitted issues
    return(GetDate(startdate, enddate, identities_db, type_analysis, "max"))


# SQL Metaqueries
def GetIRCSQLRepositoriesFrom ():
    # tables necessary for repositories
    return (", channels c")


def GetIRCSQLRepositoriesWhere(repository):
    # filters necessaries for repositories
    return (" i.channel_id = c.id and c.name=" + repository)


def GetIRCSQLCompaniesFrom (i_db):
    # tables necessary to companies analysis
    return(" , people_upeople pup, "+\
                   i_db+"companies c, "+\
                   i_db+".upeople_companies upc")


def GetIRCSQLCompaniesWhere(name):
    # filters necessary to companies analysis
    return(" i.nick = pup.people_id and "+\
           "pup.upeople_id = upc.upeople_id and "+\
           "upc.company_id = c.id and "+\
           "i.submitted_on >= upc.init and "+\
           "i.submitted_on < upc.end and "+\
           "c.name = " + name)


def GetIRCSQLCountriesFrom (i_db):
    # tables necessary to countries analysis
    return(" , people_upeople pup, "+\
           i_db+".countries c, "+\
           i_db+".upeople_countries upc")


def GetIRCSQLCountriesWhere(name):
    # filters necessary to countries analysis
    return(" i.nick = pup.people_id and "+\
           "pup.upeople_id = upc.upeople_id and "+\
           "upc.country_id = c.id and "+\
           "c.name = " + name)

def GetIRCSQLDomainsFrom (i_db):
    # tables necessary to domains analysis
    return(" , people_upeople pup, "+\
           i_db+".domains d, "+\
           i_db+".upeople_domains upd")

def GetIRCSQLDomainsWhere (name):
    # filters necessary to domains analysis
    return(" i.nick = pup.people_id and "+\
           "pup.upeople_id = upd.upeople_id and "+\
           "upd.domain_id = d.id and "+\
           "d.name = " + name)

def GetTablesOwnUniqueIdsIRC () :
    tables = 'irclog, people_upeople pup'
    return (tables)

def GetFiltersOwnUniqueIdsIRC () :
    filters = 'pup.people_id = irclog.nick'
    return (filters) 

##########
#Generic functions to obtain FROM and WHERE clauses per type of report
##########

def GetIRCSQLReportFrom (identities_db, type_analysis):
    #generic function to generate 'from' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    From = ""

    if (type_analysis is None or len(type_analysis) != 2): return From

    analysis = type_analysis[0]

    if analysis == 'repository': From = GetIRCSQLRepositoriesFrom()
    elif analysis == 'company': From = GetIRCSQLCompaniesFrom(identities_db)
    elif analysis == 'country': From = GetIRCSQLCountriesFrom(identities_db)
    elif analysis == 'domain': From = GetIRCSQLDomainsFrom(identities_db)

    return (From)



def GetIRCSQLReportWhere (type_analysis):
    #generic function to generate 'where' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    where = ""

    if (type_analysis is None or len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': where = GetIRCSQLRepositoriesWhere(value)
    elif analysis == 'company': where = GetIRCSQLCompaniesWhere(value)
    elif analysis == 'country': where = GetIRCSQLCountriesWhere(value)
    elif analysis == 'domain': where = GetIRCSQLDomainsWhere(value)

    return (where)
#
# Repositories (channels)
#

def GetTablesReposIRC () :
    return (GetTablesOwnUniqueIdsIRC()+",channels c")


def GetFiltersReposIRC () :
    filters = GetFiltersOwnUniqueIdsIRC() +" AND c.id = irclog.channel_id"
    return(filters)

#########
# PEOPLE
#########
def GetListPeopleIRC (startdate, enddate) :
    fields = "DISTINCT(pup.upeople_id) as id, count(irclog.id) total"
    tables = GetTablesOwnUniqueIdsIRC()
    filters = GetFiltersOwnUniqueIdsIRC()
    filters += " AND irclog.type='COMMENT' "
    filters += " GROUP BY nick ORDER BY total desc"
    q = GetSQLGlobal('date',fields,tables, filters, startdate, enddate)
    return(ExecuteQuery(q))

def GetQueryPeopleIRC (developer_id, period, startdate, enddate, evol):
    fields = "COUNT(irclog.id) AS sent"
    tables = GetTablesOwnUniqueIdsIRC()
    filters = GetFiltersOwnUniqueIdsIRC() + " AND pup.upeople_id = " + str(developer_id)
    filters += " AND irclog.type='COMMENT'"

    if (evol) :
        q = GetSQLPeriod(period,'date', fields, tables, filters,
                startdate, enddate)
    else:
        fields = fields + \
                ",DATE_FORMAT (min(date),'%Y-%m-%d') as first_date,"+\
                " DATE_FORMAT (max(date),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('date', fields, tables, filters,
                startdate, enddate)
    return (q)

def GetEvolPeopleIRC (developer_id, period, startdate, enddate) :
    q = GetQueryPeopleIRC(developer_id, period, startdate, enddate, True)
    return(ExecuteQuery(q))


def GetStaticPeopleIRC (developer_id, startdate, enddate) :
    q = GetQueryPeopleIRC(developer_id, None, startdate, enddate, False)
    return(ExecuteQuery(q))

def GetPeopleIRC():
    # Returns the ids of the IRC participants
    q = "SELECT DISTINCT(upeople_id) AS members FROM people_upeople"
    data = ExecuteQuery(q)
    return(data['members'])    

##############
# Microstudies
##############

def GetIRCDiffSentDays (period, init_date, days):
    # This function provides the percentage in activity between two periods.
    #
    # The netvalue indicates if this is an increment (positive value) or decrement (negative value)

    chardates = GetDates(init_date, days)
    lastmessages = StaticNumSentIRC(period, chardates[1], chardates[0])
    lastmessages = int(lastmessages['sent'])
    prevmessages = StaticNumSentIRC(period, chardates[2], chardates[1])
    prevmessages = int(prevmessages['sent'])

    data = {}
    data['diff_netsent_'+str(days)] = lastmessages - prevmessages
    data['percentage_sent_'+str(days)] = GetPercentageDiff(prevmessages, lastmessages)
    data['sent_'+str(days)] = lastmessages

    return data

def GetIRCDiffSendersDays (period, init_date, identities_db=None, days = None):
    # This function provides the percentage in activity between two periods:
    # Fixme: equal to GetDiffAuthorsDays

    chardates = GetDates(init_date, days)
    lastsenders = StaticNumSendersIRC(period, chardates[1], chardates[0], identities_db)
    lastsenders = int(lastsenders['senders'])
    prevsenders = StaticNumSendersIRC(period, chardates[2], chardates[1], identities_db)
    prevsenders = int(prevsenders['senders'])

    data = {}
    data['diff_netsenders_'+str(days)] = lastsenders - prevsenders
    data['percentage_senders_'+str(days)] = GetPercentageDiff(prevsenders, lastsenders)
    data['senders_'+str(days)] = lastsenders

    return data
