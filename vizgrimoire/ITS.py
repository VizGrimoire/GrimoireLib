## Copyright (C) 2012, 2013 Bitergia
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
## ITS.R
##
## Queries for ITS data analysis
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>
##   Luis Canas-Diaz <lcanas@bitergia.com>

import logging, os, re

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from GrimoireSQL import ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, read_options, getPeriod
from GrimoireUtils import createJSON

from data_source import DataSource
from filter import Filter
import report

class ITS(DataSource):

    @staticmethod
    def get_db_name():
        return "db_bicho"

    @staticmethod
    def get_name(): return "its"

    @staticmethod
    def _get_backend():
        automator = report.Report.get_config()
        its_backend = automator['bicho']['backend']
        backend = Backend(its_backend)
        return backend

    @staticmethod
    def _get_closed_condition():
        return ITS._get_backend().closed_condition

    @staticmethod
    def get_tickets_states(period, startdate, enddate, identities_db, backend):

        from rpy2.robjects.packages import importr
        from GrimoireUtils import dataFrame2Dict

        vizr = importr("vizgrimoire")

        evol = {}
        return evol

        for status in backend.statuses:
            logging.info ("Working with ticket status: " + status)
            #Evolution of the backlog
            tickets_status = vizr.GetEvolBacklogTickets(period, startdate, enddate, status, backend.name_log_table)
            tickets_status = dataFrame2Dict(tickets_status)
            tickets_status = completePeriodIds(tickets_status)
            # rename key
            tickets_status[status] = tickets_status.pop("pending_tickets")
            #Issues per status
            current_status = vizr.GetCurrentStatus(period, startdate, enddate, identities_db, status)
            current_status = completePeriodIds(dataFrame2Dict(current_status))
            #Merging data
            evol = dict(evol.items() + current_status.items() + tickets_status.items())
        return evol

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        closed_condition = ITS._get_closed_condition()

        if (filter_ is not None):
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]
            evol = EvolITSInfo(period, startdate, enddate, identities_db, 
                               type_analysis, closed_condition)

        else:
            data = EvolITSInfo(period, startdate, enddate, identities_db, None, closed_condition)
            evol = completePeriodIds(data)

            data = EvolIssuesCompanies(period, startdate, enddate, identities_db)
            evol = dict(evol.items() + completePeriodIds(data).items())

            data = EvolIssuesCountries(period, startdate, enddate, identities_db)
            evol = dict(evol.items() + completePeriodIds(data).items())

            data = EvolIssuesRepositories(period, startdate, enddate, identities_db)
            evol = dict(evol.items() + completePeriodIds(data).items())

            data = EvolIssuesDomains(period, startdate, enddate, identities_db)
            evol = dict(evol.items() + completePeriodIds(data).items())

            data = ITS.get_tickets_states(period, startdate, enddate, identities_db, ITS._get_backend())
            evol = dict(evol.items() + data.items())

        return evol

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, i_db, filter_ = None):
        opts = read_options()
        data =  ITS.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = ITS().get_evolutionary_filename()
        createJSON (data, os.path.join(opts.destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        closed_condition = ITS._get_closed_condition()

        if (filter_ is not None):
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]
            data = AggITSInfo(period, startdate, enddate, identities_db, type_analysis, closed_condition)
            agg = data

        else:
            agg = AggITSInfo(period, startdate, enddate, identities_db, None, closed_condition)
            data = AggAllParticipants(startdate, enddate)
            agg = dict(agg.items() +  data.items())
            data = TrackerURL()
            agg = dict(agg.items() +  data.items())

            data = AggIssuesCompanies(period, startdate, enddate, identities_db)
            agg = dict(agg.items() + data.items())

            data = AggIssuesCountries(period, startdate, enddate, identities_db)
            agg = dict(agg.items() + data.items())

            data = AggIssuesDomains(period, startdate, enddate, identities_db)
            agg = dict(agg.items() + data.items())

            # Tendencies    
            for i in [7,30,365]:
                period_data = GetDiffClosedDays(period, identities_db, enddate, i, [], closed_condition)
                agg = dict(agg.items() + period_data.items())
                period_data = GetDiffOpenedDays(period, identities_db, enddate, i, [])
                agg = dict(agg.items() + period_data.items())
                period_data = GetDiffClosersDays(period, identities_db, enddate, i, [], closed_condition)
                agg = dict(agg.items() + period_data.items())
                period_data = GetDiffChangersDays(period, identities_db, enddate, i, [])
                agg = dict(agg.items() + period_data.items())

            # Last Activity: to be removed
            for i in [7,14,30,60,90,180,365,730]:
                period_activity = GetLastActivityITS(i, closed_condition)
                agg = dict(agg.items() + period_activity.items())

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, i_db, filter_ = None):
        opts = read_options()
        data = ITS.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = ITS().get_agg_filename()
        createJSON (data, os.path.join(opts.destdir, filename))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = ITS.get_bots()
        closed_condition =  ITS._get_closed_condition()
        top = None

        if filter_ is None:
            top_closers_data = {}
            top_closers_data['closers.']=GetTopClosers(0, startdate, enddate,identities_db, bots, closed_condition, npeople)
            top_closers_data['closers.last year']=GetTopClosers(365, startdate, enddate,identities_db, bots, closed_condition, npeople)
            top_closers_data['closers.last month']=GetTopClosers(31, startdate, enddate,identities_db, bots, closed_condition, npeople)

            top_openers_data = {}
            top_openers_data['openers.']=GetTopOpeners(0, startdate, enddate,identities_db, bots, closed_condition, npeople)
            top_openers_data['openers.last year']=GetTopOpeners(365, startdate, enddate,identities_db, bots, closed_condition, npeople)
            top_openers_data['openers.last month']=GetTopOpeners(31, startdate, enddate,identities_db, bots, closed_condition, npeople)

            top = dict(top_closers_data.items() + top_openers_data.items())

        else:
            filter_name = filter_.get_name()
            item = "'"+filter_.get_item()+"'"

            if (filter_name == "company"):
                top = GetCompanyTopClosers(item, startdate, enddate, identities_db, bots, closed_condition, npeople)
            elif (filter_name == "domain"):
                top = GetDomainTopClosers(item, startdate, enddate, identities_db, bots, closed_condition, npeople)
            else:
                top = None

        return top

    @staticmethod
    def create_top_report (startdate, enddate, i_db):
        opts = read_options()
        data = ITS.get_top_data (startdate, enddate, i_db, None, opts.npeople)
        createJSON (data, opts.destdir+"/"+ITS().get_top_filename())

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            items  = GetReposNameITS(startdate, enddate)
        elif (filter_name == "company"):
            items  = GetCompaniesNameITS(startdate, enddate, identities_db, ITS._get_closed_condition(), bots)
        elif (filter_name == "country"):
            items = GetCountriesNamesITS(startdate, enddate, identities_db, ITS._get_closed_condition())
        elif (filter_name == "domain"):
            items = GetDomainsNameITS(startdate, enddate, identities_db, ITS._get_closed_condition(), bots)
        else:
            logging.error(filter_name + " not supported")
        return items

    @staticmethod
    def get_filter_summary(filter_, period, startdate, enddate, identities_db, limit):
        summary = None
        filter_name = filter_.get_name()
        closed_condition =  ITS._get_closed_condition()

        if (filter_name == "company"):
            summary =  GetClosedSummaryCompanies(period, startdate, enddate, identities_db, closed_condition, limit)
        return summary

    @staticmethod
    def create_filter_report(filter_, startdate, enddate, identities_db, bots):
        opts = read_options()
        period = getPeriod(opts.granularity)

        items = ITS.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return
        items = items['name']

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        fn = os.path.join(opts.destdir, filter_.get_filename(ITS()))
        createJSON(items, fn)

        for item in items :
            item_name = "'"+ item+ "'"
            logging.info (item_name)
            filter_item = Filter(filter_name, item)

            evol_data = ITS.get_evolutionary_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(opts.destdir, filter_item.get_evolutionary_filename(ITS()))
            createJSON(evol_data, fn)

            agg = ITS.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(opts.destdir, filter_item.get_static_filename(ITS()))
            createJSON(agg, fn)

            if (filter_name in ["company","domain"]):
                top = ITS.get_top_data(startdate, enddate, identities_db, filter_item, opts.npeople)
                fn = os.path.join(opts.destdir, filter_item.get_top_filename(ITS()))
                createJSON(top, fn)

        if (filter_name == "company"):
            closed = ITS.get_filter_summary(filter_, period, startdate, enddate, identities_db, 10)
            createJSON (closed, opts.destdir+"/"+ filter_.get_summary_filename(ITS))

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        top_data = ITS.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['closers.']["id"]
        top += top_data['closers.last year']["id"]
        top += top_data['closers.last month']["id"]
        top += top_data['openers.']["id"]
        top += top_data['openers.last year']["id"]
        top += top_data['openers.last month']["id"]
        # remove duplicates
        people = list(set(top))

        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        closed_condition =  ITS._get_closed_condition()

        evol = GetPeopleEvolITS(upeople_id, period, startdate, enddate, closed_condition)
        evol = completePeriodIds(evol)
        return evol

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        closed_condition =  ITS._get_closed_condition()
        return GetPeopleStaticITS(upeople_id, startdate, enddate, closed_condition)

    @staticmethod
    def create_r_reports(vizr, enddate):
        opts = read_options()

        # Time to Close: Other backends not yet supported
        vizr.ReportTimeToCloseITS(opts.backend, opts.destdir)

        # Demographics
        vizr.ReportDemographicsAgingITS(opts.enddate, opts.destdir)
        vizr.ReportDemographicsBirthITS(opts.enddate, opts.destdir)

        # Markov
        vizr.ReportMarkovChain(opts.destdir)

##############
# Specific FROM and WHERE clauses per type of report
##############

def GetITSSQLRepositoriesFrom ():
    # tables necessary for repositories 
    return (", trackers t")

def GetITSSQLRepositoriesWhere (repository):
    # fields necessary to match info among tables
    return (" i.tracker_id = t.id and t.url = "+repository+" ")

def GetITSSQLCompaniesFrom (i_db):
    # fields necessary for the companies analysis

    return(" , people_upeople pup, "+\
           i_db+".companies c, "+\
           i_db+".upeople_companies upc")

def GetITSSQLCompaniesWhere (name):
    # filters for the companies analysis
    return(" i.submitted_by = pup.people_id and "+\
           "pup.upeople_id = upc.upeople_id and "+\
           "upc.company_id = c.id and "+\
           "i.submitted_on >= upc.init and "+\
           "i.submitted_on < upc.end and "+\
           "c.name = "+name)

def GetITSSQLCountriesFrom (i_db):
    # fields necessary for the countries analysis

    return(" , people_upeople pup, "+\
           i_db+".countries c, "+\
           i_db+".upeople_countries upc")

def GetITSSQLCountriesWhere (name):
    # filters for the countries analysis
    return(" i.submitted_by = pup.people_id and "+\
           "pup.upeople_id = upc.upeople_id and "+\
           "upc.country_id = c.id and "+\
           "c.name = "+name)


def GetITSSQLDomainsFrom (i_db):
    # fields necessary for the domains analysis

    return(" , people_upeople pup, "+\
           i_db+".domains d, "+\
           i_db+".upeople_domains upd")


def GetITSSQLDomainsWhere (name):
    # filters for the domains analysis
    return(" i.submitted_by = pup.people_id and "+\
           "pup.upeople_id = upd.upeople_id and "+\
           "upd.domain_id = d.id and "+\
           "d.name = "+name)

##########
#Generic functions to obtain FROM and WHERE clauses per type of report
##########

def GetITSSQLReportFrom (identities_db, type_analysis):
    #generic function to generate 'from' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    From = ""

    if (type_analysis is None or len(type_analysis) != 2): return From

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': From = GetITSSQLRepositoriesFrom()
    elif analysis == 'company': From = GetITSSQLCompaniesFrom(identities_db)
    elif analysis == 'country': From = GetITSSQLCountriesFrom(identities_db)
    elif analysis == 'domain': From = GetITSSQLDomainsFrom(identities_db)

    return (From)

def GetITSSQLReportWhere (type_analysis):
    #generic function to generate 'where' clauses

    #"type" is a list of two values: type of analysis and value of 
    #such analysis
    where = ""

    if (type_analysis is None or len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': where = GetITSSQLRepositoriesWhere(value)
    elif analysis == 'company': where = GetITSSQLCompaniesWhere(value)
    elif analysis == 'country': where = GetITSSQLCountriesWhere(value)
    elif analysis == 'domain': where = GetITSSQLDomainsWhere(value)

    return (where)

##########
# Meta functions to retrieve data
##########

def GetITSInfo (period, startdate, enddate, identities_db, type_analysis, closed_condition, evolutionary):
    # Meta function to aggregate all of the evolutionary or
    # aggregated functions

    data = {}

    if (evolutionary):
        closed = EvolIssuesClosed(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        closed = completePeriodIds(closed)
        closers = EvolIssuesClosers(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        closers = completePeriodIds(closers)
        changed = EvolIssuesChanged(period, startdate, enddate, identities_db, type_analysis)
        changed = completePeriodIds(changed)
        changers = EvolIssuesChangers(period, startdate, enddate, identities_db, type_analysis)
        changers = completePeriodIds(changers)
        open = EvolIssuesOpened(period, startdate, enddate, identities_db, type_analysis)
        open = completePeriodIds(open)
        openers = EvolIssuesOpeners(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        openers = completePeriodIds(openers)
        repos = EvolIssuesRepositories(period, startdate, enddate, identities_db, type_analysis)
        repos = completePeriodIds(repos)
    else :
        closed = AggIssuesClosed(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        closers = AggIssuesClosers(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        changed = AggIssuesChanged(period, startdate, enddate, identities_db, type_analysis)
        changers = AggIssuesChangers(period, startdate, enddate, identities_db, type_analysis)
        open = AggIssuesOpened(period, startdate, enddate, identities_db, type_analysis)
        openers = AggIssuesOpeners(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        repos = AggIssuesRepositories(period, startdate, enddate, identities_db, type_analysis)
        init_date = GetInitDate(startdate, enddate, identities_db, type_analysis)
        end_date = GetEndDate(startdate, enddate, identities_db, type_analysis)

    data = dict(closed.items() + closers.items()+ changed.items())
    data = dict(data.items() + changers.items() + open.items())
    data = dict(data.items() + openers.items() + repos.items())
    if (not evolutionary):
        data = dict(data.items() + init_date.items() + end_date.items())

    return(data)

def EvolITSInfo (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    #Evolutionary info all merged in a dataframe
    return(GetITSInfo(period, startdate, enddate, identities_db, type_analysis, closed_condition, True))

def AggITSInfo (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    #Agg info all merged in a dataframe
    return(GetITSInfo(period, startdate, enddate, identities_db, type_analysis, closed_condition, False))

#TODO: check the differences between function GetCurrentOpened and GetEvolClosed,
# GetEvolOpened, etc... in some cases such as opened, openers, closed, closers, 
# changed and changers is more than enough to just count changes in table changes
# opened when the issue was submitted (and submitted by) and closers providing the
# closed condition. Do we get whe same results if using the Backlog table?

def GetOpened (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    #This function returns the evolution or agg number of opened issues
    #This function can be also reproduced using the Backlog function.
    #However this function is less time expensive.
    fields = " count(distinct(i.id)) as opened "
    tables = " issues i "+ GetITSSQLReportFrom(identities_db, type_analysis)
    filters = GetITSSQLReportWhere(type_analysis)
    q = BuildQuery(period, startdate, enddate, " submitted_on ", fields, tables, filters, evolutionary)

    data = ExecuteQuery(q)
    return (data)

def AggIssuesOpened (period, startdate, enddate, identities_db, type_analysis):
    # Returns aggregated number of opened issues
    return(GetOpened(period, startdate, enddate, identities_db, type_analysis, False))

def EvolIssuesOpened (period, startdate, enddate, identities_db, type_analysis):
    #return(GetEvolBacklogTickets(period, startdate, enddate, status, name.logtable, filter))
    return(GetOpened(period, startdate, enddate, identities_db, type_analysis, True))

def GetOpeners (period, startdate, enddate, identities_db, type_analysis, evolutionary, closed_condition):
    #This function returns the evolution or agg number of people opening issues
    fields = " count(distinct(pup.upeople_id)) as openers "
    tables = " issues i " + GetITSSQLReportFrom(identities_db, type_analysis)
    filters = GetITSSQLReportWhere(type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ", people_upeople pup"
        filters += " and i.submitted_by = pup.people_id"
    elif (type_analysis[0] == "repository"):
        #Adding people_upeople table
        tables += ", people_upeople pup"
        filters += " and i.submitted_by = pup.people_id "

    q = BuildQuery(period, startdate, enddate, " submitted_on ", fields, tables, filters, evolutionary)

    data = ExecuteQuery(q)
    return (data)


def AggIssuesOpeners (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    # Returns aggregated number of opened issues
    return(GetOpeners(period, startdate, enddate, identities_db, type_analysis, False, closed_condition))

def EvolIssuesOpeners (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    #return(GetEvolBacklogTickets(period, startdate, enddate, status, name.logtable, filter))
    return(GetOpeners(period, startdate, enddate, identities_db, type_analysis, True, closed_condition))

def GetClosed (period, startdate, enddate, identities_db, type_analysis, evolutionary, closed_condition):
    #This function returns the evolution or agg number of closed issues
    #This function can be also reproduced using the Backlog function.
    #However this function is less time expensive.
    fields = " count(distinct(i.id)) as closed "
    tables = " issues i, changes ch " + GetITSSQLReportFrom(identities_db, type_analysis)

    filters = " i.id = ch.issue_id and " + closed_condition 
    filters_ext = GetITSSQLReportWhere(type_analysis)
    if (filters_ext != ""):
        filters += " and " + filters_ext
    #Action needed to replace issues filters by changes one
    filters = filters.replace("i.submitted", "ch.changed")

    q = BuildQuery(period, startdate, enddate, " ch.changed_on ", fields, tables, filters, evolutionary)
    data = ExecuteQuery(q)
    return (data)

def AggIssuesClosed (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    # Returns aggregated number of closed issues
    return(GetClosed(period, startdate, enddate, identities_db, type_analysis, False, closed_condition))

def EvolIssuesClosed (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    #return(GetEvolBacklogTickets(period, startdate, enddate, status, name.logtable, filter))
    return(GetClosed(period, startdate, enddate, identities_db, type_analysis, True, closed_condition))

def GetClosers (period, startdate, enddate, identities_db, type_analysis, evolutionary, closed_condition):
    #This function returns the evolution or agg number of closed issues
    #This function can be also reproduced using the Backlog function.
    #However this function is less time expensive.
    fields = " count(distinct(pup.upeople_id)) as closers "
    tables = " issues i, changes ch " + GetITSSQLReportFrom(identities_db, type_analysis)

    #closed condition filters
    filters = " i.id = ch.issue_id and " + closed_condition
    filters_ext = GetITSSQLReportWhere(type_analysis)
    if (filters_ext != ""):
        filters += " and " + filters_ext
    #unique identities filters
    if (type_analysis is None or len(type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ", people_upeople pup"
        filters += " and i.submitted_by = pup.people_id"
    elif (type_analysis[0] == "repository"):
        #Adding people_upeople table
        tables += ", people_upeople pup"
        filters += " and i.submitted_by = pup.people_id "

    #Action needed to replace issues filters by changes one
    filters = filters.replace("i.submitted", "ch.changed")

    q = BuildQuery(period, startdate, enddate, " ch.changed_on ", fields, tables, filters, evolutionary)
    data = ExecuteQuery(q)
    return (data)

def AggIssuesClosers (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    # Returns aggregated number of closed issues
    return(GetClosers(period, startdate, enddate, identities_db, type_analysis, False, closed_condition))

def EvolIssuesClosers (period, startdate, enddate, identities_db, type_analysis, closed_condition):
    #return(GetEvolBacklogTickets(period, startdate, enddate, status, name.logtable, filter))
    return(GetClosers(period, startdate, enddate, identities_db, type_analysis, True, closed_condition))

def GetChanged (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    #This function returns the evolution or agg number of changed issues
    #This function can be also reproduced using the Backlog function.
    #However this function is less time expensive.
    fields = " count(distinct(ch.issue_id)) as changed "
    tables = " issues i, changes ch " + GetITSSQLReportFrom(identities_db, type_analysis)

    filters = " i.id = ch.issue_id "
    filters_ext = GetITSSQLReportWhere(type_analysis)
    if (filters_ext != ""):
        filters += " and " + filters_ext

    #Action needed to replace issues filters by changes one
    filters = filters.replace("i.submitted", "ch.changed")

    q = BuildQuery(period, startdate, enddate, " ch.changed_on ", fields, tables, filters, evolutionary)

    data = ExecuteQuery(q)
    return (data)


def AggIssuesChanged (period, startdate, enddate, identities_db, type_analysis):
    # Returns aggregated number of closed issues
    return(GetChanged(period, startdate, enddate, identities_db, type_analysis, False))

def EvolIssuesChanged (period, startdate, enddate, identities_db, type_analysis):
    return(GetChanged(period, startdate, enddate, identities_db, type_analysis, True))


def GetChangers (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    #This function returns the evolution or agg number of changed issues
    #This function can be also reproduced using the Backlog function.
    #However this function is less time expensive.
    fields = " count(distinct(pup.upeople_id)) as changers "
    tables = " issues i, changes ch " + GetITSSQLReportFrom(identities_db, type_analysis)

    filters = " i.id = ch.issue_id "
    filters_ext = GetITSSQLReportWhere(type_analysis)
    if (filters_ext != ""):
        filters += " and " + filters_ext

    #unique identities filters
    if (type_analysis is None or len(type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ", people_upeople pup"
        filters += " and i.submitted_by = pup.people_id"

    elif (type_analysis[0] == "repository"):
        #Adding people_upeople table
        tables += ", people_upeople pup"
        filters += " and i.submitted_by = pup.people_id "

    #Action needed to replace issues filters by changes one
    filters = filters.replace("i.submitted", "ch.changed")

    q = BuildQuery(period, startdate, enddate, " ch.changed_on ", fields, tables, filters, evolutionary)

    data = ExecuteQuery(q)
    return (data)

def AggIssuesChangers (period, startdate, enddate, identities_db, type_analysis):
    # Returns aggregated number of closed issues
    return(GetChangers(period, startdate, enddate, identities_db, type_analysis, False))

def EvolIssuesChangers (period, startdate, enddate, identities_db, type_analysis):
    return(GetChangers(period, startdate, enddate, identities_db, type_analysis, True))

# Repositories
def GetIssuesRepositories (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # Generic function that counts repositories

    fields = " COUNT(DISTINCT(tracker_id)) AS trackers  "
    tables = " issues i " + GetITSSQLReportFrom(identities_db, type_analysis)
    filters = GetITSSQLReportWhere(type_analysis)

    q = BuildQuery(period, startdate, enddate, " i.submitted_on ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolIssuesRepositories (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of trackers
    return(GetIssuesRepositories(period, startdate, enddate, identities_db, type_analysis, True))

def AggIssuesRepositories (period, startdate, enddate, identities_db, type_analysis = []):
    # Evolution of trackers
    return(GetIssuesRepositories(period, startdate, enddate, identities_db, type_analysis, False))

def GetIssuesStudies (period, startdate, enddate, identities_db, type_analysis, evolutionary, study):
    # Generic function that counts evolution/agg number of specific studies with similar
    # database schema such as domains, companies and countries
    fields = ' count(distinct(name)) as ' + study
    tables = " issues i " + GetITSSQLReportFrom(identities_db, type_analysis)
    filters = GetITSSQLReportWhere(type_analysis)

    #Filtering last part of the query, not used in this case
    #filters = gsub("and\n( )+(d|c|cou|com).name =.*$", "", filters)

    q = BuildQuery(period, startdate, enddate, " i.submitted_on ", fields, tables, filters, evolutionary)
    q = re.sub(r'and (d|c|cou|com).name.*=', "", q)
    data = ExecuteQuery(q)
    return(data)

def EvolIssuesDomains (period, startdate, enddate, identities_db):
    # Evol number of domains used
    return(GetIssuesStudies(period, startdate, enddate, identities_db, ['domain', ''], True, 'domains'))

def EvolIssuesCountries (period, startdate, enddate, identities_db):
    # Evol number of countries
    return(GetIssuesStudies(period, startdate, enddate, identities_db, ['country', ''], True, 'countries'))

def EvolIssuesCompanies (period, startdate, enddate, identities_db):
    # Evol number of companies
    data = GetIssuesStudies(period, startdate, enddate, identities_db, ['company', ''], True, 'companies')
    return(data)

def AggIssuesDomains (period, startdate, enddate, identities_db):
    # Agg number of domains
    return(GetIssuesStudies(period, startdate, enddate, identities_db, ['domain', ''], False, 'domains'))

def AggIssuesCountries (period, startdate, enddate, identities_db):
    # Agg number of countries
    return(GetIssuesStudies(period, startdate, enddate, identities_db, ['country', ''], False, 'countries'))

def AggIssuesCompanies (period, startdate, enddate, identities_db):
    # Agg number of companies
    return(GetIssuesStudies(period, startdate, enddate, identities_db, ['company', ''], False, 'companies'))

def GetDate (startdate, enddate, identities_db, type_analysis, type):
    # date of submmitted issues (type= max or min)
    if (type=="max"):
        fields = " DATE_FORMAT (max(submitted_on), '%Y-%m-%d') as last_date"
    else :
        fields = " DATE_FORMAT (min(submitted_on), '%Y-%m-%d') as first_date"

    tables = " issues i " + GetITSSQLReportFrom(identities_db, type_analysis)
    filters = GetITSSQLReportWhere(type_analysis)

    q = BuildQuery(None, startdate, enddate, " i.submitted_on ", fields, tables, filters, False)
    data = ExecuteQuery(q)
    return(data)

def GetInitDate (startdate, enddate, identities_db, type_analysis):
    #Initial date of submitted issues
    return(GetDate(startdate, enddate, identities_db, type_analysis, "min"))

def GetEndDate (startdate, enddate, identities_db, type_analysis):
    #End date of submitted issues
    return(GetDate(startdate, enddate, identities_db, type_analysis, "max"))

###############
# Others
###############

def AggAllParticipants (startdate, enddate):
    # All participants from the whole history
    q = "SELECT count(distinct(pup.upeople_id)) as allhistory_participants from people_upeople pup"

    return(ExecuteQuery(q))


def TrackerURL ():
    # URL of the analyzed tracker
    q = "SELECT url, name as type FROM trackers t JOIN "+\
        "supported_trackers s ON t.type = s.id limit 1"

    return(ExecuteQuery(q))

###############
# Lists of repositories, companies, countries and other analysis
###############

def GetReposNameITS (startdate, enddate) :
    # List the url of each of the repositories analyzed
    # Those are order by the number of opened issues (dec order)
    q = " SELECT t.url as name "+\
               "   FROM issues i, "+\
               "        trackers t "+\
               "   WHERE i.tracker_id=t.id and "+\
               "         i.submitted_on >= "+ startdate+ " and "+\
               "         i.submitted_on < "+ enddate+\
               "   GROUP BY t.url  "+\
               "   ORDER BY count(distinct(i.id)) DESC "

    data = ExecuteQuery(q)
    return (data)

def GetTablesDomainsITS (i_db, table='') :
    tables = GetTablesOwnUniqueIdsITS(table)
    tables += ','+i_db+'.upeople_domains upd'
    return(tables)

def GetFiltersDomainsITS (table='') :
    filters = GetFiltersOwnUniqueIdsITS(table)
    filters += " AND pup.upeople_id = upd.upeople_id"
    return(filters)

def GetDomainsNameITS (startdate, enddate, identities_db, closed_condition, filter) :
    affiliations = ""
    for aff in filter:
        affiliations += " dom.name<>'"+aff+"' and "

    tables = GetTablesDomainsITS(identities_db)
    tables += ","+identities_db+".domains dom"

    q = "SELECT dom.name "+\
        "FROM "+ tables + " "+\
        "WHERE " + GetFiltersDomainsITS() +" AND "+\
        "       dom.id = upd.domain_id and "+\
        "       "+ affiliations +" "+\
        "       c.changed_on >= "+ startdate+ " AND "+\
        "       c.changed_on < "+ enddate+ " AND "+\
        "       "+ closed_condition+" "+\
        "GROUP BY dom.name "+\
        "ORDER BY COUNT(DISTINCT(c.issue_id)) DESC"
    data = ExecuteQuery(q)
    return (data)

def GetCountriesNamesITS (startdate, enddate, identities_db, closed_condition) :
    # List each of the countries analyzed
    # Those are order by number of closed issues
    q = "select cou.name "+\
        "from issues i, "+\
        "     changes ch, "+\
        "     people_upeople pup, "+\
        "     "+ identities_db+ ".upeople_countries upc, "+\
        "     "+ identities_db+ ".countries cou "+\
        "where i.id = ch.issue_id and "+\
        "      ch.changed_by = pup.people_id and "+\
        "      pup.upeople_id = upc.upeople_id and "+\
        "      upc.country_id = cou.id and "+\
        "      ch.changed_on >= "+ startdate+ " and "+\
        "      ch.changed_on < "+ enddate+" and "+\
        "      "+ closed_condition+ " "+\
        "      group by cou.name  "+\
        "      order by count(distinct(i.id)) desc"

    data = ExecuteQuery(q)
    return (data)

def GetCompaniesNameITS (startdate, enddate, identities_db, closed_condition, filter) :
    affiliations = ""
    for aff in filter:
        affiliations += " c.name<>'"+aff+"' and "

    # list each of the companies analyzed
    # those are order by number of closed issues
    q = "select c.name "+\
        "from issues i, "+\
        "     changes ch, "+\
        "     people_upeople pup, "+\
        "     "+ identities_db+ ".upeople_companies upc, "+\
        "     "+ identities_db+ ".companies c "+\
        "where i.id = ch.issue_id and "+\
        "      ch.changed_by = pup.people_id and "+\
        "      pup.upeople_id = upc.upeople_id and "+\
        "      upc.company_id = c.id and "+\
        "      ch.changed_on >= "+ startdate+ " and "+\
        "      ch.changed_on < "+ enddate+" and "+\
        "      "+ affiliations  +\
               closed_condition +\
        "      group by c.name  "+\
        "      order by count(distinct(i.id)) desc"

    data = ExecuteQuery(q)
    return (data)

################
# Last activity functions
################


##
## GetDiffClosedDays
##
## Get differences in number of closed tickets between two periods.
##  - date: final date of the two periods.
##  - days: number of days for each period.
##  - closed_condition: SQL string to define the condition of "closed"
##     for a ticket
## Example of parameters, for analizing the difference during the last
##  two weeks for the day 2013-11-25:
##  (date="2013-11-25", days=7, closed_condition=...)
##
def GetDiffClosedDays (period, identities_db, date, days, type_analysis, closed_condition):
    chardates = GetDates(date, days)
    last = AggIssuesClosed(period, chardates[1], chardates[0], identities_db, type_analysis, closed_condition)
    last = int(last['closed'])
    prev = AggIssuesClosed(period, chardates[2], chardates[1], identities_db, type_analysis, closed_condition)
    prev = int(prev['closed'])

    data = {}
    data['diff_netclosed_'+str(days)] = last - prev
    data['percentage_closed_'+str(days)] = GetPercentageDiff(prev, last)
    # data['closed_'+str(days)] = last
    return (data)

##
## GetDiffClosersDays
##
## Get differences in number of ticket closers between two periods.
##  - date: final date of the two periods.
##  - days: number of days for each period.
##  - closed_condition: SQL string to define the condition of "closed"
##     for a ticket
## Example of parameters, for analizing the difference during the last
##  two weeks for the day 2013-11-25:
##  (date="2013-11-25", days=7, closed_condition=...)
##
def GetDiffClosersDays (period, identities_db, date, days, type_analysis, closed_condition):
    # This function provides the percentage in activity between two periods
    chardates = GetDates(date, days)
    last = AggIssuesClosers(period, chardates[1], chardates[0], identities_db, type_analysis, closed_condition)
    last = int(last['closers'])
    prev = AggIssuesClosers(period, chardates[2], chardates[1], identities_db, type_analysis, closed_condition)
    prev = int(prev['closers'])

    data = {}
    data['diff_netclosers_'+str(days)] = last - prev
    data['percentage_closers_'+str(days)] = GetPercentageDiff(prev, last)
    # data['closers_'+str(days)] = last
    return (data)

def GetDiffOpenedDays (period, identities_db, date, days, type_analysis):
    # This function provides the percentage in activity between two periods
    chardates = GetDates(date, days)
    last = AggIssuesOpened(period, chardates[1], chardates[0], identities_db, type_analysis)
    last = int(last['opened'])
    prev = AggIssuesOpened(period, chardates[2], chardates[1], identities_db, type_analysis)
    prev = int(prev['opened'])

    data = {}
    data['diff_netopened_'+str(days)] = last - prev
    data['percentage_opened_'+str(days)] = GetPercentageDiff(prev, last)
    #data['opened_'+str(days)] = last
    return (data)

def GetDiffChangersDays (period, identities_db, date, days, type_analysis):
    # This function provides the percentage in activity between two periods
    chardates = GetDates(date, days)
    last = AggIssuesChangers(period, chardates[1], chardates[0], identities_db, type_analysis)
    last = int(last['changers'])
    prev = AggIssuesChangers(period, chardates[2], chardates[1], identities_db, type_analysis)
    prev = int(prev['changers'])

    data = {}
    data['diff_netchangers_'+str(days)] = last - prev
    data['percentage_changers_'+str(days)] = GetPercentageDiff(prev, last)
    # data['changers_'+str(days)] = last
    return (data)

def GetLastActivityITS (days, closed_condition):
    # opened issues
    days = str(days)
    q = "select count(*) as opened_"+days+" "+\
        "from issues "+\
        "where submitted_on >= ( "+\
        "      select (max(submitted_on) - INTERVAL "+days+" day) "+\
        "      from issues)"

    data1 = ExecuteQuery(q)

    # closed issues
    q = "select count(distinct(issue_id)) as closed_"+days+" "+\
        "from changes "+\
        "where  "+closed_condition+" "+\
        "and changed_on >= ( "+\
        "      select (max(changed_on) - INTERVAL "+days+" day) "+\
        "      from changes)"

    data2 = ExecuteQuery(q)

    # closers
    q = "SELECT count(distinct(pup.upeople_id)) as closers_"+days+" "+\
         "FROM changes, people_upeople pup "+\
         "WHERE pup.people_id = changes.changed_by and "+\
         "changed_on >= ( "+\
         "    select (max(changed_on) - INTERVAL "+days+" day) "+\
         "     from changes) AND "+ closed_condition


    data3 = ExecuteQuery(q)

    # people_involved    
    q = "SELECT count(distinct(pup.upeople_id)) as changers_"+days+" "+\
         "FROM changes, people_upeople pup "+\
         "WHERE pup.people_id = changes.changed_by and "+\
         "changed_on >= ( "+\
         "    select (max(changed_on) - INTERVAL "+days+" day) "+\
         "     from changes)"

    data4 = ExecuteQuery(q)

    agg_data = dict(data1.items()+data2.items())
    agg_data = dict(agg_data.items()+data3.items())

    return (agg_data)


def GetActiveChangersITS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT distinct(pup.upeople_id) as active_changers"+\
        " FROM changes, people_upeople pup "+\
        " WHERE pup.people_id = changes.changed_by and "+\
        " changed_on >= ( %s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActiveCommentersITS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT DISTINCT(pup.upeople_id) AS active_commenters"+\
        " FROM comments c, people_upeople pup"+\
        " WHERE pup.people_id = c.submitted_by AND"+\
        " submitted_on >= (%s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActiveSubmittersITS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT DISTINCT(pup.upeople_id) AS active_submitters"+\
      " FROM issues i, people_upeople pup"+\
      " WHERE pup.people_id = i.submitted_by AND"+\
      " submitted_on >= ( %s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActivePeopleITS(days, enddate):
    #Gets the IDs of the active people during the last days (until enddate)
    # for comments, issue creation and changes
    submitters = GetActiveSubmittersITS(days, enddate)
    changers = GetActiveChangersITS(days, enddate)
    commenters = GetActiveCommentersITS(days, enddate)
    people_its = submitters['active_submitters'] + changers['active_changers'] +\
        commenters['active_commenters']
    people_its = list(set(people_its))
    return(people_its)


################
# Top functions
################

def GetTopClosersByAssignee (days, startdate, enddate, identities_db, filter) :

    affiliations = ""
    for aff in filter:
        affiliations += " com.name<>'"+ aff +"' and "

    date_limit = ""
    if (days != 0 ) :
        sql = "SELECT @maxdate:=max(changed_on) from changes limit 1"
        ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate, changed_on)<"+str(days)

    q = "SELECT up.id as id, "+\
        "       up.identifier as closers, "+\
        "       count(distinct(ill.issue_id)) as closed "+\
        "FROM people_upeople pup,  "+\
        "     "+ identities_db+ ".upeople_companies upc, "+\
        "     "+ identities_db+ ".upeople up,  "+\
        "     "+ identities_db+ ".companies com, "+\
        "     issues_log_launchpad ill  "+\
        "WHERE ill.assigned_to = pup.people_id and "+\
        "      pup.upeople_id = up.id and  "+\
        "      up.id = upc.upeople_id and  "+\
        "      upc.company_id = com.id and "+\
        "      "+ affiliations+ " "+\
        "      ill.date >= upc.init and "+\
        "      ill.date < upc.end and  "+\
        "      ill.change_id  in (  "+\
        "         select id "+\
        "         from changes  "+\
        "         where new_value='Fix Committed' and "+\
        "               changed_on>="+ startdate+ " and  "+\
        "               changed_on<"+ enddate+ " "+ date_limit+") "+\
        "GROUP BY up.identifier "+\
        "ORDER BY closed desc, closers limit 10"


    data = ExecuteQuery(q)
    return (data)


def GetTablesOwnUniqueIdsITS (table='') :
    tables = 'changes c, people_upeople pup'
    if (table == "issues"): tables = 'issues i, people_upeople pup'
    return (tables)

def GetTablesCompaniesITS (i_db, table='') :
    tables = GetTablesOwnUniqueIdsITS(table)
    tables += ','+i_db+'.upeople_companies upc'
    return (tables)

def GetFiltersOwnUniqueIdsITS (table='') :
    filters = 'pup.people_id = c.changed_by'
    if (table == "issues"): filters = 'pup.people_id = i.submitted_by'
    return (filters)


def GetFiltersCompaniesITS (table='') :
    filters = GetFiltersOwnUniqueIdsITS(table)
    filters += " AND pup.upeople_id = upc.upeople_id"
    if (table == 'issues') :
        filters += " AND submitted_on >= upc.init AND submitted_on < upc.end"
    else :
         filters += " AND changed_on >= upc.init AND changed_on < upc.end"
    return (filters)

def GetCompanyTopClosers (company_name, startdate, enddate,
        identities_db, filter, closed_condition, limit) :
    affiliations = ""
    for aff in filter:
        affiliations += " AND up.identifier<>'"+aff+"' "

    q = "SELECT up.id as id, up.identifier as closers, "+\
        "       COUNT(DISTINCT(c.id)) as closed "+\
        "FROM "+GetTablesCompaniesITS(identities_db)+", "+\
        "     "+identities_db+".companies com, "+\
        "     "+identities_db+".upeople up "+\
        "WHERE "+GetFiltersCompaniesITS()+" AND " + closed_condition + " "+\
        "      AND pup.upeople_id = up.id "+\
        "      AND upc.company_id = com.id "+\
        "      AND com.name = "+ company_name +" "+\
        "      AND changed_on >= "+startdate+" AND changed_on < "+enddate+\
            affiliations +\
        " GROUP BY up.identifier ORDER BY closed DESC, closers LIMIT " + limit

    data = ExecuteQuery(q)
    return (data)

def GetTopClosers (days, startdate, enddate,
        identities_db, filter, closed_condition, limit) :

    affiliations = ""
    for aff in filter:
        affiliations += " com.name<>'"+ aff +"' and "

    date_limit = ""
    if (days != 0) :
        sql = "SELECT @maxdate:=max(changed_on) from changes limit 1"
        ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate, changed_on)<"+str(days)

    q = "SELECT up.id as id, up.identifier as closers, "+\
        "       count(distinct(c.id)) as closed "+\
        "FROM "+GetTablesCompaniesITS(identities_db)+ ", "+\
        "     "+identities_db+".companies com, "+\
        "     "+identities_db+".upeople up "+\
        "WHERE "+GetFiltersCompaniesITS() +" and "+\
        "      "+affiliations+ " "+\
        "      upc.company_id = com.id and "+\
        "      c.changed_by = pup.people_id and "+\
        "      pup.upeople_id = up.id and "+\
        "      c.changed_on >= "+ startdate+ " and "+\
        "      c.changed_on < "+ enddate+ " and " +\
        "      "+closed_condition+ " " + date_limit+ " "+\
        "GROUP BY up.identifier "+\
        "ORDER BY closed desc, closers "+\
        "LIMIT "+ limit

    data = ExecuteQuery(q)
    return (data)


def GetDomainTopClosers (domain_name, startdate, enddate,
        identities_db, filter, closed_condition, limit) :
    affiliations = ""
    for aff in filter:
        affiliations += " AND up.identifier<>'"+aff+"' "

    q = "SELECT up.id as id, up.identifier as closers, "+\
        "COUNT(DISTINCT(c.id)) as closed "+\
        "FROM "+GetTablesDomainsITS(identities_db)+", "+\
        "     "+identities_db+".domains dom, "+\
        "     "+identities_db+".upeople up "+\
        "WHERE "+ GetFiltersDomainsITS()+" AND "+closed_condition+" "+\
        "      AND pup.upeople_id = up.id "+\
        "      AND upd.domain_id = dom.id "+\
        "      AND dom.name = "+domain_name+" "+\
        "      AND changed_on >= "+startdate+" AND changed_on < " +enddate +\
              affiliations+ " "+\
        "GROUP BY up.identifier ORDER BY closed DESC, closers LIMIT " + limit

    data = ExecuteQuery(q)
    return (data)


def GetTopOpeners (days, startdate, enddate,
        identities_db, filter, closed_condition, limit) :
    affiliations = ""
    for aff in filter:
        affiliations += " com.name<>'"+ aff +"' and "
    date_limit = ""
    if (days != 0 ) :
        sql = "SELECT @maxdate:=max(submitted_on) from issues limit 1"
        ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate, submitted_on)<"+str(days)

    q = "SELECT up.id as id, up.identifier as openers, "+\
        "    count(distinct(i.id)) as opened "+\
        "FROM "+GetTablesCompaniesITS(identities_db,'issues')+", " +\
        "    "+identities_db+".companies com, "+\
        "    "+identities_db+".upeople up "+\
        "WHERE "+GetFiltersCompaniesITS('issues') +" and "+\
        "    "+ affiliations+ " "+\
        "    upc.company_id = com.id and "+\
        "    pup.upeople_id = up.id and "+\
        "    i.submitted_on >= "+ startdate+ " and "+\
        "    i.submitted_on < "+ enddate+\
            date_limit+ " "+\
        "    GROUP BY up.identifier "+\
        "    ORDER BY opened desc, openers "+\
        "    LIMIT " + limit
    data = ExecuteQuery(q)
    return (data)

#################
# People information, to be refactored
#################

def GetPeopleListITS (startdate, enddate) :
    fields = "DISTINCT(pup.upeople_id) as pid, count(c.id) as total"
    tables = GetTablesOwnUniqueIdsITS()
    filters = GetFiltersOwnUniqueIdsITS()
    filters += " GROUP BY pid ORDER BY total desc"
    q = GetSQLGlobal('changed_on',fields,tables, filters, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)


def GetPeopleQueryITS (developer_id, period, startdate, enddate, evol,  closed_condition) :
    fields = " COUNT(distinct(c.issue_id)) AS closed"
    tables = GetTablesOwnUniqueIdsITS()
    filters = GetFiltersOwnUniqueIdsITS() + " AND pup.upeople_id = "+ str(developer_id)
    filters += " AND "+ closed_condition

    if (evol) :
        q = GetSQLPeriod(period,'changed_on', fields, tables, filters,
                            startdate, enddate)
    else :
        fields += ",DATE_FORMAT (min(changed_on),'%Y-%m-%d') as first_date, "+\
                  "DATE_FORMAT (max(changed_on),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('changed_on', fields, tables, filters,
                            startdate, enddate)

    return (q)

def GetPeopleEvolITS (developer_id, period, startdate, enddate, closed_condition) :
    ## FIXME is this function used only to calculate closed issues? if not it must be
    ## fixed
    q = GetPeopleQueryITS(developer_id, period, startdate, enddate, True, closed_condition)

    data = ExecuteQuery(q)
    return (data)

def GetPeopleStaticITS (developer_id, startdate, enddate, closed_condition) :
    ## FIXME is this function used only to calculate closed issues? if not it must be
    ## fixed
    q = GetPeopleQueryITS(developer_id, None, startdate, enddate, False, closed_condition)

    data = ExecuteQuery(q)
    return (data)

#################
# Micro studies
#################

def EvolBMIIndex(period, startdate, enddate, identities_db, type_analysis, closed_condition):
    # Metric based on chapter 4.3.1 from
    # "Metrics and Models in Software Quality Engineering"
    # by Stephen H. Kan
    closed = EvolIssuesClosed(period, startdate, enddate, identities_db, type_analysis, closed_condition)
    opened = EvolIssuesOpened(period, startdate, enddate, identities_db, type_analysis)

    evol_bmi = [closed['closed'][i] / float(opened['opened'][i]) * 100\
                for i in range(len(closed['closed']))]

    return {'closed' : closed['closed'],
            'opened' : opened['opened'],
            'bmi' : evol_bmi}

def GetClosedSummaryCompanies (period, startdate, enddate, identities_db, closed_condition, num_companies):
    count = 1
    first_companies = {}

    companies = GetCompaniesNameITS(startdate, enddate, identities_db, closed_condition, ["-Bot", "-Individual", "-Unknown"])
    companies = companies['name']

    for company in companies:
        type_analysis = ["company", "'"+company+"'"]
        closed = EvolIssuesClosed(period, startdate, enddate, identities_db, type_analysis, closed_condition)
        closed = completePeriodIds(closed)
        # Rename field closed to company name
        closed[company] = closed["closed"]
        del closed['closed']

        if (count <= num_companies):
            #Case of companies with entity in the dataset
            first_companies = dict(first_companies.items() + closed.items())
        else :
            #Case of companies that are aggregated in the field Others
            if 'Others' not in first_companies:
                first_companies['Others'] = closed[company]
            else:
                first_companies['Others'] = [a+b for a, b in zip(first_companies['Others'],closed[company])]
        count = count + 1
    first_companies = completePeriodIds(first_companies)

    return(first_companies)


class Backend(object):

    closed_condition = ""
    reopened_condition = ""
    name_log_table = ""
    statuses = ""
    open_status = ""
    reopened_status = ""
    name_log_table = ""

    def __init__(self, its_type):
        if (its_type == 'allura'):
            Backend.closed_condition = "new_value='CLOSED'"
        if (its_type == 'bugzilla' or its_type == 'bg'):
            Backend.closed_condition = "(new_value='RESOLVED' OR new_value='CLOSED')"
            Backend.reopened_condition = "new_value='NEW'"
            Backend.name_log_table = 'issues_log_bugzilla'
            Backend.statuses = ["NEW", "ASSIGNED"]
            #Pretty specific states in Red Hat's Bugzilla
            Backend.statuses = ["ASSIGNED", "CLOSED", "MODIFIED", "NEW", "ON_DEV", \
                    "ON_QA", "POST", "RELEASE_PENDING", "VERIFIED"]

        if (its_type == 'github'):
            Backend.closed_condition = "field='closed'"

        if (its_type == 'jira'):
            Backend.closed_condition = "new_value='CLOSED'"
            Backend.reopened_condition = "new_value='Reopened'"
            #Backend.new_condition = "status='Open'"
            #Backend.reopened_condition = "status='Reopened'"
            Backend.open_status = 'Open'
            Backend.reopened_status = 'Reopened'
            Backend.name_log_table = 'issues_log_jira'

        if (its_type == 'lp'):
            #Backend.closed_condition = "(new_value='Fix Released' or new_value='Invalid' or new_value='Expired' or new_value='Won''t Fix')"
            Backend.closed_condition = "(new_value='Fix Committed')"
            Backend.statuses = ["Confirmed", "Fix Committed", "New", "In Progress", "Triaged", "Incomplete", "Invalid", "Won\\'t Fix", "Fix Released", "Opinion", "Unknown", "Expired"]
            Backend.name_log_table = 'issues_log_launchpad'

        if (its_type == 'redmine'):
            Backend.statuses = ["New", "Verified", "Need More Info", "In Progress", "Feedback",
                         "Need Review", "Testing", "Pending Backport", "Pending Upstream",
                         "Resolved", "Closed", "Rejected", "Won\\'t Fix", "Can\\'t reproduce",
                         "Duplicate"]
            Backend.closed_condition = "(new_value='Resolved' OR new_value='Closed' OR new_value='Rejected'" +\
                                  " OR new_value='Won\\'t Fix' OR new_value='Can\\'t reproduce' OR new_value='Duplicate')"
            Backend.reopened_condition = "new_value='Reopened'" # FIXME: fake condition
            Backend.name_log_table = 'issues_log_redmine'
