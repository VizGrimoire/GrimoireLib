## Copyright (C) 2013-2014 Bitergia
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
##
## Queries for source code review data analysis
##
##
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo San Felix <acs@bitergia.com>

import logging, os, sys
from numpy import median, average

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from GrimoireSQL import ExecuteQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds
from GrimoireUtils import checkListArray, removeDecimals, get_subprojects
from GrimoireUtils import getPeriod, createJSON, checkFloatArray

from data_source import DataSource
from filter import Filter


class SCR(DataSource):

    @staticmethod
    def get_db_name():
        return "db_gerrit"

    @staticmethod
    def get_name(): return "scr"

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        evol = {}

        if (filter_ is not None):
            type_analysis = [filter_.get_name(), filter_.get_item()]

            data = EvolReviewsSubmitted(period, startdate, enddate, type_analysis, identities_db)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsMerged(period, startdate, enddate, type_analysis, identities_db)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsAbandoned(period, startdate, enddate, type_analysis, identities_db)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsPending(period, startdate, enddate, type_analysis, identities_db)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            if (period == "month"):
                data = EvolTimeToReviewSCR(period, startdate, enddate, identities_db, type_analysis)
                data['review_time_days_avg'] = checkFloatArray(data['review_time_days_avg'])
                data['review_time_days_median'] = checkFloatArray(data['review_time_days_median'])
                evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            return evol

        else:
            data = EvolReviewsSubmitted(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsOpened(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsNew(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsNewChanges(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            # data = EvolReviewsInProgress(period, startdate, enddate)
            # evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsClosed(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsMerged(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsMergedChanges(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsAbandoned(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsAbandonedChanges(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolReviewsPending(period, startdate, enddate, [])
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            #Patches info
            data = EvolPatchesVerified(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            # data = EvolPatchesApproved(period, startdate, enddate)
            # evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolPatchesCodeReview(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolPatchesSent(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            #Waiting for actions info
            data = EvolWaiting4Reviewer(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolWaiting4Submitter(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            #Reviewers info
            data = EvolReviewers(period, startdate, enddate)
            evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            # Time to Review info
            if period == "month": # only month supported now
                data = EvolTimeToReviewSCR (period, startdate, enddate)
                for i in range(0,len(data['review_time_days_avg'])):
                    val = data['review_time_days_avg'][i] 
                    data['review_time_days_avg'][i] = float(val)
                    if (val == 0): data['review_time_days_avg'][i] = 0
                evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
            return evol

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  SCR.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = SCR().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        agg = {}

        if (filter_ is not None):
            type_analysis = [filter_.get_name(), filter_.get_item()]
            data = StaticReviewsSubmitted(period, startdate, enddate, type_analysis, identities_db)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsMerged(period, startdate, enddate, type_analysis, identities_db)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsAbandoned(period, startdate, enddate, type_analysis, identities_db)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsPending(period, startdate, enddate, type_analysis, identities_db)
            agg = dict(agg.items() + data.items())
            data = StaticTimeToReviewSCR(startdate, enddate, identities_db, type_analysis, identities_db)
            val = data['review_time_days_avg']
            if (not val or val == 0): data['review_time_days_avg'] = 0
            else: data['review_time_days_avg'] = float(val)
            val = data['review_time_days_median']
            if (not val or val == 0): data['review_time_days_median'] = 0
            else: data['review_time_days_median'] = float(val)
            agg = dict(agg.items() + data.items())

        else:
            agg = StaticReviewsSubmitted(period, startdate, enddate)
            data = StaticReviewsOpened(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsNew(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsInProgress(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsClosed(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsMerged(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsAbandoned(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticReviewsPending(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticPatchesVerified(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticPatchesApproved(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticPatchesCodeReview(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticPatchesSent(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticWaiting4Reviewer(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = StaticWaiting4Submitter(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            #Reviewers info
            data = StaticReviewers(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            # Time to Review info
            data = StaticTimeToReviewSCR(startdate, enddate)
            data['review_time_days_avg'] = float(data['review_time_days_avg'])
            data['review_time_days_median'] = float(data['review_time_days_median'])
            agg = dict(agg.items() + data.items())

            # Tendencies
            for i in [7,30,365]:
                period_data = GetSCRDiffSubmittedDays(period, enddate, i, identities_db)
                agg = dict(agg.items() + period_data.items())
                period_data = GetSCRDiffMergedDays(period, enddate, i, identities_db)
                agg = dict(agg.items() + period_data.items())
                period_data = GetSCRDiffPendingDays(period, enddate, i, identities_db)
                agg = dict(agg.items() + period_data.items())
                period_data = GetSCRDiffAbandonedDays(period, enddate, i, identities_db)
                agg = dict(agg.items() + period_data.items())

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data = SCR.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = SCR().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = SCR.get_bots()

        top_reviewers = {}
        top_reviewers['reviewers'] = GetTopReviewersSCR(0, startdate, enddate, identities_db, bots, npeople)
        top_reviewers['reviewers.last year']= GetTopReviewersSCR(365, startdate, enddate, identities_db, bots, npeople)
        top_reviewers['reviewers.last month']= GetTopReviewersSCR(31, startdate, enddate, identities_db, bots, npeople)

        # Top openers
        top_openers = {}
        top_openers['openers.']=GetTopOpenersSCR(0, startdate, enddate,identities_db, bots, npeople)
        top_openers['openers.last year']=GetTopOpenersSCR(365, startdate, enddate,identities_db, bots, npeople)
        top_openers['openers.last_month']=GetTopOpenersSCR(31, startdate, enddate,identities_db, bots, npeople)

        # Top mergers
        top_mergers = {}
        top_mergers['mergers.last year']=GetTopMergersSCR(365, startdate, enddate,identities_db, bots, npeople)
        top_mergers['mergers.']=GetTopMergersSCR(0, startdate, enddate,identities_db, bots, npeople)
        top_mergers['mergers.last_month']=GetTopMergersSCR(31, startdate, enddate,identities_db, bots, npeople)

        # The order of the list item change so we can not check it
        top_all = dict(top_reviewers.items() +  top_openers.items() + top_mergers.items())

        return (top_all)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = SCR.get_top_data (startdate, enddate, i_db, None, npeople)
        createJSON (data, destdir+"/"+SCR().get_top_filename())

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            items  = GetReposSCRName(startdate, enddate)
        elif (filter_name == "company"):
            items  = GetCompaniesSCRName(startdate, enddate, identities_db)
        elif (filter_name == "country"):
            items = GetCountriesSCRName(startdate, enddate, identities_db)
        elif (filter_name == "domain"):
            logging.error("SCR " + filter_name + " not supported")
        elif (filter_name == "project"):
            items = get_projects_scr_name(startdate, enddate, identities_db)
        else:
            logging.error("SCR " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items = SCR.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return
        items = items['name']

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        # For repos aggregated data. Include metrics to sort in javascript.
        if (filter_name == "repository"):
            items_list = {"name":[],"review_time_days_median":[],"submitted":[]}
        else:
            items_list = items

        for item in items :
            item_file = item.replace("/","_")
            if (filter_name == "repository"):
                items_list["name"].append(item_file)

            logging.info (item)
            filter_item = Filter(filter_name, item)

            evol = SCR.get_evolutionary_data(period, startdate, enddate, 
                                               identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(SCR()))
            createJSON(evol, fn)

            # Static
            agg = SCR.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(SCR()))
            createJSON(agg, fn)
            if (filter_name == "repository"):
                items_list["submitted"].append(agg["submitted"])
                items_list["review_time_days_median"].append(agg['review_time_days_median'])

        fn = os.path.join(destdir, filter_.get_filename(SCR()))
        createJSON(items_list, fn)

    # Unify top format
    @staticmethod
    def _safeTopIds(top_data_period):
        if not isinstance(top_data_period['id'], (list)):
            for name in top_data_period:
                top_data_period[name] = [top_data_period[name]]
        return top_data_period['id']

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):

        top_data = SCR.get_top_data (startdate, enddate, identities_db, None, npeople)

        top  = SCR._safeTopIds(top_data['reviewers'])
        top += SCR._safeTopIds(top_data['reviewers.last year'])
        top += SCR._safeTopIds(top_data['reviewers.last month'])
        top += SCR._safeTopIds(top_data['openers.'])
        top += SCR._safeTopIds(top_data['openers.last year'])
        top += SCR._safeTopIds(top_data['openers.last_month'])
        top += SCR._safeTopIds(top_data['mergers.'])
        top += SCR._safeTopIds(top_data['mergers.last year'])
        top += SCR._safeTopIds(top_data['mergers.last_month'])
        # remove duplicates
        people = list(set(top)) 

        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        evol = GetPeopleEvolSCR(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        return GetPeopleStaticSCR(upeople_id, startdate, enddate)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_metrics_definition ():
        mdef =  {
            "scr_merged" : {
                "divid" : "scr_merged",
                "column" : "merged",
                "name" : "Merged changes",
                "desc" : "Number of changes merged into the source code"
            },
            "scr_mergers" : {
                "divid" : "scr_mergers",
                "column" : "mergers",
                "name" : "Successful submitters",
                "action" : "merged",
                "desc" : "Number of persons submitting changes that got accepted"
            },
            "scr_opened" : {
                "divid" : "scr_opened",
                "column" : "opened",
                "name" : "Opened reviews",
                "desc" : "Number of review processes opened"
            },
            "scr_closed" : {
                "divid" : "scr_closed",
                "column" : "closed",
                "name" : "Closed reviews",
                "desc" : "Number of closed review processes (merged or abandoned)"
            },
            "scr_new" : {
                "divid" : "scr_new",
                "column" : "new",
                "name" : "New reviews",
                "desc" : "Number of new review processes"
            },
            "scr_abandoned" : {
                "divid" : "scr_abandoned",
                "column" : "abandoned",
                "name" : "Abandoned",
                "desc" : "Number of abandoned review processes"
            },
            "scr_verified" : {
                "divid" : "scr_verified",
                "column" : "verified",
                "name" : "Verified",
                "desc" : "Number of verified changes"
            },
            "scr_approved" : {
                "divid" : "scr_approved",
                "column" : "approved",
                "name" : "Approved",
                "desc" : "Number of code review processes in approved state"
            },
            "scr_codereview" : {
                "divid" : "scr_codereview",
                "column" : "codereview",
                "name" : "Code review",
                "desc" : "Number of code review processes in code review state"
            },
            "scr_WaitingForReviewer" : {
                "divid" : "scr_WaitingForReviewer",
                "column" : "WaitingForReviewer",
                "name" : "Waiting for reviewer",
                "desc" : "Number of code review processes waiting for reviewer"
            },        
            "scr_WaitingForSubmitter" : {
                "divid" : "scr_WaitingForSubmitter",
                "column" : "WaitingForSubmitter",
                "name" : "Waiting for submitter",
                "desc" : "Number of code review processes waiting for submitter"
            },
            "scr_submitted" : {
                "divid" : "scr_submitted",
                "column" : "submitted",
                "name" : "submitted",
                "desc" : "Number of submitted code review processes"
            },
            "scr_companies" : {
                "divid" : "scr_companies",
                "column" : "companies",
                "name" : "Organizations",
                "desc" : "Number of organizations (companies, etc.) with persons active in code review"
            },
            "scr_countries" : {
                "divid" : "scr_countries",
                "column" : "countries",
                "name" : "Countries",
                "desc" : "Number of countries with persons active in code review"
            },
            "scr_domains" : {
                "divid" : "scr_domains",
                "column" : "domains",
                "name" : "Domains",
                "desc" : "Number of domains with persons active in code review"
            },
            "scr_repositories" : {
                "divid" : "scr_repositories",
                "column" : "repositories",
                "name" : "Repositories",
                "desc" : "Number of respositories with active code review activities"
            },
            "scr_people" : {
                "divid" : "scr_people",
                "column" : "people",
                "name" : "People",
                "desc" : "Number of persons active in code review activities"
            },
            "scr_closers" : {
                "divid" : "scr_closers",
                "column" : "closers",
                "name" : "Closers",
                "desc" : "Number of persons closing code review processes",
                "action" : "closed"
            },
            "scr_openers" : {
                "divid" : "scr_openers",
                "column" : "openers",
                "name" : "Openers",
                "desc" : "Number of persons closing code review processes",
                "action" : "opened"
            }
        }
        return mdef

##########
# Specific FROM and WHERE clauses per type of report
##########
def GetSQLRepositoriesFromSCR ():
    #tables necessaries for repositories
    return (" , trackers t")

def GetSQLRepositoriesWhereSCR (repository):
    #fields necessaries to match info among tables
    return (" and t.url ='"+ repository+ "' and t.id = i.tracker_id")

def GetSQLProjectFromSCR ():
    # projects are mapped to repositories
    return (" , trackers t")

def GetSQLProjectWhereSCR (project, identities_db):
    # include all repositories for a project and its subprojects

    repos = """and t.url IN (
           SELECT repository_name
           FROM   %s.projects p, %s.project_repositories pr
           WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
               AND pr.data_source='scr'
    )""" % (identities_db, identities_db, get_subprojects(project, identities_db))

    return (repos   + " and t.id = i.tracker_id")

def GetSQLCompaniesFromSCR (identities_db):
    #tables necessaries for companies
    return (" , people_upeople pup,"+\
            identities_db+".upeople_companies upc,"+\
            identities_db+".companies c")


def GetSQLCompaniesWhereSCR (company):
    #fields necessaries to match info among tables
    return ("and i.submitted_by = pup.people_id "+\
              "and pup.upeople_id = upc.upeople_id "+\
              "and i.submitted_on >= upc.init "+\
              "and i.submitted_on < upc.end "+\
              "and upc.company_id = c.id "+\
              "and c.name ='"+ company+"'")


def GetSQLCountriesFromSCR (identities_db):
    #tables necessaries for companies
    return (" , people_upeople pup, "+\
              identities_db+".upeople_countries upc, "+\
              identities_db+".countries c ")


def GetSQLCountriesWhereSCR (country):
    #fields necessaries to match info among tables
    return ("and i.submitted_by = pup.people_id "+\
              "and pup.upeople_id = upc.upeople_id "+\
              "and upc.country_id = c.id "+\
              "and c.name ='"+country+"'")


##########
#Generic functions to obtain FROM and WHERE clauses per type of report
##########

def GetSQLReportFromSCR (identities_db, type_analysis):
    #generic function to generate 'from' clauses
    #"type" is a list of two values: type of analysis and value of
    #such analysis

    From = ""

    if (len(type_analysis) != 2): return From

    analysis = type_analysis[0]

    if (analysis):
        if analysis == 'repository': From = GetSQLRepositoriesFromSCR()
        elif analysis == 'company': From = GetSQLCompaniesFromSCR(identities_db)
        elif analysis == 'country': From = GetSQLCountriesFromSCR(identities_db)
        elif analysis == 'project': From = GetSQLProjectFromSCR()

    return (From)


def GetSQLReportWhereSCR (type_analysis, identities_db = None):
    #generic function to generate 'where' clauses

    #"type" is a list of two values: type of analysis and value of
    #such analysis

    where = ""
    if (len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if (analysis):
        if analysis == 'repository': where = GetSQLRepositoriesWhereSCR(value)
        elif analysis == 'company': where = GetSQLCompaniesWhereSCR(value)
        elif analysis == 'country': where = GetSQLCountriesWhereSCR(value)
        elif analysis == 'project':
            if (identities_db is None):
                logging.error("project filter not supported without identities_db")
                sys.exit(0)
            else:
                where = GetSQLProjectWhereSCR(value, identities_db)

    return (where)


#########
# General functions
#########

def GetReposSCRName  (startdate, enddate, limit = 0):
    limit_sql=""
    if (limit > 0): limit_sql = " LIMIT " + str(limit)

    q = "SELECT t.url as name, COUNT(DISTINCT(i.id)) AS issues "+\
           " FROM  issues i, trackers t "+\
           " WHERE i.tracker_id = t.id AND "+\
           "  i.submitted_on >="+  startdate+ " AND "+\
           "  i.submitted_on < "+ enddate +\
           " GROUP BY t.url "+\
           " ORDER BY issues DESC "+limit_sql
    names = ExecuteQuery(q)
    if not isinstance(names['name'], (list)): names['name'] = [names['name']]
    return(names)

def GetCompaniesSCRName  (startdate, enddate, identities_db, limit = 0):
    limit_sql=""
    if (limit > 0): limit_sql = " LIMIT " + str(limit)

    q = "SELECT c.id as id, c.name as name, COUNT(DISTINCT(i.id)) AS total "+\
               "FROM  "+identities_db+".companies c, "+\
                       identities_db+".upeople_companies upc, "+\
                "     people_upeople pup, "+\
                "     issues i "+\
               "WHERE i.submitted_by = pup.people_id AND "+\
               "  upc.upeople_id = pup.upeople_id AND "+\
               "  c.id = upc.company_id AND "+\
               "  i.status = 'merged' AND "+\
               "  i.submitted_on >="+  startdate+ " AND "+\
               "  i.submitted_on < "+ enddate+ " "+\
               "GROUP BY c.name "+\
               "ORDER BY total DESC " + limit_sql
    return(ExecuteQuery(q))

def GetCountriesSCRName  (startdate, enddate, identities_db, limit = 0):
    limit_sql=""
    if (limit > 0): limit_sql = " LIMIT " + str(limit)

    q = "SELECT c.name as name, COUNT(DISTINCT(i.id)) AS issues "+\
           "FROM  "+identities_db+".countries c, "+\
                   identities_db+".upeople_countries upc, "+\
            "    people_upeople pup, "+\
            "    issues i "+\
           "WHERE i.submitted_by = pup.people_id AND "+\
           "  upc.upeople_id = pup.upeople_id AND "+\
           "  c.id = upc.country_id AND "+\
           "  i.status = 'merged' AND "+\
           "  i.submitted_on >="+  startdate+ " AND "+\
           "  i.submitted_on < "+ enddate+ " "+\
           "GROUP BY c.name "+\
           "ORDER BY issues DESC "+limit_sql
    return(ExecuteQuery(q))

def get_projects_scr_name  (startdate, enddate, identities_db, limit = 0):
    # Projects activity needs to include subprojects also
    logging.info ("Getting projects list for SCR")

    # Get all projects list
    q = "SELECT p.id AS name FROM  %s.projects p" % (identities_db)
    projects = ExecuteQuery(q)
    data = []

    # Loop all projects getting reviews
    for project in projects['name']:
        type_analysis = ['project', project]
        period = None
        evol = False
        reviews = GetReviews (period, startdate, enddate, "submitted",
                              type_analysis, evol, identities_db)
        reviews = reviews['submitted']
        if (reviews > 0):
            data.append([reviews,project])

    # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
    from operator import itemgetter
    data_sort = sorted(data, key=itemgetter(0),reverse=True)
    names = [name[1] for name in data_sort]

    if (limit > 0): names = names[:limit]
    return({"name":names})

#########
#Functions about the status of the review
#########

# REVIEWS
def GetReviews (period, startdate, enddate, type_, type_analysis, evolutionary, identities_db):
    #Building the query
    fields = " count(distinct(i.issue)) as " + type_
    tables = "issues i" + GetSQLReportFromSCR(identities_db, type_analysis)
    if type_ == "submitted": filters = ""
    elif type_ == "opened": filters = " (i.status = 'NEW' or i.status = 'WORKINPROGRESS') "
    elif type_ == "new": filters = " i.status = 'NEW' "
    elif type_ == "inprogress": filters = " i.status = 'WORKINGPROGRESS' "
    elif type_ == "closed": filters = " (i.status = 'MERGED' or i.status = 'ABANDONED') "
    elif type_ == "merged": filters = " i.status = 'MERGED' "
    elif type_ == "abandoned": filters = " i.status = 'ABANDONED' "
    filters = filters + GetSQLReportWhereSCR(type_analysis, identities_db)

    #Adding dates filters (and evolutionary or static analysis)
    if (evolutionary):
        q = GetSQLPeriod(period, "i.submitted_on", fields, tables, filters,
                         startdate, enddate)
    else:
        q = GetSQLGlobal(" i.submitted_on ", fields, tables, filters, startdate, enddate)

    return(ExecuteQuery(q))


# Reviews status using changes table
def GetReviewsChanges(period, startdate, enddate, type_, type_analysis, evolutionary, identities_db):
    fields = "count(issue_id) as "+ type_+ "_changes"
    tables = "changes c, issues i"
    tables = tables + GetSQLReportFromSCR(identities_db, type_analysis)
    filters = "c.issue_id = i.id AND new_value='"+type_+"'"
    filters = filters + GetSQLReportWhereSCR(type_analysis, identities_db)

    #Adding dates filters (and evolutionary or static analysis)
    if (evolutionary):
        q = GetSQLPeriod(period, " changed_on", fields, tables, filters,
                            startdate, enddate)
    else:
        q = GetSQLGlobal(" changed_on ", fields, tables, filters, startdate, enddate)

    return(ExecuteQuery(q))


# EVOLUTIONoneRY META FUNCTIONS BASED ON REVIEWS

def EvolReviewsSubmitted (period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "submitted", type_analysis, True, identities_db))

def EvolReviewsOpened (period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "opened", type_analysis, True, identities_db))

def EvolReviewsNew(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "new", type_analysis, True, identities_db))

def GetEvolChanges(period, startdate, enddate, value):
    fields = "count(issue_id) as "+ value+ "_changes"
    tables = "changes"
    filters = "new_value='"+value+"'"
    q = GetSQLPeriod(period, " changed_on", fields, tables, filters,
            startdate, enddate)
    return(ExecuteQuery(q))

def EvolReviewsNewChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviewsChanges(period, startdate, enddate, "new", type_analysis, True, identities_db))

def EvolReviewsInProgress(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "inprogress", type_analysis, True, identities_db))

def EvolReviewsClosed(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "closed", type_analysis, True, identities_db))

def EvolReviewsMerged(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "merged", type_analysis, True, identities_db))

def EvolReviewsMergedChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviewsChanges(period, startdate, enddate, "merged", type_analysis, True, identities_db))

def EvolReviewsAbandoned(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "abandoned", type_analysis, True, identities_db))


def EvolReviewsAbandonedChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviewsChanges(period, startdate, enddate, "abandoned", type_analysis, True, identities_db))


def EvolReviewsPending(period, startdate, enddate, type_analysis = [], identities_db=None):
    data = EvolReviewsSubmitted(period, startdate, enddate, type_analysis, identities_db)
    data = completePeriodIds(data, period, startdate, enddate)
    data1 = EvolReviewsMerged(period, startdate, enddate, type_analysis, identities_db)
    data1 = completePeriodIds(data1, period, startdate, enddate)
    data2 = EvolReviewsAbandoned(period, startdate, enddate, type_analysis, identities_db)
    data2 = completePeriodIds(data2, period, startdate, enddate)
    evol = dict(data.items() + data1.items() + data2.items())
    pending = {"pending":[]}

    for i in range(0, len(evol['merged'])):
        pending_val = evol["submitted"][i] - evol["merged"][i] - evol["abandoned"][i]
        pending["pending"].append(pending_val)
    pending[period] = evol[period]
    pending = completePeriodIds(pending, period, startdate, enddate)
    return pending

# PENDING = SUBMITTED - MERGED - ABANDONED
def EvolReviewsPendingChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    data = EvolReviewsSubmitted(period, startdate, enddate, type_analysis, identities_db)
    data = completePeriodIds(data, period, startdate, enddate)
    data1 = EvolReviewsMergedChanges(period, startdate, enddate, type_analysis, identities_db)
    data1 = completePeriodIds(data1, period, startdate, enddate)
    data2 = EvolReviewsAbandonedChanges(period, startdate, enddate, type_analysis, identities_db)
    data2 = completePeriodIds(data2, period, startdate, enddate)
    evol = dict(data.items() + data1.items() + data2.items())
    pending = {"pending":[]}

    for i in range(0,len(evol['merged_changes'])):
        pending_val = evol["submitted"][i] - evol["merged_changes"][i] - evol["abandoned_changes"][i]
        pending["pending"].append(pending_val)

    pending[period] = evol[period]
    pending = completePeriodIds(pending, period, startdate, enddate)
    return pending

# STATIC META FUNCTIONS BASED ON REVIEWS

def StaticReviewsSubmitted (period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "submitted", type_analysis, False, identities_db))


def StaticReviewsOpened (period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "opened", type_analysis, False, identities_db))


def StaticReviewsNew(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "new", type_analysis, False, identities_db))


def StaticReviewsNewChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviewsChanges(period, startdate, enddate, "new", False))


def StaticReviewsInProgress(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "inprogress", type_analysis, False, identities_db))


def StaticReviewsClosed(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "closed", type_analysis, False, identities_db))


def StaticReviewsMerged(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "merged", type_analysis, False, identities_db))


def StaticReviewsMergedChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviewsChanges(period, startdate, enddate, "merged", False))


def StaticReviewsAbandoned(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviews(period, startdate, enddate, "abandoned", type_analysis, False, identities_db))


def StaticReviewsAbandonedChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    return (GetReviewsChanges(period, startdate, enddate, "abandoned", False))


# PENDING = SUBMITTED - MERGED - ABANDONED
def StaticReviewsPending(period, startdate, enddate, type_analysis = [], identities_db=None):
    submitted = StaticReviewsSubmitted(period, startdate, enddate, type_analysis, identities_db)
    merged = StaticReviewsMerged(period, startdate, enddate, type_analysis, identities_db)
    abandoned = StaticReviewsAbandoned(period, startdate, enddate, type_analysis, identities_db)
    pending = submitted['submitted']-merged['merged']-abandoned['abandoned']
    return ({"pending":pending})


def StaticReviewsPendingChanges(period, startdate, enddate, type_analysis = [], identities_db=None):
    submitted = StaticReviewsSubmitted(period, startdate, enddate, type_analysis, identities_db)
    merged = StaticReviewsMergedChanges(period, startdate, enddate, type_analysis, identities_db)
    abandoned = StaticReviewsAbandonedChanges(period, startdate, enddate, type_analysis, identities_db)
    pending = submitted['submitted']-merged['merged']-abandoned['abandoned']
    return ({"pending":pending})


#WORK ON PATCHES: ANY REVIEW MAY HAVE MORE THAN ONE PATCH
def GetEvaluations (period, startdate, enddate, type_, type_analysis, evolutionary, identities_db = None):
    # verified - VRIF
    # approved - APRV
    # code review - CRVW
    # submitted - SUBM

    #Building the query
    fields = " count(distinct(c.id)) as " + type_
    tables = " changes c, issues i " + GetSQLReportFromSCR(None, type_analysis)
    if type_ == "verified": filters =  " (c.field = 'VRIF' OR c.field = 'Verified') "
    elif type_ == "approved": filters =  " c.field = 'APRV'  "
    elif type_ == "codereview": filters =  "   (c.field = 'CRVW' OR c.field = 'Code-Review') "
    elif type_ == "sent": filters =  " c.field = 'SUBM'  "
    filters = filters + " and i.id = c.issue_id "
    filters = filters + GetSQLReportWhereSCR(type_analysis, identities_db)

    #Adding dates filters
    if (evolutionary):
        q = GetSQLPeriod(period, " c.changed_on", fields, tables, filters,
                          startdate, enddate)
    else:
        q = GetSQLGlobal(" c.changed_on", fields, tables, filters,
                      startdate, enddate)
    return(ExecuteQuery(q))

# EVOLUTIONoneRY METRICS
def EvolPatchesVerified (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "verified", type_analysis, True))


def EvolPatchesApproved (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "approved", type_analysis, True))


def EvolPatchesCodeReview (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "codereview", type_analysis, True))


def EvolPatchesSent (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "sent", type_analysis, True))


#STATIC METRICS
def StaticPatchesVerified  (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "verified", type_analysis, False))


def StaticPatchesApproved (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "approved", type_analysis, False))


def StaticPatchesCodeReview (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "codereview", type_analysis, False))


def StaticPatchesSent (period, startdate, enddate, type_analysis = []):
    return (GetEvaluations (period, startdate, enddate, "sent", type_analysis, False))


#PATCHES WAITING FOR REVIEW FROM REVIEWER
def GetWaiting4Reviewer (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    fields = " count(distinct(c.id)) as WaitingForReviewer "
    tables = " changes c, "+\
             "  issues i, "+\
             "        (select c.issue_id as issue_id, "+\
             "                c.old_value as old_value, "+\
             "                max(c.id) as id "+\
             "         from changes c, "+\
             "              issues i "+\
             "         where c.issue_id = i.id and "+\
             "               i.status='NEW' "+\
             "         group by c.issue_id, c.old_value) t1 "
    tables = tables + GetSQLReportFromSCR(identities_db, type_analysis)
    filters =  " i.id = c.issue_id  "+\
               "  and t1.id = c.id "+\
               "  and (c.field='CRVW' or c.field='Code-Review' or c.field='Verified' or c.field='VRIF') "+\
               "  and (c.new_value=1 or c.new_value=2) "
    filters = filters + GetSQLReportWhereSCR(type_analysis, identities_db)

    if (evolutionary):
        q = GetSQLPeriod(period, " c.changed_on", fields, tables, filters,
                          startdate, enddate)
    else:
        q = GetSQLGlobal(" c.changed_on ", fields, tables, filters,
                          startdate, enddate)

    return(ExecuteQuery(q))


def EvolWaiting4Reviewer (period, startdate, enddate, identities_db=None, type_analysis = []):
    return (GetWaiting4Reviewer(period, startdate, enddate, identities_db, type_analysis, True))


def StaticWaiting4Reviewer (period, startdate, enddate, identities_db=None, type_analysis = []):
    return (GetWaiting4Reviewer(period, startdate, enddate, identities_db, type_analysis, False))


def GetWaiting4Submitter (period, startdate, enddate, identities_db, type_analysis, evolutionary):

    fields = "count(distinct(c.id)) as WaitingForSubmitter "
    tables = "  changes c, "+\
             "   issues i, "+\
             "        (select c.issue_id as issue_id, "+\
             "                c.old_value as old_value, "+\
             "                max(c.id) as id "+\
             "         from changes c, "+\
             "              issues i "+\
             "         where c.issue_id = i.id and "+\
             "               i.status='NEW' "+\
             "         group by c.issue_id, c.old_value) t1 "
    tables = tables + GetSQLReportFromSCR(identities_db, type_analysis)
    filters = " i.id = c.issue_id "+\
              "  and t1.id = c.id "+\
              "  and (c.field='CRVW' or c.field='Code-Review' or c.field='Verified' or c.field='VRIF') "+\
              "  and (c.new_value=-1 or c.new_value=-2) "
    filters = filters + GetSQLReportWhereSCR(type_analysis, identities_db)

    if (evolutionary):
        q = GetSQLPeriod(period, " c.changed_on", fields, tables, filters,
                          startdate, enddate)
    else:
        q = GetSQLGlobal(" c.changed_on ", fields, tables, filters,
                          startdate, enddate)
    return(ExecuteQuery(q))


def EvolWaiting4Submitter (period, startdate, enddate, identities_db=None, type_analysis = []):
    return (GetWaiting4Submitter(period, startdate, enddate, identities_db, type_analysis, True))


def StaticWaiting4Submitter (period, startdate, enddate, identities_db=None, type_analysis = []):
    return (GetWaiting4Submitter(period, startdate, enddate, identities_db, type_analysis, False))


#REVIEWERS

def GetReviewers (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # TODO: so far without unique identities

    fields = " count(distinct(changed_by)) as reviewers "
    tables = " changes c "
    filters = ""

    if (evolutionary):
        q = GetSQLPeriod(period, " c.changed_on", fields, tables, filters,
                          startdate, enddate)
    else:
        q = GetSQLGlobal(" c.changed_on ", fields, tables, filters,
                          startdate, enddate)
    return(ExecuteQuery(q))


def EvolReviewers  (period, startdate, enddate, identities_db=None, type_analysis = []):
    return (GetReviewers(period, startdate, enddate, identities_db, type_analysis, True))


def StaticReviewers  (period, startdate, enddate, identities_db = None, type_analysis = []):
    return (GetReviewers(period, startdate, enddate, identities_db, type_analysis, False))


# Nobody is using it yet
def GetLongestReviews  (startdate, enddate, identities_db, type_analysis = []):

#    q = "select i.issue as review, "+\
#        "         t1.old_value as patch, "+\
#        "         timestampdiff (HOUR, t1.min_time, t1.max_time) as timeOpened "+\
#        "  from ( "+\
#        "        select c.issue_id as issue_id, "+\
#        "               c.old_value as old_value, "+\
#        "               min(c.changed_on) as min_time, "+\
#        "               max(c.changed_on) as max_time "+\
#        "        from changes c, "+\
#        "             issues i "+\
#        "        where c.issue_id = i.id and "+\
#        "              i.status='NEW' "+\
#        "        group by c.issue_id, "+\
#        "                 c.old_value) t1, "+\
#        "       issues i "+\
#        "  where t1.issue_id = i.id "+\
#        "  order by timeOpened desc "+\
#        "  limit 20"
    fields = " i.issue as review, " + \
             " t1.old_value as patch, " + \
            " timestampdiff (HOUR, t1.min_time, t1.max_time) as timeOpened, "
    tables = " issues i, "+\
            " (select c.issue_id as issue_id, "+\
            "           c.old_value as old_value, "+\
            "           min(c.changed_on) as min_time, "+\
            "           max(c.changed_on) as max_time "+\
            "    from changes c, "+\
            "         issues i "+\
            "    where c.issue_id = i.id and "+\
            "          i.status='NEW' "+\
            "    group by c.issue_id, "+\
            "             c.old_value) t1 "
    tables = tables + GetSQLReportFromSCR(identities_db, type_analysis)
    filters = " t1.issue_id = i.id "
    filters = filters + GetSQLReportWhereSCR(type_analysis, identities_db)

    q = GetSQLGlobal(" i.submitted_on ", fields, tables, filters,
                           startdate, enddate)

    return(ExecuteQuery(q))

##
# Tops
##

# Is this right???
def GetTopReviewersSCR (days, startdate, enddate, identities_db, bots, limit):
    date_limit = ""
    filter_bots = ''
    for bot in bots:
        filter_bots = filter_bots + " up.identifier<>'"+bot+"' and "

    if (days != 0 ):
        q = "SELECT @maxdate:=max(changed_on) from changes limit 1"
        ExecuteQuery(q)
        date_limit = " AND DATEDIFF(@maxdate, changed_on)<" + str(days)

    q = "SELECT up.id as id, up.identifier as reviewers, "+\
        "               count(distinct(c.id)) as reviewed "+\
        "        FROM people_upeople pup, changes c, "+ identities_db+".upeople up "+\
        "        WHERE "+ filter_bots+ " "+\
        "            c.changed_by = pup.people_id and "+\
        "            pup.upeople_id = up.id and "+\
        "            c.changed_on >= "+ startdate + " and "+\
        "            c.changed_on < "+ enddate + " "+\
        "            "+ date_limit + " "+\
        "        GROUP BY up.identifier "+\
        "        ORDER BY reviewed desc, reviewers "+\
        "        LIMIT " + limit
    return(ExecuteQuery(q))


def GetTopSubmittersQuerySCR   (days, startdate, enddate, identities_db, bots, limit, merged = False):
    date_limit = ""
    merged_sql = ""
    rol = "openers"
    action = "opened"
    filter_bots = ''
    for bot in bots:
        filter_bots = filter_bots+ " up.identifier<>'"+bot+"' and "

    if (days != 0 ):
        q = "SELECT @maxdate:=max(submitted_on) from issues limit 1"
        ExecuteQuery(q)
        date_limit = " AND DATEDIFF(@maxdate, submitted_on)<"+str(days)

    if (merged):
        merged_sql = " AND status='MERGED' "
        rol = "mergers"
        action = "merged"


    q = "SELECT up.id as id, up.identifier as "+rol+", "+\
        "            count(distinct(i.id)) as "+action+" "+\
        "        FROM people_upeople pup, issues i, "+identities_db+".upeople up "+\
        "        WHERE "+ filter_bots+ " "+\
        "            i.submitted_by = pup.people_id and "+\
        "            pup.upeople_id = up.id and "+\
        "            i.submitted_on >= "+ startdate+ " and "+\
        "            i.submitted_on < "+ enddate+ " "+\
        "            "+date_limit+ merged_sql+ " "+\
        "        GROUP BY up.identifier "+\
        "        ORDER BY "+action+" desc, id "+\
        "        LIMIT "+ limit
    return(q)


def GetTopOpenersSCR (days, startdate, enddate, identities_db, bots, limit):
    q = GetTopSubmittersQuerySCR (days, startdate, enddate, identities_db, bots, limit)
    return(ExecuteQuery(q))


def GetTopMergersSCR   (days, startdate, enddate, identities_db, bots, limit):
    q = GetTopSubmittersQuerySCR (days, startdate, enddate, identities_db, bots, limit, True)
    return(ExecuteQuery(q))


#########
# PEOPLE: Pretty similar to ITS
#########
def GetTablesOwnUniqueIdsSCR (table=''):
    tables = 'changes c, people_upeople pup'
    if (table == "issues"): tables = 'issues i, people_upeople pup'
    return (tables)


def GetFiltersOwnUniqueIdsSCR  (table=''):
    filters = 'pup.people_id = c.changed_by'
    if (table == "issues"): filters = 'pup.people_id = i.submitted_by'
    return (filters)


def GetPeopleListSCR (startdate, enddate, bots):

    filter_bots = ""
    for bot in bots:
        filter_bots += " name<>'"+bot+"' and "

    fields = "DISTINCT(pup.upeople_id) as id, count(i.id) as total, name"
    tables = GetTablesOwnUniqueIdsSCR('issues') + ", people"
    filters = filter_bots
    filters += GetFiltersOwnUniqueIdsSCR('issues')+ " and people.id = pup.people_id"
    filters += " GROUP BY id ORDER BY total desc"
    q = GetSQLGlobal('submitted_on', fields, tables, filters, startdate, enddate)
    return(ExecuteQuery(q))


def GetPeopleQuerySCR (developer_id, period, startdate, enddate, evol):
    fields = "COUNT(c.id) AS closed"
    tables = GetTablesOwnUniqueIdsSCR()
    filters = GetFiltersOwnUniqueIdsSCR()+ " AND pup.upeople_id = "+ str(developer_id)

    if (evol):
        q = GetSQLPeriod(period,'changed_on', fields, tables, filters,
                startdate, enddate)
    else:
        fields = fields + \
                ",DATE_FORMAT (min(changed_on),'%Y-%m-%d') as first_date, "+\
                "  DATE_FORMAT (max(changed_on),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('changed_on', fields, tables, filters,
                startdate, enddate)
    return (q)


def GetPeopleEvolSCR (developer_id, period, startdate, enddate):
    q = GetPeopleQuerySCR(developer_id, period, startdate, enddate, True)
    return(ExecuteQuery(q))

def GetPeopleStaticSCR (developer_id, startdate, enddate):
    q = GetPeopleQuerySCR(developer_id, None, startdate, enddate, False)
    return(ExecuteQuery(q))

################
# Time to review
################

# Real reviews spend >1h, are not autoreviews, and bots are filtered out.
def GetTimeToReviewQuerySCR (startdate, enddate, identities_db = None, type_analysis = [], bots = []):
    filter_bots = ''
    for bot in bots:
        filter_bots = filter_bots + " people.name<>'"+bot+"' and "

    # Subquery to get the time to review for all reviews
    # fields = "DATEDIFF(changed_on,submitted_on) AS revtime, changed_on "
    # fields = "TIMEDIFF(changed_on,submitted_on)/(24*3600) AS revtime, changed_on "
    fields = "TIMESTAMPDIFF(SECOND, submitted_on, changed_on)/(24*3600) AS revtime, changed_on "
    tables = "issues i, changes, people "
    tables = tables + GetSQLReportFromSCR(identities_db, type_analysis)
    filters = filter_bots + " i.id = changes.issue_id "
    filters += " AND people.id = changes.changed_by "
    filters += GetSQLReportWhereSCR(type_analysis, identities_db)
    filters += " AND field='status' AND new_value='MERGED' "
    # remove autoreviews
    filters += " AND i.submitted_by<>changes.changed_by "
    filters += " ORDER BY changed_on "
    q = GetSQLGlobal('changed_on', fields, tables, filters,
                    startdate, enddate)
    min_days_for_review = 0.042 # one hour
    q = "SELECT revtime, changed_on FROM ("+q+") qrevs WHERE revtime>"+str(min_days_for_review)
    return (q)

# Average can be calculate directly from SQL. But not used.
def EvolTimeToReviewSCRsql (period, startdate, enddate, identities_db = None, type_analysis = []):
    q = GetTimeToReviewQuerySCR (startdate, enddate, identities_db, type_analysis)
    # Evolution in time of AVG review time
    fields = "SUM(revtime)/COUNT(revtime) AS review_time_days_avg "
    tables = "("+q+") t"
    filters = ""
    q = GetSQLPeriod(period,'changed_on', fields, tables, filters,
            startdate, enddate)
    data = ExecuteQuery(q)
    if not isinstance(data['review_time_days_avg'], (list)): 
        data['review_time_days_avg'] = [data['review_time_days_avg']]
    return(data)

# Average can be calculate directly from SQL. But not used.
def StaticTimeToReviewSCRsql (startdate, enddate, identities_db = None, type_analysis = []):
    q = GetTimeToReviewQuerySCR (startdate, enddate, identities_db, type_analysis)
    # Total AVG review time
    q = " SELECT AVG(revtime) AS review_time_days_avg FROM ("+q+") t"
    return(ExecuteQuery(q))

def StaticTimeToReviewSCR (startdate, enddate, identities_db = None, type_analysis = [], bots = []):
    data = ExecuteQuery(GetTimeToReviewQuerySCR (startdate, enddate, identities_db, type_analysis, bots))
    data = data['revtime']
    if (isinstance(data, list) == False): data = [data]
    # ttr_median = sorted(data)[len(data)//2]
    if (len(data) == 0):
        ttr_median = float("nan")
        ttr_avg = float("nan")
    else:
        ttr_median = median(removeDecimals(data))
        ttr_avg = average(removeDecimals(data))
    return {"review_time_days_median":ttr_median, "review_time_days_avg":ttr_avg}


def EvolTimeToReviewSCR (period, startdate, enddate, identities_db = None, type_analysis = []):
    q = GetTimeToReviewQuerySCR (startdate, enddate, identities_db, type_analysis)
    review_list = ExecuteQuery(q)
    checkListArray(review_list)

    metrics_list = {"month":[],"review_time_days_median":[],"review_time_days_avg":[]}
    # metrics_list = {"month":[],"review_time_days_median":[]}
    review_list_len = len(review_list['changed_on'])
    if len(review_list['changed_on']) == 0: return metrics_list
    start = review_list['changed_on'][0]
    start_month = start.year*12 + start.month
    # end = review_list['changed_on'][review_list_len-1]
    # end_month = end.year*12 + end.month
    month = start_month

    metrics_data = []
    for i in range (0,review_list_len):
        date = review_list['changed_on'][i]
        if (date.year*12 + date.month) > month:
            metrics_list['month'].append(month)
            if review_list_len == 1: 
                metrics_data.append (review_list['revtime'][i])
            if len(metrics_data) == 0: 
                ttr_median = float('nan')
                ttr_avg = float('nan')
            else: 
                ttr_median = median(removeDecimals(metrics_data))
                ttr_avg = average(removeDecimals(metrics_data))
            # avg = sum(median) / float(len(median))
            # metrics_list['review_time_avg'].append(avg)
            metrics_list['review_time_days_median'].append(ttr_median)
            metrics_list['review_time_days_avg'].append(ttr_avg)
            metrics_data = [review_list['revtime'][i]]
            month = date.year*12 + date.month
        if  i == review_list_len-1:
            month = date.year*12 + date.month

            # Close last month also
            if (date.year*12 + date.month) > month:
                metrics_data = [review_list['revtime'][i]]
            elif (date.year*12 + date.month) == month:
                metrics_data.append (review_list['revtime'][i])
            metrics_list['month'].append(month)
            ttr_median = median(removeDecimals(metrics_data))
            ttr_avg = average(removeDecimals(metrics_data))
            metrics_list['review_time_days_median'].append(ttr_median)
            metrics_list['review_time_days_avg'].append(ttr_avg)
        else: metrics_data.append (review_list['revtime'][i])
    return metrics_list

##############
# Microstudies
##############

def GetSCRDiffSubmittedDays (period, init_date, days,
        identities_db=None, type_analysis = []):
    chardates = GetDates(init_date, days)
    last = StaticReviewsSubmitted(period, chardates[1], chardates[0])
    last = int(last['submitted'])
    prev = StaticReviewsSubmitted(period, chardates[2], chardates[1])
    prev = int(prev['submitted'])

    data = {}
    data['diff_netsubmitted_'+str(days)] = last - prev
    data['percentage_submitted_'+str(days)] = GetPercentageDiff(prev, last)
    data['submitted_'+str(days)] = last
    return (data)

def GetSCRDiffMergedDays (period, init_date, days,
        identities_db=None, type_analysis = []):

    chardates = GetDates(init_date, days)
    last = StaticReviewsMerged(period, chardates[1], chardates[0])
    last = int(last['merged'])
    prev = StaticReviewsMerged(period, chardates[2], chardates[1])
    prev = int(prev['merged'])

    data = {}
    data['diff_netmerged_'+str(days)] = last - prev
    data['percentage_merged_'+str(days)] = GetPercentageDiff(prev, last)
    data['merged_'+str(days)] = last
    return (data)

def GetSCRDiffAbandonedDays (period, init_date, days,
        identities_db=None, type_analysis = []):

    chardates = GetDates(init_date, days)
    last = StaticReviewsAbandoned(period, chardates[1], chardates[0])
    last = int(last['abandoned'])
    prev = StaticReviewsAbandoned(period, chardates[2], chardates[1])
    prev = int(prev['abandoned'])

    data = {}
    data['diff_netabandoned_'+str(days)] = last - prev
    data['percentage_abandoned_'+str(days)] = GetPercentageDiff(prev, last)
    data['abandoned_'+str(days)] = last
    return (data)


def GetSCRDiffPendingDays (period, init_date, days,
        identities_db=None, type_analysis = []):

    chardates = GetDates(init_date, days)
    last = StaticReviewsPending(period, chardates[1], chardates[0])
    last = int(last['pending'])
    prev = StaticReviewsPending(period, chardates[2], chardates[1])
    prev = int(prev['pending'])

    data = {}
    data['diff_netpending_'+str(days)] = last - prev
    data['percentage_pending_'+str(days)] = GetPercentageDiff(prev, last)
    data['pending_'+str(days)] = last
    return (data)
