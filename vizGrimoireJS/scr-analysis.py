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
# This file is a part of the vizGrimoire.R package
#
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>
#
#
# Usage:
#     PYTHONPATH=../vizgrimoire LANG= R_LIBS=../../r-lib ./scr-analysis.py 
#                                                -d acs_irc_automatortest_2388_2 -u root 
#                                                -i acs_cvsanaly_automatortest_2388 
#                                                -s 2010-01-01 -e 2014-01-20 
#                                                -o ../../../json -r people,repositories
#

from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
import logging
import sys
from Wikimedia import GetCompaniesQuartersSCR, GetPeopleQuartersSCR
from Wikimedia import GetNewSubmitters, GetNewMergers, GetNewAbandoners
from Wikimedia import GetGoneSubmitters, GetGoneMergers, GetGoneAbandoners
from Wikimedia import GetNewSubmittersActivity, GetGoneSubmittersActivity
from Wikimedia import GetPeopleIntakeSQL

import GrimoireUtils, GrimoireSQL
from GrimoireUtils import dataFrame2Dict, createJSON, completePeriodIds
from GrimoireUtils import valRtoPython, read_options, getPeriod, checkFloatArray
import SCR

def aggData(period, startdate, enddate, idb, destdir, bots):
    # Wikimedia data ok after '2013-04-30' for changes based metrics
    startok = "'2013-04-30'"

    agg = SCR.StaticReviewsSubmitted(period, startdate, enddate)
    data = SCR.StaticReviewsOpened(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsNew(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsInProgress(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsClosed(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsMerged(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsAbandoned(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsPending(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticPatchesVerified(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticPatchesApproved(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticPatchesCodeReview(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticPatchesSent(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticWaiting4Reviewer(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticWaiting4Submitter(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    # Waiting reviews
    data = SCR.StaticReviewsWaiting4Submitter(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    data = SCR.StaticReviewsWaiting4Reviewer(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    #Reviewers info
    data = SCR.StaticReviewers(period, startdate, enddate)
    agg = dict(agg.items() + data.items())
    # Time to Review info
    data = SCR.StaticTimeToReviewSCR(startok, enddate, idb, [], bots)
    data['review_time_days_avg'] = float(data['review_time_days_avg'])
    data['review_time_days_median'] = float(data['review_time_days_median'])
    agg = dict(agg.items() + data.items())
    data = SCR.StaticTimeToReviewPendingSCR(startok, enddate, idb, [], bots)
    data['review_time_pending_days_avg'] = float(data['review_time_pending_days_avg'])
    data['review_time_pending_days_median'] = float(data['review_time_pending_days_median'])
    agg = dict(agg.items() + data.items())

    # Tendencies
    for i in [7,30,365]:
        period_data = SCR.GetSCRDiffSubmittedDays(period, enddate, i, idb)
        agg = dict(agg.items() + period_data.items())
        period_data = SCR.GetSCRDiffMergedDays(period, enddate, i, idb)
        agg = dict(agg.items() + period_data.items())
        period_data = SCR.GetSCRDiffPendingDays(period, enddate, i, idb)
        agg = dict(agg.items() + period_data.items())
        period_data = SCR.GetSCRDiffAbandonedDays(period, enddate, i, idb)
        agg = dict(agg.items() + period_data.items())

    # Create JSON
    createJSON(agg, destdir+"/scr-static.json")

def tsData(period, startdate, enddate, idb, destdir, granularity, conf):
    # Wikimedia data ok after '2013-04-30' for changes based metrics
    startok = "'2013-04-30'"

    evol = {}
    data = SCR.EvolReviewsSubmitted(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsOpened(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsNew(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsNewChanges(period, startok, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    # data = SCR.EvolReviewsInProgress(period, startdate, enddate)
    # evol = dict(evol.items() + completePeriodIds(data).items())
    data = SCR.EvolReviewsClosed(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsMerged(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsMergedChanges(period, startok, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsAbandoned(period, startok, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsAbandonedChanges(period, startok, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsPending(period, startdate, enddate, [])
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    #Patches info
    data = SCR.EvolPatchesVerified(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    # data = SCR.EvolPatchesApproved(period, startdate, enddate)
    # evol = dict(evol.items() + completePeriodIds(data).items())
    data = SCR.EvolPatchesCodeReview(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolPatchesSent(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    #Waiting for actions info
    data = SCR.EvolWaiting4Reviewer(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolWaiting4Submitter(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    data = SCR.EvolReviewsWaiting4Reviewer(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    #Reviewers info
    data = SCR.EvolReviewers(period, startdate, enddate)
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    # Time to Review info
    data = SCR.EvolTimeToReviewSCR (period, startok, enddate)
#    for i in range(0,len(data['review_time_days_avg'])):
#        val = data['review_time_days_avg'][i]
#        data['review_time_days_avg'][i] = float(val)
#        if (val == 0): data['review_time_days_avg'][i] = 0
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

    data = SCR.EvolTimeToReviewPendingSCR(period, startdate, enddate)

#    for i in range(0,len(data['review_time_pending_days_avg'])):
#        val = data['review_time_pending_days_avg'][i]
#        data['review_time_pending_days_avg'][i] = float(val)
#        if (val == 0): data['review_time_pending_days_avg'][i] = 0
    evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
    # Create JSON
    createJSON(evol, destdir+"/scr-evolutionary.json")

# Unify top format
def safeTopIds(top_data_period):
    if not isinstance(top_data_period['id'], (list)):
        for name in top_data_period:
            top_data_period[name] = [top_data_period[name]]
    return top_data_period['id']

def peopleData(period, startdate, enddate, idb, destdir, top_data):
    top = safeTopIds(top_data['reviewers'])
    top += safeTopIds(top_data['reviewers.last year'])
    top += safeTopIds(top_data['reviewers.last month'])
    top += safeTopIds(top_data['openers.'])
    top += safeTopIds(top_data['openers.last year'])
    top += safeTopIds(top_data['openers.last_month'])
    top += safeTopIds(top_data['mergers.'])
    top += safeTopIds(top_data['mergers.last year'])
    top += safeTopIds(top_data['mergers.last_month'])
    # remove duplicates
    people = list(set(top)) 
    createJSON(people, destdir+"/scr-people.json")

    for upeople_id in people:
        evol = SCR.GetPeopleEvolSCR(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        createJSON(evol, destdir+"/people-"+str(upeople_id)+"-scr-evolutionary.json")

        agg = SCR.GetPeopleStaticSCR(upeople_id, startdate, enddate)
        createJSON(agg, destdir+"/people-"+str(upeople_id)+"-scr-static.json")

def reposData(period, startdate, enddate, idb, destdir, conf):
    startok = "'2013-04-30'"
    repos  = SCR.GetReposSCRName(startdate, enddate)
    repos = repos["name"]
    # For repos aggregated data. Include metrics to sort in javascript.
    repos_list = {"name":[],"review_time_days_median":[],"review_time_pending_days_median":[],
                  "submitted":[],"new":[],"ReviewsWaitingForReviewer":[],
                  "review_time_pending_ReviewsWaitingForReviewer_days_median":[],
                  "review_time_pending_update_ReviewsWaitingForReviewer_days_median":[]}

    # missing information from the rest of type of reviews, patches and
    # number of patches waiting for reviewer and submitter 
    for repo in repos:
        repo_file = repo.replace("/","_")
        logging.info(repo_file)
        repos_list["name"].append(repo_file)
        # logging.info("Repo: " + repo_file)
        type_analysis = ['repository', repo]

        evol = {}
        data = SCR.EvolReviewsSubmitted(period, startdate, enddate, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsMerged(period, startdate, enddate, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsAbandoned(period, startdate, enddate, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsNew(period, startdate, enddate, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

        # data = vizr.EvolReviewsPendingChanges(period, startdate, enddate, conf, type_analysis)
        # evol = dict(evol.items() + completePeriodIds(dataFrame2Dict(data, period, startdate, enddate)).items())
        # data = SCR.EvolReviewsPendingChanges(period, startdate, enddate, conf, type_analysis, idb)
        data = SCR.EvolReviewsPending(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolTimeToReviewSCR(period, startok, enddate, idb, type_analysis)
        data['review_time_days_avg'] = checkFloatArray(data['review_time_days_avg'])
        data['review_time_days_median'] = checkFloatArray(data['review_time_days_median'])
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolTimeToReviewPendingSCR (period, startok, enddate, idb, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsWaiting4Reviewer (period, startok, enddate, idb, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        createJSON(evol, destdir+ "/"+repo_file+"-scr-rep-evolutionary.json")

        # Static
        agg = {}
        data = SCR.StaticReviewsSubmitted(period, startdate, enddate, type_analysis)
        repos_list["submitted"].append(data["submitted"])
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsMerged(period, startdate, enddate, type_analysis)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsAbandoned(period, startdate, enddate, type_analysis)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsNew(period, startdate, enddate, type_analysis)
        repos_list["new"].append(data["new"])
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsPending(period, startdate, enddate, type_analysis)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticTimeToReviewSCR(startok, enddate, idb, type_analysis)
        val = data['review_time_days_avg']
        if (not val or val == 0): data['review_time_days_avg'] = 0
        else: data['review_time_days_avg'] = float(val)
        val = data['review_time_days_median']
        if (not val or val == 0): data['review_time_days_median'] = 0
        else: data['review_time_days_median'] = float(val)
        agg = dict(agg.items() + data.items())
        repos_list["review_time_days_median"].append(data['review_time_days_median'])
        data = SCR.StaticTimeToReviewPendingSCR(startok, enddate, idb, type_analysis, bots)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsWaiting4Reviewer(period, startok, enddate, idb,  type_analysis)
        repos_list["ReviewsWaitingForReviewer"].append(data['ReviewsWaitingForReviewer'])
        agg = dict(agg.items() + data.items())
        repos_list["review_time_pending_days_median"].append(data['review_time_pending_days_median'])
        repos_list["review_time_pending_update_days_median"].append(data['review_time_pending_update_days_median'])
        repos_list["review_time_pending_ReviewsWaitingForReviewer_days_median"].append(data['review_time_pending_ReviewsWaitingForReviewer_days_median'])
        repos_list["review_time_pending_update_ReviewsWaitingForReviewer_days_median"].append(data['review_time_pending_update_ReviewsWaitingForReviewer_days_median'])
        createJSON(agg, destdir + "/"+repo_file + "-scr-rep-static.json")

    createJSON(repos_list, destdir+"/scr-repos.json")

def companiesData(period, startdate, enddate, idb, destdir):
    startok = "'2013-04-30'"
    # companies  = dataFrame2Dict(vizr.GetCompaniesSCRName(startdate, enddate, idb))
    companies  = SCR.GetCompaniesSCRName(startdate, enddate, idb)
    companies = companies['name']
    companies_files = [company.replace('/', '_') for company in companies]
    createJSON(companies_files, destdir+"/scr-companies.json")

    # missing information from the rest of type of reviews, patches and
    # number of patches waiting for reviewer and submitter 
    for company in companies:
        logging.info("Company: " + company)
        company_file = company.replace("/","_")
        type_analysis = ['company', company]
        # Evol
        evol = {}
        data = SCR.EvolReviewsSubmitted(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsMerged(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsAbandoned(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolTimeToReviewSCR(period, startok, enddate, idb, type_analysis)
        data['review_time_days_avg'] = checkFloatArray(data['review_time_days_avg'])
        data['review_time_days_median'] = checkFloatArray(data['review_time_days_median'])
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolTimeToReviewPendingSCR (period, startok, enddate, idb, type_analysis)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        createJSON(evol, destdir+ "/"+company_file+"-scr-com-evolutionary.json")
        # Static
        agg = {}
        data = SCR.StaticReviewsSubmitted(period, startdate, enddate, type_analysis, idb)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsMerged(period, startdate, enddate, type_analysis, idb)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsAbandoned(period, startdate, enddate, type_analysis, idb)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticTimeToReviewSCR(startok, enddate, idb, type_analysis)
        val = data['review_time_days_avg']
        if (not val or val == 0): data['review_time_days_avg'] = 0
        else: data['review_time_days_avg'] = float(val)
        val = data['review_time_days_median']
        if (not val or val == 0): data['review_time_days_median'] = 0
        else: data['review_time_days_median'] = float(val)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticTimeToReviewPendingSCR(startok, enddate, idb, type_analysis, bots)
        agg = dict(agg.items() + data.items())
        createJSON(agg, destdir+"/"+company_file+"-scr-com-static.json")


def countriesData(period, startdate, enddate, idb, destdir):
    countries  = SCR.GetCountriesSCRName(startdate, enddate, idb)
    countries = countries['name']
    countries_files = [country.replace('/', '_') for country in countries]
    createJSON(countries_files, destdir+"/scr-countries.json")

    # missing information from the rest of type of reviews, patches and
    # number of patches waiting for reviewer and submitter 
    for country in countries:
        country_file = country.replace("/","_")
        type_analysis = ['country', country]
        # Evol
        evol = {}
        data = SCR.EvolReviewsSubmitted(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsMerged(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = SCR.EvolReviewsAbandoned(period, startdate, enddate, type_analysis, idb)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items()) 
        createJSON(evol, destdir+ "/"+country_file+"-scr-cou-evolutionary.json")

        # Static
        agg = {}
        data = SCR.StaticReviewsSubmitted(period, startdate, enddate, type_analysis, idb)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsMerged(period, startdate, enddate, type_analysis, idb)
        agg = dict(agg.items() + data.items())
        data = SCR.StaticReviewsAbandoned(period, startdate, enddate, type_analysis, idb)
        agg = dict(agg.items() + data.items())
        createJSON(agg, destdir+"/"+country_file+"-scr-cou-static.json")

def topData(period, startdate, enddate, idb, destdir, bots, npeople):
    top_reviewers = {}
    top_reviewers['reviewers'] = SCR.GetTopReviewersSCR(0, startdate, enddate, idb, bots, npeople)
    top_reviewers['reviewers.last year']= SCR.GetTopReviewersSCR(365, startdate, enddate, idb, bots, npeople)
    top_reviewers['reviewers.last month']= SCR.GetTopReviewersSCR(31, startdate, enddate, idb, bots, npeople)

    # Top openers
    top_openers = {}
    top_openers['openers.']=SCR.GetTopOpenersSCR(0, startdate, enddate,idb, bots, npeople)
    top_openers['openers.last year']=SCR.GetTopOpenersSCR(365, startdate, enddate,idb, bots, npeople)
    top_openers['openers.last_month']=SCR.GetTopOpenersSCR(31, startdate, enddate,idb, bots, npeople)

    # Top mergers
    top_mergers = {}
    top_mergers['mergers.last year']=SCR.GetTopMergersSCR(365, startdate, enddate,idb, bots, npeople)
    top_mergers['mergers.']=SCR.GetTopMergersSCR(0, startdate, enddate,idb, bots, npeople)
    top_mergers['mergers.last_month']=SCR.GetTopMergersSCR(31, startdate, enddate,idb, bots, npeople)

    # The order of the list item change so we can not check it
    top_all = dict(top_reviewers.items() +  top_openers.items() + top_mergers.items())
    createJSON (top_all, destdir+"/scr-top.json")

    return (top_all)

def quartersData(period, startdate, enddate, idb, destdir, bots):
    # Needed files. Ugly hack for date format
    people = SCR.GetPeopleListSCR("'"+startdate+"'", "'"+enddate+"'", bots)
    createJSON(people, destdir+"/scr-people-all.json", False)
    companies = SCR.GetCompaniesSCRName("'"+startdate+"'", "'"+enddate+"'", idb)
    createJSON(companies, destdir+"/scr-companies-all.json", False)

    start = datetime.strptime(startdate, "%Y-%m-%d")
    start_quarter = (start.month-1)%3 + 1
    end = datetime.strptime(enddate, "%Y-%m-%d")
    end_quarter = (end.month-1)%3 + 1

    companies_quarters = {}
    people_quarters = {}

    quarters = (end.year - start.year) * 4 + (end_quarter - start_quarter)

    for i in range(0, quarters):
        year = start.year
        quarter = (i%4)+1
        # logging.info("Analyzing companies and people quarter " + str(year) + " " +  str(quarter))
        data = GetCompaniesQuartersSCR(year, quarter, idb)
        companies_quarters[str(year)+" "+str(quarter)] = data
        data_people = GetPeopleQuartersSCR(year, quarter, idb, 25, bots)
        people_quarters[str(year)+" "+str(quarter)] = data_people
        start = start + relativedelta(months=3)
    createJSON(companies_quarters, destdir+"/scr-companies-quarters.json")
    createJSON(people_quarters, destdir+"/scr-people-quarters.json")

def CodeContribKPI(destdir):
    code_contrib = {}
    code_contrib["submitters"] = GetNewSubmitters()
    code_contrib["mergers"] = GetNewMergers()
    code_contrib["abandoners"] = GetNewAbandoners()
    createJSON(code_contrib, destdir+"/scr-code-contrib-new.json")

    code_contrib = {}
    code_contrib["submitters"] = GetGoneSubmitters()
    code_contrib["mergers"] = GetGoneMergers()
    code_contrib["abandoners"] = GetGoneAbandoners()
    createJSON(code_contrib, destdir+"/scr-code-contrib-gone.json")


    data = GetNewSubmittersActivity()
    evol = {}
    evol['people'] = {}
    for upeople_id in data['upeople_id']:
        pdata = SCR.GetPeopleEvolSCR(upeople_id, period, startdate, enddate)
        pdata = completePeriodIds(pdata, period, startdate, enddate)
        evol['people'][upeople_id] = {"submissions":pdata['submissions']}
        # Just to have the time series data
        evol = dict(evol.items() + pdata.items())
    if 'changes' in evol:
        del evol['changes'] # closed (metrics) is included in people
    createJSON(evol, destdir+"/new-people-activity-scr-evolutionary.json")

    data = GetGoneSubmittersActivity()
    evol = {}
    evol['people'] = {}
    for upeople_id in data['upeople_id']:
        pdata = SCR.GetPeopleEvolSCR(upeople_id, period, startdate, enddate)
        pdata = completePeriodIds(pdata, period, startdate, enddate)
        evol['people'][upeople_id] = {"submissions":pdata['submissions']}
        # Just to have the time series data
        evol = dict(evol.items() + pdata.items())
    if 'changes' in evol:
        del evol['changes'] # closed (metrics) is included in people
    createJSON(evol, destdir+"/gone-people-activity-scr-evolutionary.json")

    # data = GetPeopleLeaving()
    # createJSON(data, destdir+"/leaving-people-scr.json")

    evol = {}
    data = completePeriodIds(GetPeopleIntakeSQL(0,1), period, startdate, enddate)
    evol['month'] = data['month']
    evol['id'] = data['id']
    evol['date'] = data['date']
    evol['num_people_1'] = data['people']
    evol['num_people_1_5'] = completePeriodIds(GetPeopleIntakeSQL(1,5),period, startdate, enddate)['people']
    evol['num_people_5_10'] = completePeriodIds(GetPeopleIntakeSQL(5,10), period, startdate, enddate)['people']
    createJSON(evol, destdir+"/scr-people-intake-evolutionary.json")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting SCR data source analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")
    # filtered bots

    bots = ['wikibugs','gerrit-wm','wikibugs_','wm-bot','','Translation updater bot','jenkins-bot','L10n-bot']
    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+opts.startdate+"'"
    enddate = "'"+opts.enddate+"'"

    GrimoireSQL.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)

    tsData (period, startdate, enddate, opts.identities_db, opts.destdir, opts.granularity, opts)
    aggData(period, startdate, enddate, opts.identities_db, opts.destdir, bots)
    quartersData(period, opts.startdate, opts.enddate, opts.identities_db, opts.destdir, bots)
    top = topData(period, startdate, enddate, opts.identities_db, opts.destdir, bots, opts.npeople)

    if ('people' in reports):
        peopleData (period, startdate, enddate, opts.identities_db, opts.destdir, top)
    if ('repositories' in reports):
        reposData (period, startdate, enddate, opts.identities_db, opts.destdir, opts)
    if ('countries' in reports):
        countriesData (period, startdate, enddate, opts.identities_db, opts.destdir)
    if ('companies' in reports):
        companiesData (period, startdate, enddate, opts.identities_db, opts.destdir)

    # Specific Wikiemdia KPI analysis
    CodeContribKPI(opts.destdir)

    logging.info("SCR data source analysis OK")
