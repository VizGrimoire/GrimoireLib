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
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Alvaro del Castillo San Felix <acs@bitergia.com>
##   Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#
# Usage:
#     PYTHONPATH=../vizgrimoire LANG= R_LIBS=../../r-lib ./mls-analysis.py 
#                                                -d acs_mlstats_automatortest_2388 -u root 
#                                                -i acs_cvsanaly_automatortest_2388 
#                                                -s 2010-01-01 -e 2014-01-20 
#                                                -o ../../../json -r people,repositories
#

import logging
from rpy2.robjects.packages import importr
import sys
import datetime

from threads import Threads

isoweek = importr("ISOweek")
vizr = importr("vizgrimoire")

import GrimoireUtils, GrimoireSQL
from GrimoireUtils import dataFrame2Dict, createJSON, completePeriodIds
from GrimoireUtils import valRtoPython, read_options, getPeriod
import MLS

def aggData(period, startdate, enddate, identities_db, destdir):
    data = MLS.StaticMLSInfo(period, startdate, enddate, identities_db, rfield)
    agg = data


    if ('companies' in reports):
        data = MLS.AggMLSCompanies(period, startdate, enddate, identities_db)
        agg = dict(agg.items() + data.items())

    if ('countries' in reports):
        data = MLS.AggMLSCountries(period, startdate, enddate, identities_db)
        agg = dict(agg.items() + data.items())

    if ('domains' in reports):
        data = MLS.AggMLSDomains(period, startdate, enddate, identities_db)
        agg = dict(agg.items() + data.items())

    # Tendencies
    for i in [7,30,365]:
        period_data = MLS.GetDiffSentDays(period, enddate, i)
        agg = dict(agg.items() + period_data.items())
        period_data = MLS.GetDiffSendersDays(period, enddate, i)
        agg = dict(agg.items() + period_data.items())

    # Last Activity: to be removed
    for i in [7,14,30,60,90,180,365,730]:
        period_activity = MLS.lastActivity(i)
        agg = dict(agg.items() + period_activity.items())

    createJSON (agg, destdir+"/mls-static.json")

def tsData(period, startdate, enddate, identities_db, destdir, granularity, conf):

    evol = {}
    data = MLS.EvolMLSInfo(period, startdate, enddate, identities_db, rfield)
    evol = dict(evol.items() + completePeriodIds(data).items())


    if ('companies' in reports):
        data  = MLS.EvolMLSCompanies(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data).items())

    if ('countries' in reports):
        data = MLS.EvolMLSCountries(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data).items())

    if ('domains' in reports):
        data = MLS.EvolMLSDomains(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data).items())

    createJSON (evol, destdir+"/mls-evolutionary.json")


def peopleData(period, startdate, enddate, identities_db, destdir, top_data):
    top = top_data['senders.']["id"]
    top += top_data['senders.last year']["id"]
    top += top_data['senders.last month']["id"]
    # remove duplicates
    people = list(set(top))
    createJSON(people, destdir+"/mls-people.json")

    for upeople_id in people:
        evol = MLS.GetEvolPeopleMLS(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol)
        createJSON(evol, destdir+"/people-"+str(upeople_id)+"-mls-evolutionary.json")

        static = MLS.GetStaticPeopleMLS(upeople_id, startdate, enddate)
        createJSON(static, destdir+"/people-"+str(upeople_id)+"-mls-static.json")


def reposData(period, startdate, enddate, identities_db, destdir, conf, repofield, npeople):
    repos = MLS.reposNames(rfield, startdate, enddate)
    createJSON (repos, destdir+"/mls-lists.json")
    # repos = repos['mailing_list_url']
    check = True
    if not isinstance(repos, (list)):
        repos = [repos]
    repos_files = [repo.replace('/', '_').replace("<","__").replace(">","___")
            for repo in repos]
    createJSON(repos_files, destdir+"/mls-repos.json")

    for repo in repos:
        # Evol data   
        repo_name = "'"+repo+"'"
        data = MLS.EvolMLSInfo(period, startdate, enddate, identities_db, rfield, ["repository", repo_name])
        data = completePeriodIds(data)
        listname_file = repo.replace("/","_").replace("<","__").replace(">","___")

        # TODO: Multilist approach. We will obsolete it in future
        createJSON (data, destdir+"/mls-"+listname_file+"-rep-evolutionary.json")
        # Multirepos filename
        createJSON (data, destdir+"/"+listname_file+"-mls-rep-evolutionary.json")

        top_senders = MLS.repoTopSenders (repo, identities_db, startdate, enddate, repofield, npeople)
        createJSON(top_senders, destdir+ "/"+listname_file+"-mls-rep-top-senders.json")

        # Static data
        data = MLS.StaticMLSInfo(period, startdate, enddate, identities_db, rfield, ["repository", repo_name])
        # TODO: Multilist approach. We will obsolete it in future
        createJSON (data, destdir+"/"+listname_file+"-rep-static.json")
        # Multirepos filename
        createJSON (data, destdir+ "/"+listname_file+"-mls-rep-static.json")

def companiesData(period, startdate, enddate, identities_db, destdir, npeople):
    companies = MLS.companiesNames(identities_db, startdate, enddate)
    createJSON(companies, destdir+"/mls-companies.json")

    for company in companies:
        company_name = "'"+company+ "'"
        data = MLS.EvolMLSInfo(period, startdate, enddate, identities_db, rfield, ["company", company_name])
        data = completePeriodIds(data)
        createJSON(data, destdir+"/"+company+"-mls-com-evolutionary.json")

        top_senders = MLS.companyTopSenders (company, identities_db, startdate, enddate, npeople)
        createJSON(top_senders, destdir+"/"+company+"-mls-com-top-senders.json")

        data = MLS.StaticMLSInfo(period, startdate, enddate, identities_db, rfield, ["company", company_name])
        createJSON(data, destdir+"/"+company+"-mls-com-static.json")

    sent = MLS.GetSentSummaryCompanies(period, startdate, enddate, opts.identities_db, 10)
    createJSON (sent, opts.destdir+"/mls-sent-companies-summary.json")

def countriesData(period, startdate, enddate, identities_db, destdir, npeople):

    countries = MLS.countriesNames(identities_db, startdate, enddate) 
    createJSON (countries, destdir + "/mls-countries.json")

    for country in countries:
        country_name = "'" + country + "'"
        type_analysis = ["country", country_name]
        data = MLS.EvolMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis)
        data = completePeriodIds(data)
        createJSON (data, destdir+"/"+country+"-mls-cou-evolutionary.json")

        top_senders = MLS.countryTopSenders (country, identities_db, startdate, enddate, npeople)
        createJSON(top_senders, destdir+"/"+country+"-mls-cou-top-senders.json")

        data = MLS.StaticMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis)
        createJSON (data, destdir+"/"+country+"-mls-cou-static.json")

def domainsData(period, startdate, enddate, identities_db, destdir, npeople):

    domains = MLS.domainsNames(identities_db, startdate, enddate)
    createJSON(domains, destdir+"/mls-domains.json")

    for domain in domains:
        domain_name = "'"+domain+"'"
        type_analysis = ["domain", domain_name]
        data = MLS.EvolMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis)
        data = completePeriodIds(data)
        createJSON(data, destdir+"/"+domain+"-mls-dom-evolutionary.json")

        data = MLS.domainTopSenders(domain, identities_db, startdate, enddate, npeople)
        createJSON(data, destdir+"/"+domain+"-mls-dom-top-senders.json")

        data = MLS.StaticMLSInfo(period, startdate, enddate, identities_db, rfield, type_analysis)
        createJSON(data, destdir+"/"+domain+"-mls-dom-static.json")


def getLongestThreads(startdate, enddate, identities_db):
    # This function builds a coherent data structure according
    # to other simila structures. The Class Threads only returns
    # the head of the threads (the first message) and the message_id
    # of each of its children.

    main_topics = Threads(startdate, enddate, identities_db)

    longest_threads = main_topics.topLongestThread(10)
    l_threads = {}
    l_threads['message_id'] = []
    l_threads['length'] = []
    l_threads['subject'] = []
    l_threads['date'] = []
    l_threads['initiator_name'] = []
    l_threads['initiator_id'] = []
    for email in longest_threads:
        l_threads['message_id'].append(email.message_id)
        l_threads['length'].append(main_topics.lenThread(email.message_id))
        l_threads['subject'].append(email.subject)
        l_threads['date'].append(email.date.strftime("%Y-%m-%d"))
        l_threads['initiator_name'].append(email.initiator_name)
        l_threads['initiator_id'].append(email.initiator_id)

    return l_threads



def topData(period, startdate, enddate, identities_db, destdir, bots, npeople):
    # List of top information of interest for the project.
    # So far providing information about top people sending messages
    # and longest threads for three periods: the whole history, last year and
    # last month.

    top_senders_data = {}
    top_senders_data['senders.']=MLS.top_senders(0, startdate, enddate,identities_db,bots, npeople)
    top_senders_data['senders.last year']=MLS.top_senders(365, startdate, enddate,identities_db, bots, npeople)
    top_senders_data['senders.last month']=MLS.top_senders(31, startdate, enddate,identities_db,bots, npeople)

    top_senders_data['threads.'] = getLongestThreads(startdate, enddate, identities_db)
    startdate = datetime.date.today() - datetime.timedelta(days=365)
    startdate =  "'" + str(startdate) + "'"
    top_senders_data['threads.last year'] = getLongestThreads(startdate, enddate, identities_db)
    startdate = datetime.date.today() - datetime.timedelta(days=30)
    startdate =  "'" + str(startdate) + "'"
    top_senders_data['threads.last month'] = getLongestThreads(startdate, enddate, identities_db)

    createJSON (top_senders_data, destdir+"/mls-top.json")

    return top_senders_data


def microstudies(vizr, enddate, destdir):

    unique_ids = True
    vizr.ReportDemographicsAgingMLS(enddate, destdir, unique_ids)
    vizr.ReportDemographicsBirthMLS(enddate, destdir, unique_ids)

    ## Which quantiles we're interested in
    quantiles_spec = [0.99,0.95,0.5,0.25]

    ## Yearly quantiles of time to attention (minutes)
    ## Monthly quantiles of time to attention (hours)
    ## JSON files generated from VizR
    vizr.ReportTimeToAttendMLS(destdir)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting MLS data source analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")

    # filtered bots
    bots = ['wikibugs','gerrit-wm','wikibugs_','wm-bot','','Translation updater bot','jenkins-bot']

    # Working at the same time with VizR and VizPy yet
    vizr.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)
    GrimoireSQL.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)


    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+opts.startdate+"'"
    enddate = "'"+opts.enddate+"'"
    # rfield = valRtoPython(vizr.reposField())[0]
    rfield = "mailing_list_url"

    tsData (period, startdate, enddate, opts.identities_db, opts.destdir, opts.granularity, opts)
    aggData(period, startdate, enddate, opts.identities_db, opts.destdir)

    top = topData(period, startdate, enddate, opts.identities_db, opts.destdir, bots, opts.npeople)
    if ('people' in reports):
        peopleData (period, startdate, enddate, opts.identities_db, opts.destdir, top)

    if ('repositories' in reports):
        reposData (period, startdate, enddate, opts.identities_db, opts.destdir, opts, rfield, opts.npeople)
    if ('countries' in reports):
        countriesData (period, startdate, enddate, opts.identities_db, opts.destdir, opts.npeople)
    if ('companies' in reports):
        companiesData (period, startdate, enddate, opts.identities_db, opts.destdir, opts.npeople)
    if ('domains' in reports):
        domainsData (period, startdate, enddate, opts.identities_db, opts.destdir, opts.npeople)


    # R specific reports
    microstudies(vizr, opts.enddate, opts.destdir)

    logging.info("MLS data source analysis OK")
