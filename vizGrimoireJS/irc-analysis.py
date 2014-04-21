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
#     PYTHONPATH=../vizgrimoire LANG= R_LIBS=../../r-lib ./irc-analysis.py 
#                                                -d acs_irc_automatortest_2388_2 -u root 
#                                                -i acs_cvsanaly_automatortest_2388 
#                                                -s 2010-01-01 -e 2014-01-20 
#                                                -o ../../../json -r people,repositories
#
# For migrating to Python3: z = dict(list(x.items()) + list(y.items()))



import logging
import sys

import GrimoireUtils, GrimoireSQL
from GrimoireUtils import dataFrame2Dict, createJSON, completePeriodIds
from GrimoireUtils import valRtoPython, read_options, getPeriod
import IRC

def aggData(period, startdate, enddate, idb, destdir):
    agg_data = {}

    # Tendencies
    for i in [7,30,365]:
        period_data = IRC.GetIRCDiffSentDays(period, enddate, i)
        agg_data = dict(agg_data.items() + period_data.items())
        period_data = IRC.GetIRCDiffSendersDays(period, enddate, idb, i)
        agg_data = dict(agg_data.items() + period_data.items())

    # Global aggregated data
    static_data = IRC.GetStaticDataIRC(period, startdate, enddate, idb, None)
    agg_data = dict(agg_data.items() + static_data.items())

    createJSON (agg_data, destdir+"/irc-static.json")

def tsData(period, startdate, enddate, idb, destdir):
    ts_data = {}
    ts_data = IRC.GetEvolDataIRC(period, startdate, enddate, idb, None)
    ts_data = completePeriodIds(ts_data, period, startdate, enddate)
    createJSON (ts_data, destdir+"/irc-evolutionary.json")

def peopleData(period, startdate, enddate, idb, destdir, top_data):
    top = top_data['senders.']["id"]
    top += top_data['senders.last year']["id"]
    top += top_data['senders.last month']["id"]
    # remove duplicates
    people = list(set(top)) 
    createJSON(people, destdir+"/irc-people.json")

    for upeople_id in people:
        # evol = dataFrame2Dict(vizr.GetEvolPeopleIRC(upeople_id, period, startdate, enddate))
        evol = IRC.GetEvolPeopleIRC(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        person_file = destdir+"/people-"+str(upeople_id)+"-irc-evolutionary.json"
        createJSON(evol, person_file)

        person_file = destdir+"/people-"+str(upeople_id)+"-irc-static.json"
        # aggdata = dataFrame2Dict(vizr.GetStaticPeopleIRC(upeople_id, startdate, enddate))
        aggdata = IRC.GetStaticPeopleIRC(upeople_id, startdate, enddate)
        createJSON(aggdata, person_file)

# TODO: pretty similar to peopleData. Unify?
def reposData(period, startdate, enddate, idb, destdir):
    repos = IRC.GetReposNameIRC()
    repos_file = destdir+"/irc-repos.json"
    createJSON(repos, repos_file)

    for repo in repos:
        evol = IRC.GetRepoEvolSentSendersIRC(repo, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        repo_file = destdir+"/"+repo+"-irc-rep-evolutionary.json"
        createJSON(evol, repo_file)

        repo_file = destdir+"/"+repo+"-irc-rep-static.json"
        aggdata = IRC.GetRepoStaticSentSendersIRC(repo, startdate, enddate)
        createJSON(aggdata, repo_file)

def topData(period, startdate, enddate, idb, destdir, bots, npeople):
    top_senders = {}
    top_senders['senders.'] = \
        IRC.GetTopSendersIRC(0, startdate, enddate, idb, bots, npeople)
    top_senders['senders.last year'] = \
        IRC.GetTopSendersIRC(365, startdate, enddate, idb, bots, npeople)
    top_senders['senders.last month'] = \
        IRC.GetTopSendersIRC(31, startdate, enddate, idb, bots, npeople)
    top_file = destdir+"/irc-top.json"
    createJSON (top_senders, top_file)

    return(top_senders)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting IRC data source analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")
    # filtered bots
    bots = ['wikibugs','gerrit-wm','wikibugs_','wm-bot','']
        # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+opts.startdate+"'"
    enddate = "'"+opts.enddate+"'"

    GrimoireSQL.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)

    aggData (period, startdate, enddate, opts.identities_db, opts.destdir)
    tsData (period, startdate, enddate, opts.identities_db, opts.destdir)
    top = topData(period, startdate, enddate, opts.identities_db, opts.destdir, bots, opts.npeople)
    if ('people' in reports):
        peopleData (period, startdate, enddate, opts.identities_db, opts.destdir, top)
    if ('repositories' in reports):
        reposData (period, startdate, enddate, opts.identities_db, opts.destdir)

    logging.info("Done IRC data source analysis")
