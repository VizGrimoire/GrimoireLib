#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##   Luis Cañas-Díaz <lcanas@bitergia.com>
##
## python openstack_report.py -a dic_cvsanaly_openstack_2259 -d dic_bicho_openstack_gerrit_3392 -i dic_cvsanaly_openstack_2259 -r 2013-07-01,2013-10-01,2014-01-01,2014-04-01,2014-07-01 -c lcanas_bicho_openstack_1376

import imp, inspect
from optparse import OptionParser
from os import listdir, path, environ
from os.path import isfile, join
import sys

import locale
import matplotlib as mpl
# This avoids the use of the $DISPLAY value for the charts
mpl.use('Agg')
import matplotlib.pyplot as plt
import prettyplotlib as ppl
from prettyplotlib import brewer2mpl
import numpy as np
from datetime import datetime


def ts_chart(title, unixtime_dates, data, file_name):

    fig = plt.figure()
    plt.title(title)

    dates = []
    for unixdate in unixtime_dates:
        dates.append(datetime.fromtimestamp(float(unixdate)))

    ppl.plot(dates, data)
    fig.autofmt_xdate()
    fig.savefig(file_name + ".eps")


def barh_chart(title, yvalues, xvalues, file_name):

    fig, ax = plt.subplots(1)
    x_pos = np.arange(len(xvalues))

    plt.title(title)
    y_pos = np.arange(len(yvalues))

    #plt.barh(y_pos, xvalues)
    ppl.barh(y_pos, xvalues, grid='x')
    plt.yticks(y_pos, yvalues)
    plt.savefig(file_name + ".eps")



def read_options():

    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 0.1")

    parser.add_option("-a", "--dbcvsanaly",
                      action="store",
                      dest="dbcvsanaly",
                      help="CVSAnalY db where information is stored")
    parser.add_option("-b", "--dbmlstats",
                      action="store",
                      dest="dbmlstats",
                      help="Mailing List Stats db where information is stored")
    parser.add_option("-c", "--dbbicho",
                      action="store",
                      dest="dbbicho",
                      help="Bicho db where information is stored")
    parser.add_option("-d", "--dbreview",
                      action="store",
                      dest="dbreview",
                      help="Review db where information is stored")
    parser.add_option("-e", "--dbirc",
                      action="store",
                      dest="dbirc",
                      help="IRC where information is stored")
    parser.add_option("-f", "--dbqaforums",
                      action="store",
                      dest="dbqaforums",
                      help="QAForums where information is stored")
    parser.add_option("-i", "--identities",
                      action="store",
                      dest="dbidentities",
                      help="Database with unique identities and affiliations")

    parser.add_option("-u","--dbuser",
                      action="store",
                      dest="dbuser",
                      default="root",
                      help="Database user")
    parser.add_option("-p","--dbpassword",
                      action="store",
                      dest="dbpassword",
                      default="",
                      help="Database password")

    parser.add_option("-r", "--releases",
                      action="store",
                      dest="releases",
                      default="2010-01-01,2011-01-01,2012-01-01",
                      help="Releases for the report")
    parser.add_option("-t", "--type",
                      action="store",
                      dest="backend",
                      default="bugzilla",
                      help="Type of backend: bugzilla, allura, jira, github")
    parser.add_option("-g", "--granularity",
                      action="store",
                      dest="granularity",
                      default="months",
                      help="year,months,weeks granularity")
    parser.add_option("--npeople",
                      action="store",
                      dest="npeople",
                      default="10",
                      help="Limit for people analysis")
    # TBD
    #parser.add_option("--list-metrics",
    #                  help="List available metrics")

    (opts, args) = parser.parse_args()

    return opts


def build_releases(releases_dates):
    # Builds a list of tuples of dates that limit
    # each of the timeperiods to analyze

    releases = []
    dates = releases_dates.split(",")
    init = dates[0]
    for date in dates:
        if init <> date:
            releases.append((init, date))
            init = date

    return releases


def scm_report(dbcon, filters):
    commits = scm.Commits(dbcon, filters)
    createJSON(commits.get_agg(), "./release/scm_commits.json")

    authors = scm.Authors(dbcon, filters)
    createJSON(authors.get_agg(), "./release/scm_authors.json")

    dataset = {}
    dataset["commits"] = commits.get_agg()["commits"]
    dataset["authors"] = authors.get_agg()["authors"]

    return dataset

def its_report(dbcon, filters):
    from ITS import ITS
    ITS.set_backend("launchpad")
    opened = its.Opened(dbcon, filters)
    createJSON(opened.get_agg(), "./release/its_opened.json")

    closed = its.Closed(dbcon, filters)
    createJSON(closed.get_agg(), "./release/its_closed.json")

    dataset = {}
    dataset["opened"] = opened.get_agg()["opened"]
    dataset["closed"] = closed.get_agg()["closed"]

    return dataset


def scr_report(dbcon, filters):
    submitted = scr.Submitted(dbcon, filters)
    createJSON(submitted.get_agg(), "./release/scr_submitted.json")

    merged = scr.Merged(dbcon, filters)
    createJSON(merged.get_agg(), "./release/scr_merged.json")

    abandoned = scr.Abandoned(dbcon, filters)
    createJSON(abandoned.get_agg(), "./release/scr_abandoned.json")

    waiting4reviewer = scr.ReviewsWaitingForReviewer(dbcon, filters)
    createJSON(waiting4reviewer.get_agg(), "./release/scr_waiting4reviewer.json")

    waiting4submitter = scr.ReviewsWaitingForSubmitter(dbcon, filters)
    createJSON(waiting4submitter.get_agg(), "./release/scr_waiting4submitter.json")

    dataset = {}
    dataset["submitted"] = submitted.get_agg()["submitted"]
    dataset["merged"] = merged.get_agg()["merged"]
    dataset["abandoned"] = abandoned.get_agg()["abandoned"]
    dataset["waiting4reviewer"] = waiting4reviewer.get_agg()["ReviewsWaitingForReviewer"]
    dataset["waiting4submitter"] = waiting4submitter.get_agg()["ReviewsWaitingForSubmitter"]

    return dataset

# Until we use VizPy we will create JSON python files with _py
def createCSV(data, filepath, skip_fields = []):
    fd = open(filepath, "w")
    keys = list(set(data.keys()) - set(skip_fields))
    
    header = u''
    for k in keys:
        header += unicode(k)
        header += u','        
    header = header[:-1]
    body = ''
    length = len(data[keys[0]]) # the length should be the same for all
    cont = 0
    while (cont < length):
        for k in keys:
            try:
                body += unicode(data[k][cont])
            except UnicodeDecodeError:
                body += u'ERROR'
            body += u','
        body = body[:-1]
        body += u'\n'
        cont += 1
    fd.write(header.encode('utf-8'))
    fd.write('\n')
    fd.write(body.encode('utf-8'))
    fd.close()
    print "CSV file generated at: %s" % (filepath)

def init_env():
    grimoirelib = path.join("..","vizgrimoire")
    metricslib = path.join("..","vizgrimoire","metrics")
    studieslib = path.join("..","vizgrimoire","analysis")
    alchemy = path.join("..","grimoirelib_alch")
    for dir in [grimoirelib,metricslib,studieslib,alchemy]:
        sys.path.append(dir)

    # env vars for R
    environ["LANG"] = ""
    environ["R_LIBS"] = "../../r-lib"


def projects(user, password, database):
    dbcon = DSQuery(user, password, database, None)
    query = "select id from projects"
    return dbcon.ExecuteQuery(query)["id"]

if __name__ == '__main__':

    locale.setlocale(locale.LC_ALL, 'en_US')

    init_env()

    from metrics import Metrics
    from query_builder import DSQuery, SCMQuery, QAForumsQuery, MLSQuery, SCRQuery, ITSQuery
    from metrics_filter import MetricFilters
    import scm_metrics as scm
    import qaforums_metrics as qa
    import mls_metrics as mls
    import scr_metrics as scr
    import its_metrics as its
    from GrimoireUtils import createJSON
    from GrimoireSQL import SetDBChannel

    # parse options
    opts = read_options()    

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = build_releases(opts.releases)


    projects_list = projects(opts.dbuser, opts.dbpassword, opts.dbidentities)
    print projects_list

    for project in projects_list:
        releases_data = {}
        for release in releases:
            releases_data[release] = {}

            startdate = "'" + release[0] + "'"
            enddate = "'" + release[1] + "'"
            filters = MetricFilters("month", startdate, enddate, ["project", str(project)], opts.npeople)
            scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
            #SCM report
            dataset = scm_report(scm_dbcon, filters)
            releases_data[release]["scm"] = dataset

            #ITS report
            its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
            dataset = its_report(its_dbcon, filters)
            releases_data[release]["its"] = dataset

            #MLS Report
            scr_dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities)
            dataset = scr_report(scr_dbcon, filters)
            releases_data[release]["scr"] = dataset


        labels = []
        commits = []
        authors = []
        opened = []
        submitted = []
        merged = []
        abandoned = []
        closed = []
        for release in releases:
            labels.append(release[1])
            #scm
            commits.append(releases_data[release]["scm"]["commits"])
            authors.append(releases_data[release]["scm"]["authors"])
            #its
            opened.append(releases_data[release]["its"]["opened"])
            closed.append(releases_data[release]["its"]["closed"])
            #scr
            submitted.append(releases_data[release]["scr"]["submitted"])
            merged.append(releases_data[release]["scr"]["merged"])
            abandoned.append(releases_data[release]["scr"]["abandoned"])
        
        

        barh_chart("Commits " + project, labels, commits, "commits"  + project)
        barh_chart("Authors " + project, labels, commits, "authors" + project)
        barh_chart("Opened tickets " +  project, labels, opened, "opened" + project)
        barh_chart("Closed tickets " + project, labels, closed, "closed" + project)
        barh_chart("Submitted reviews " + project, labels, submitted, "submitted_reviews" + project)
        barh_chart("Merged reviews " + project, labels, merged, "merged_reviews" + project)
        barh_chart("Abandoned reviews  " + project, labels, abandoned, "abandoned_reviews" + project)



