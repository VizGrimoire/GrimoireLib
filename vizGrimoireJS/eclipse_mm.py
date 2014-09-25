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
##
## python eclipse_mm.py -a cp_cvsanaly_Eclipse_3328 -d cp_gerrit_Eclipse_3328 -i cp_cvsanaly_Eclipse_3328 -r 2014-04-01,2014-07-01 -c cp_bicho_Eclipse_3328 -b cp_mlstats_Eclipse_4134


import imp, inspect
from optparse import OptionParser
from os import listdir, path, environ
from os.path import isfile, join
import sys

import locale
import numpy as np
from datetime import datetime

PROJECT = "tools.cdt"
NLOC = 914354

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
    parser.add_option("-r", "--dates",
                      action="store",
                      dest="dates",
                      default="root",
                      help="Initial date, final date <date1,date2>")
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


def scm_report(dbcon, filters):

    # Name: SCM Commits
    # Mnemo: SCM_COMMITS_1M
    # Description: Total number of commits registered on the artefact during 
    # the last month
    commits = scm.Commits(dbcon, filters)
    # NOTE: Commits ignore merges, are for all of the branches,
    # ignore specified bots, count unique revisions
    scm_commits_1m = commits.get_trends(filters.enddate, 30)["commits_30"]

    # Name: Files committed
    # Mnemo: SCM_COMMITTED_FILES_1M
    # Description: Total number of files found in all commits that happened 
    # during the last month.
    files = scm.Files(dbcon, filters)
    # NOTE: In addition to commits conditions, a committed file is defined as a 
    # 'touched' file: added, removed, modified, copied, moved, etc.
    scm_committed_files_1m = files.get_trends(filters.enddate, 30)["files_30"]

    # Name: SCM committers
    # Mnemo: SCM_COMMITTERS_1M
    # Description: Total number of different user logins found in commits 
    # during last month.
    authors = scm.Authors(dbcon, filters)
    # NOTE: We should probably calculate authors instead of committers.
    # In addition: SCM_COMMITTERS and SCM_AUTHORS point to the same metric
    scm_committers_1m = authors.get_trends(filters.enddate, 30)["authors_30"]

    dataset = {}
    dataset["scm_commits_1m"] = scm_commits_1m
    dataset["scm_committed_files_1m"] = scm_committed_files_1m
    dataset["scm_committers_1m"] = scm_committers_1m
    
    return dataset
    
def its_report(dbcon, filters):

    # Name: ITS updates
    # Mnemo: ITS_UPDATES_1M
    # Description: The number of updates to the issue tracking system during 
    # last month.
    changes = its.Changed(dbcon, filters)
    # NOTE: this metric is defined as CPI and to not be retrieved by Grimoire
    # We should probably retrieve it.
    # updates = opened issues + changes in the period.
    its_updates_1m = changes.get_trends(filters.enddate, 30)["changed_30"]
    opened = its.Opened(dbcon, filters)
    its_updates_1m += opened.get_agg()["opened"]

    # Name: ITS authors
    # Mnemo: ITS_AUTH_1M
    # Description: The number of different authors (according to their login) 
    # have contributed to the issue tracking system during last month.
    authors = its.Changers(dbcon, filters)
    # NOTE: this metric is not the correct one, we should probably add people opening
    # and closing issues, and not only those participating in the changes part
    its_auth_1m = authors.get_trends(filters.enddate, 30)["changers_30"]

    # Name: Median time to fix bug
    # Mnemo: ITS_FIX_MED_1M
    # Description: Median time between the creation of an bug and its closure. 
    # Bugs are identified according to the registered type of issue.
    # NOTE: this may be calculated through the review system
    # TODO
    #vizr = importr("vizgrimoire")
    #vizr.SetDBChannel(database=dbcon.database, user=dbcon.user, password=dbcon.password)
    #vizr.ReportTimeToCloseITS("bugzilla", "./")
    timeto = its.TimeToClose(dbcon, filters)
    timeto_list = timeto.get_agg()
    dhesa = DHESA(timeto_list["timeto"])
    its_fix_med_1m = dhesa.data["median"]
    its_fix_med_1m = its_fix_med_1m / 3600.0
    its_fix_med_1m = round(its_fix_med_1m / 24.0, 2)


    # Name: Defect density
    # Mnemo: ITS_BUGS_DENSITY
    # Description: Overall total number of bugs on product divided by 1000's 
    # of lines (KLOC).
    # TBD
    opened = its.Opened(dbcon, filters)
    kloc = float(NLOC) / 1000.0
    its_bugs_density = float(opened.get_agg()["opened"]) / kloc

    dataset = {}
    dataset["its_updates_1m"] = its_updates_1m
    dataset["its_auth_1m"] = its_auth_1m
    dataset["its_bugs_density"] = its_bugs_density
    dataset["its_fix_med_1m"] = its_fix_med_1m
   
    return dataset 

def scr_report(dbcon, filters):
    pass

def mls_report(dbcon, filters):

    # MLS project info is not supported yet. Thus, instead of using the 
    # "project" type of analysis, the "repository" type of analysis is used.
    #filters.type_analysis = ["repository", "'/mnt/mailman_archives/cdt-dev.mbox'"]

    # Name:Developer ML posts
    # Mnemo: MLS_DEV_VOL_1M
    # Description: Total number of posts on the developer mailing list during 
    # the last month.
    emails = mls.EmailsSent(dbcon, filters)
    # NOTE: We need to identify the specific developer mailing list
    mls_dev_vol_1m = emails.get_trends(filters.enddate, 30)["sent_30"]

    # Name: User ML posts
    # Mnemo: MLS_USR_VOL_1M
    # Description: Total number of posts on the user mailing list during the last month.
    # NOTE: this value is focused on the user mailing list. However, the metric is
    # named as the developers one. Not user lists were found.
    # TODO

    # Name: User ML response time
    # Mnemo: MLS_USR_RESP_TIME_MED_1M
    # Description: Median time of first reply to questions in the user mailing list during last week.
    # This metric is still implemented in R
    # TODO: implement quantiles in Python to analyze this
    #vizr = importr("vizgrimoire")
    #vizr.SetDBChannel(database=dbcon.database, user=dbcon.user, password=dbcon.password)
    #vizr.ReportTimeToAttendMLS("./")

    # Name: Developer ML subjects
    # Mnemo: MLS_DEV_SUBJ_1M
    # Description: Number of threads in the developer mailing list 
    # during last month
    threads = mls.Threads(dbcon, filters)
    mls_dev_subj_1m = threads.get_trends(filters.enddate, 30)["threads_30"]
     
    # Name: Developer ML response ratio
    # Mnemo: MLS_DEV_RESP_RATIO_1M
    # Description: Average number of responses for a question in the developer 
    # mailing list during last month.
    emails = mls.EmailsSentResponse(dbcon, filters)
    mls_dev_resp_ratio_1m = emails.get_trends(filters.enddate, 30)["sent_response_30"]

    if mls_dev_subj_1m > 0:
        mls_dev_resp_ratio_1m = float(mls_dev_resp_ratio_1m) / float(mls_dev_subj_1m)
    else:
        mls_dev_resp_ratio_1m = 0

    dataset = {}
    dataset["mls_dev_vol_1m"] = mls_dev_vol_1m
    dataset["mls_dev_subj_1m"] = mls_dev_subj_1m
    dataset["mls_dev_resp_ratio_1m"] = mls_dev_resp_ratio_1m

    return dataset


def init_env():
    grimoirelib = path.join("..","vizgrimoire")
    metricslib = path.join("..","vizgrimoire","metrics")
    studieslib = path.join("..","vizgrimoire","analysis")
    datahandler = path.join("..","vizgrimoire","datahandlers")
    alchemy = path.join("..")
    for dir in [grimoirelib,metricslib,studieslib,alchemy, datahandler]:
        sys.path.append(dir)

    # env vars for R
    environ["LANG"] = ""
    environ["R_LIBS"] = "../r-lib"


if __name__ == '__main__':

    locale.setlocale(locale.LC_ALL, 'en_US.utf8')

    init_env()

    from metrics import Metrics
    from query_builder import DSQuery, SCMQuery, MLSQuery, SCRQuery, ITSQuery
    from metrics_filter import MetricFilters
    import scm_metrics as scm
    import mls_metrics as mls
    import scr_metrics as scr
    import its_metrics as its
    from GrimoireUtils import createJSON
    from GrimoireSQL import SetDBChannel
    from rpy2.robjects.packages import importr
    from ITS import ITS
    from data_handler import DHESA

    # parse options
    opts = read_options()    

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = opts.dates.split(",")
    startdate = "'" + releases[0] + "'"
    enddate = "'" + releases[1] + "'"

    # Projects analysis. This includes SCM, SCR and ITS.
    people_out = []
    affs_out = ["-Bot","-Individual","-Unknown"]


    data = {}

    filters = MetricFilters("month", startdate, enddate, ["project", "'"+PROJECT+"'"], opts.npeople,
                             people_out, affs_out)
    filters_scm = MetricFilters("month", startdate, enddate, ["project,branch", "'"+PROJECT+"','master'"], opts.npeople, people_out, affs_out)

    #SCM report
    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
    data.update(scm_report(scm_dbcon, filters_scm))

    #ITS report
    ITS.set_backend("bg")
    its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
    data.update(its_report(its_dbcon, filters))

    #SCR Report
    #scr_dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities)
    #data["scr"] = scr_report(scr_dbcon, filters)

    #MLS Report
    mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities)
    data.update(mls_report(mls_dbcon, filters))

    print data


