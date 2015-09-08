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
## python eclipse_mm.py -a cp_cvsanaly_PolarsysMaturity -d cp_gerrit_PolarsysMaturity -i cp_cvsanaly_PolarsysMaturity -c cp_bicho_PolarsysMaturity -b cp_mlstats_PolarsysMaturity -e cp_mlstats_fudforums 


import imp, inspect
from optparse import OptionParser
from os import listdir, path, environ
from os.path import isfile, join
import sys
import json

import locale
import numpy as np
import datetime

PERIOD = 90 # Number of days for the analysis

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
    parser.add_option("-e", "--dbfudforums",
                      action="store",
                      dest="dbfudforums",
                      help="FudForums database")
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


def scm_report(dbcon, filters, sloc):

    ksloc = float(sloc) / 1000.0

    # Name: SCM Commits
    # Mnemo: SCM_COMMITS_1M
    # Description: Total number of commits registered on the artefact during 
    # the last month
    commits = scm.Commits(dbcon, filters)
    # NOTE: Commits ignore merges, are for all of the branches,
    # ignore specified bots, count unique revisions
    scm_commits_3m = commits.get_trends(filters.enddate, PERIOD)["commits_%d" % PERIOD]

    # Name: Files committed
    # Mnemo: SCM_COMMITTED_FILES_1M
    # Description: Total number of files found in all commits that happened 
    # during the last month.
    files = scm.Files(dbcon, filters)
    # NOTE: In addition to commits conditions, a committed file is defined as a 
    # 'touched' file: added, removed, modified, copied, moved, etc.
    scm_committed_files_3m = files.get_trends(filters.enddate, PERIOD)["files_%d" % PERIOD]

    # Name: SCM committers
    # Mnemo: SCM_COMMITTERS_1M
    # Description: Total number of different user logins found in commits 
    # during last month.
    authors = scm.Authors(dbcon, filters)
    # NOTE: We should probably calculate authors instead of committers.
    # In addition: SCM_COMMITTERS and SCM_AUTHORS point to the same metric
    scm_committers_3m = authors.get_trends(filters.enddate, PERIOD)["authors_%d" % PERIOD]

    # Name: File Statibility Index
    # Mnemo: SCM_Stability_1M
    # Description: Average number of commits touching each file in source 
    #              code management repositories dated during the last month.
    value = filters.type_analysis[1].split(",")[0]
    query = """ select a.file_id,
                       count(distinct(a.id)) as pokes
                from actions a,
                     scmlog s,
                     repositories r,
                     projects pr
                where a.commit_id = s.id and
                      s.author_date >=%s and
                      s.repository_id = r.id and
                      r.name = pr.title and
                      pr.id = %s
                      group by a.file_id """ % (filters.startdate, value)
    file_pokes =  dbcon.ExecuteQuery(query)
    file_pokes = file_pokes["pokes"]
    avg_file_pokes = "nan"
    if isinstance(file_pokes, list) and len(file_pokes) > 0:
        avg_file_pokes = DHESA(file_pokes)
        avg_file_pokes = avg_file_pokes.data["mean"]

    ksloc = float(sloc) / 1000.0


    dataset = {}
    dataset["scm_stability_3m"] = avg_file_pokes

    dataset["scm_commits_3m"] = scm_commits_3m
    dataset["scm_committed_files_3m"] = scm_committed_files_3m
    dataset["scm_committers_3m"] = scm_committers_3m

    return dataset

def its_report(dbcon, filters, sloc):

    ksloc = float(sloc) / 1000.0

    # Name: ITS updates
    # Mnemo: ITS_UPDATES_1M
    # Description: The number of updates to the issue tracking system during 
    # last month.
    changes = its.Changed(dbcon, filters)
    # NOTE: this metric is defined as CPI and to not be retrieved by Grimoire
    # We should probably retrieve it.
    # updates = opened issues + changes in the period.
    its_updates_3m = changes.get_trends(filters.enddate, PERIOD)["changed_%d" % PERIOD]
    opened = its.Opened(dbcon, filters)
    its_updates_3m += opened.get_agg()["opened"]

    # ITS BUGS OPEN
    query = """ select count(distinct(i.id)) as opened_issues
                from issues i, trackers t
                where (i.status<>'CLOSED' and i.status<>'RESOLVED') and
                i.tracker_id = t.id and
                t.url IN (
                          SELECT repository_name
                          FROM %s.projects p, %s.project_repositories pr
                          WHERE p.project_id = pr.project_id AND pr.data_source='its'
                                and p.id=%s) ;
            """ % (dbcon.identities_db, dbcon.identities_db, filters.type_analysis[1])

    its_bugs_open = dbcon.ExecuteQuery(query)["opened_issues"]

    # Name: ITS authors
    # Mnemo: ITS_AUTH_1M
    # Description: The number of different authors (according to their login) 
    # have contributed to the issue tracking system during last month.
    authors = its.Changers(dbcon, filters)
    # NOTE: this metric is not the correct one, we should probably add people opening
    # and closing issues, and not only those participating in the changes part
    its_auth_3m = authors.get_trends(filters.enddate, PERIOD)["changers_%d" % PERIOD]

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
    if not isinstance(timeto_list["timeto"], list):
        timeto_list["timeto"] = [int(timeto_list["timeto"])]
    dhesa = DHESA(timeto_list["timeto"])
    its_fix_med_3m = dhesa.data["median"]
    its_fix_med_3m = its_fix_med_3m / 3600.0
    its_fix_med_3m = round(its_fix_med_3m / 24.0, 2)


    # Name: Defect density
    # Mnemo: ITS_BUGS_DENSITY
    # Description: Overall total number of bugs on product divided by 1000's 
    # of lines (KLOC).
    # TBD
    project_name = filters.type_analysis[1].replace("'", "")
    startdate = filters.startdate
    enddate = filters.enddate
    filters.startdate = "'1900-01-01'"
    filters.enddate = "'2100-01-01'"
    opened = its.Opened(dbcon, filters)
    its_bugs_density = float(opened.get_agg()["opened"]) / ksloc
    filters.startdate = startdate
    filters.enddate = enddate

    dataset = {}
    dataset["its_updates_3m"] = its_updates_3m
    dataset["its_auth_3m"] = its_auth_3m
    dataset["its_bugs_density"] = its_bugs_density
    dataset["its_fix_med_3m"] = its_fix_med_3m
    dataset["its_bugs_open"] = its_bugs_open

    return dataset

def scr_report(dbcon, filters):
    pass

def repositories(dbcon, filters):
    # Checks if there are repositories for the specific project
    # at filters.type_analysis.

    tables = dbcon.GetSQLProjectsFrom()
    filters = dbcon.GetSQLProjectsWhere(filters.type_analysis[1])

    fields = "count(*) as nrepositories"

    query = "select %s from %s, messages m where %s and %s" % (fields, tables.pop(), filters.pop(), filters.pop())

    data = dbcon.ExecuteQuery(query)

    return data["nrepositories"]

def mls_report(dbcon, filters, sloc):

    ksloc = float(sloc) / 1000.0

    if int(repositories(dbcon, filters)) <= 0:
        dataset = {}
        dataset["mls_dev_vol_3m"] = "null"
        dataset["mls_dev_subj_3m"] = "null"
        dataset["mls_dev_auth_3m"] = "null"
        dataset["mls_dev_resp_ratio_3m"] = "null"
        dataset["mls_dev_resp_time_med_3m"] = "null"

        return dataset


    # MLS project info is not supported yet. Thus, instead of using the 
    # "project" type of analysis, the "repository" type of analysis is used.
    #filters.type_analysis = ["repository", "'/mnt/mailman_archives/cdt-dev.mbox'"]

    # Name:Developer ML posts
    # Mnemo: MLS_DEV_VOL_1M
    # Description: Total number of posts on the developer mailing list during 
    # the last month.
    emails = mls.EmailsSent(dbcon, filters)
    # NOTE: We need to identify the specific developer mailing list
    mls_dev_vol_3m = emails.get_trends(filters.enddate, PERIOD)["sent_%d" % PERIOD]

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
    timeto = mls.TimeToFirstReply(dbcon, filters)
    timeto_list = timeto.get_agg()
    if not isinstance(timeto_list, list):
        timeto_list = [int(timeto_list)]

    mls_dev_resp_time_med_3m = "nan"
    if len(timeto_list) > 0:
        dhesa = DHESA(timeto_list)
        mls_dev_resp_time_med_3m = dhesa.data["median"]
        mls_dev_resp_time_med_3m = mls_dev_resp_time_med_3m / 3600.0
        mls_dev_resp_time_med_3m = round(mls_dev_resp_time_med_3m / 24.0, 2)


    # Name: Developer ML subjects
    # Mnemo: MLS_DEV_SUBJ_1M
    # Description: Number of threads in the developer mailing list 
    # during last month
    activethreads = mls.ActiveThreads(dbcon, filters)
    mls_dev_subj_3m = activethreads.get_agg()

    #threads = mls.Threads(dbcon, filters)
    #print threads._get_sql(False)
    #mls_dev_subj_3m = threads.get_trends(filters.enddate, PERIOD)["threads_%d" % PERIOD]

    # Name:
    # Mnemo: MLS_DEV_AUTH_1M
    # Description: Number of authors in the developers mailing list during the last month
    authors = mls.EmailsSenders(dbcon, filters)
    mls_dev_auth_3m = authors.get_trends(filters.enddate, PERIOD)["senders_%d" % PERIOD]


    # Name: Developer ML response ratio
    # Mnemo: MLS_DEV_RESP_RATIO_1M
    # Description: Average number of responses for a question in the developer 
    # mailing list during last month.
    emails = mls.EmailsSentResponse(dbcon, filters)
    mls_dev_resp_ratio_3m = emails.get_trends(filters.enddate, PERIOD)["sent_response_%d" % PERIOD]

    if mls_dev_subj_3m > 0:
        mls_dev_resp_ratio_3m = float(mls_dev_resp_ratio_3m) / float(mls_dev_subj_3m)
    else:
        mls_dev_resp_ratio_3m = "nan"

    dataset = {}
    dataset["mls_dev_vol_3m"] = mls_dev_vol_3m
    dataset["mls_dev_subj_3m"] = mls_dev_subj_3m
    dataset["mls_dev_auth_3m"] = mls_dev_auth_3m
    dataset["mls_dev_resp_ratio_3m"] = mls_dev_resp_ratio_3m
    dataset["mls_dev_resp_time_med_3m"] = mls_dev_resp_time_med_3m

    return dataset


if __name__ == '__main__':

    locale.setlocale(locale.LC_ALL, 'en_US.utf8')

    from vizgrimoire.metrics.metrics import Metrics
    from vizgrimoire.metrics.query_builder import DSQuery, SCMQuery, MLSQuery, SCRQuery, ITSQuery
    from vizgrimoire.metrics.metrics_filter import MetricFilters
    import vizgrimoire.metrics.scm_metrics as scm
    import vizgrimoire.metrics.mls_metrics as mls
    import vizgrimoire.metrics.scr_metrics as scr
    import vizgrimoire.metrics.its_metrics as its
    from vizgrimoire.GrimoireUtils import createJSON
    from vizgrimoire.GrimoireSQL import SetDBChannel
    from rpy2.robjects.packages import importr
    from vizgrimoire.ITS import ITS
    from vizgrimoire.datahandlers.data_handler import DHESA

    # parse options
    opts = read_options()

    today = datetime.date.today()
    startdate = today - datetime.timedelta(days=PERIOD)
    startdate = "'" + startdate.strftime("%Y-%m-%d") + "'"
    enddate = "'" + today.strftime("%Y-%m-%d") + "'"

    # Projects analysis. This includes SCM, SCR and ITS.
    people_out = []
    affs_out = ["-Bot","-Individual","-Unknown"]

    # Lines of code per repository
    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)

    data = {}

    # List of projects:
    query = "select project_id, id from projects"
    projects = scm_dbcon.ExecuteQuery(query)

    for project in projects["id"]:

        try:
            #Obtaining ncloc metric from Sonar dataset
            with open("../../../json/" + project + "-metrics-sonarqube.json") as sonar_data:
                sonar_metrics = json.load(sonar_data)
        except:
            continue

        project_sloc = float(sonar_metrics["ncloc"])
        print "Project: " + str(project)
        print "NCLOC: " + str(project_sloc)

        filters = MetricFilters("month", startdate, enddate, ["project", "'"+project+"'"], opts.npeople,
                             people_out, affs_out)

        #SCM report
        scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
        data.update(scm_report(scm_dbcon, filters, project_sloc))

        #ITS report
        ITS.set_backend("bg")
        its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
        data.update(its_report(its_dbcon, filters, project_sloc))

        #SCR Report
        #scr_dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities)
        #data["scr"] = scr_report(scr_dbcon, filters)

        #MLS Report
        mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities)
        data.update(mls_report(mls_dbcon, filters, project_sloc))
        #FUDFORUMS Report
        fudforums_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbfudforums, opts.dbidentities)
        dataset = mls_report(fudforums_dbcon, filters, project_sloc)

        data_aux = {}
        data_aux["mls_usr_vol_3m"] = dataset["mls_dev_vol_3m"]
        data_aux["mls_usr_subj_3m"] = dataset["mls_dev_subj_3m"]
        data_aux["mls_usr_auth_3m"] = dataset["mls_dev_auth_3m"]
        data_aux["mls_usr_resp_ratio_3m"] = dataset["mls_dev_resp_ratio_3m"]
        data_aux["mls_usr_resp_time_med_3m"] = dataset["mls_dev_resp_time_med_3m"]
        data.update(data_aux)


        createJSON(data, "../../../json/" + project + "-metrics-grimoirelib.json")


