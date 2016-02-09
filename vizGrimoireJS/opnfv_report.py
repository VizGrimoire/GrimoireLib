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
## python opnfv_report.py -a opnfv_2016q1_git -d opnfv_2016q1_gerrit -i opnfv_2016q1_identities -r 2015-01-01,2015-04-01,2015-07-01,2015-10-01,2016-01-01 -c opnfv_2016q1_tickets -b opnfv_2016q1_mailing_lists -f opnfv_2016q1_qaforums -e opnfv_2016q1_irc


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
#from data_handler import DHESA


def bar3_chart(title, labels, data1, file_name, data2, data3, legend=["", ""]):

    colors = ["orange", "grey"]

    fig, ax = plt.subplots(1)
    xpos = np.arange(len(data1))
    width = 0.28

    plt.title(title)
    y_pos = np.arange(len(data1))

    ppl.bar(xpos+width+width, data3, color="orange", width=0.28, annotate=True)
    ppl.bar(xpos+width, data1, color='grey', width=0.28, annotate=True)
    ppl.bar(xpos, data2, grid='y', width = 0.28, annotate=True)
    plt.xticks(xpos+width, labels)
    plt.legend(legend, loc=2)


    plt.savefig(file_name + ".eps")
    plt.close()



def bar_chart(title, labels, data1, file_name, data2 = None, legend=["", ""]):

    colors = ["orange", "grey"]

    fig, ax = plt.subplots(1)
    xpos = np.arange(len(data1))
    width = 0.35

    plt.title(title)
    y_pos = np.arange(len(data1))

    if data2 is not None:
        ppl.bar(xpos+width, data1, color="orange", width=0.35, annotate=True)
        ppl.bar(xpos, data2, grid='y', width = 0.35, annotate=True)
        plt.xticks(xpos+width, labels)
        plt.legend(legend, loc=2)

    else:
        ppl.bar(xpos, data1, grid='y', annotate=True)
        plt.xticks(xpos+width, labels)

    plt.savefig(file_name + ".eps")
    plt.close()



def ts_chart(title, unixtime_dates, data, file_name):

    fig = plt.figure()
    plt.title(title)

    dates = []
    for unixdate in unixtime_dates:
        dates.append(datetime.fromtimestamp(float(unixdate)))

    ppl.plot(dates, data)
    fig.autofmt_xdate()
    fig.savefig(file_name + ".eps")


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
    parser.add_option("-j", "--projects",
                      action="store",
                      dest="dbprojects",
                      help="Database with projects information")

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

def scm_general(dbcon, filters):
    # Aggregated information for SCM data source

    from vizgrimoire.analysis.onion_model import CommunityStructure
    onion = CommunityStructure(dbcon, filters)
    result = onion.result()

    dataset = {}
    dataset["core"] = result["core"]
    dataset["regular"] = result["regular"]
    dataset["occasional"] = result["occasional"]

    authors_period = scm.AuthorsPeriod(dbcon, filters)
    dataset["authorsperiod"] = authors_period.get_agg()["avg_authors_month"]

    authors = scm.Authors(dbcon, filters)
    top_authors = authors.get_list()
    dataset["topauthors"] = top_authors

    return dataset

def scm_report(dbcon, filters):
    # Per release aggregated information
    project_name = "general"

    commits = scm.Commits(dbcon, filters)
    createJSON(commits.get_agg(), "./release/scm_commits_"+project_name+".json")

    authors = scm.Authors(dbcon, filters)
    createJSON(authors.get_agg(), "./release/scm_authors_"+project_name+".json")

    dataset = {}
    dataset["commits"] = commits.get_agg()["commits"]
    dataset["authors"] = authors.get_agg()["authors"]

    # tops authors activity
    top_authors = authors.get_list()
    if not isinstance(top_authors["commits"], list):
        top_authors["commits"] = [top_authors["commits"]]
        top_authors["id"] = [top_authors["id"]]
        top_authors["authors"] = [top_authors["authors"]]
    createJSON(top_authors, "./release/scm_top_authors_project_"+project_name+".json")
    createCSV(top_authors, "./release/scm_top_authors_project_"+project_name+".csv", ["id"])

    # top companies activity
    companies = scm.Companies(dbcon, filters)
    top_companies = companies.get_list(filters)
    if not isinstance(top_companies["company_commits"], list):
        top_companies["company_commits"] = [top_companies["company_commits"]]
        top_companies["companies"] = [top_companies["name"]]
    createJSON(top_companies, "./release/scm_top_companies_project_"+project_name+".json")
    createCSV(top_companies, "./release/scm_top_companies_project_"+project_name+".csv")

    return dataset

def its_report(dbcon, filters):
    # Per release its information

    from vizgrimoire.ITS import ITS
    ITS.set_backend("jira")

    project_name = "general"
    project_name = project_name.replace(" ", "")
    if project_name == 'Documentation':
        ITS._get_backend().closed_condition = "(new_value='Fix Committed' or new_value='Fix Released')"
    else:
        ITS.closed_condition = "(new_value='Fix Committed')"

    opened = its.Opened(dbcon, filters)
    createJSON(opened.get_agg(), "./release/its_opened_"+project_name+".json")
    closed = its.Closed(dbcon, filters)
    createJSON(closed.get_agg(), "./release/its_closed_"+project_name+".json")

    dataset = {}
    dataset["opened"] = opened.get_agg()["opened"]
    dataset["closed"] = closed.get_agg()["closed"]

    return dataset


def scr_report(dbcon, filters):
    # Per release code review information
    project_name = "general"
    project_name = project_name.replace(" ", "")


    submitted = scr.Submitted(dbcon, filters)
    createJSON(submitted.get_agg(), "./release/scr_submitted_"+project_name+".json")

    merged = scr.Merged(dbcon, filters)
    createJSON(merged.get_agg(), "./release/scr_merged.json_"+project_name+"")

    abandoned = scr.Abandoned(dbcon, filters)
    createJSON(abandoned.get_agg(), "./release/scr_abandoned_"+project_name+".json")

    bmi = scr.BMISCR(dbcon, filters)
    createJSON(bmi.get_agg(), "./release_scr_bmi_"+project_name+".json")

    active_core = scr.ActiveCoreReviewers(dbcon, filters)
    createJSON(active_core.get_agg(), "./release/scr_activecore_"+project_name+".json")

    iterations = scr.PatchesPerReview(dbcon, filters)
    createJSON(iterations.get_agg()["median"], "./release/scr_iterations_"+project_name+".json")

    filters.period = "month"
    time2review = scr.TimeToReview(dbcon, filters)

    time2reviewpatch = scr.TimeToReviewPatch(dbcon, filters)
    time2 = time2reviewpatch.get_agg()

    time_list = time2["waitingtime4reviewer"]
    waiting4reviewer_times = []
    for time in time_list:
        if time >= 0:
            waiting4reviewer_times.append(time)
    data = DHESA(waiting4reviewer_times)
    waiting4reviewer_mean = float(data.data["mean"]) / 86400.0 # seconds in a day
    waiting4reviewer_median = float(data.data["median"]) / 86400.0

    time_list = time2["waitingtime4submitter"]
    waiting4submitter_times = []
    for time in time_list:
        if time >=0:
            waiting4submitter_times.append(time)
    data = DHESA(waiting4submitter_times)
    waiting4submitter_mean = float(data.data["mean"]) / 86400.0
    waiting4submitter_median = float(data.data["median"]) / 86400.0

    dataset = {}
    dataset["submitted"] = submitted.get_agg()["submitted"]
    dataset["merged"] = merged.get_agg()["merged"]
    dataset["abandoned"] = abandoned.get_agg()["abandoned"]
    dataset["bmiscr"] = round(bmi.get_agg()["bmiscr"], 2)
    dataset["active_core"] = active_core.get_agg()["active_core_reviewers"]
    dataset["review_time_days_median"] = round(time2review.get_agg()["review_time_days_median"], 2)
    dataset["review_time_days_avg"] = round(time2review.get_agg()["review_time_days_avg"], 2)
    dataset["iterations_mean"] = round(iterations.get_agg()["mean"], 2)
    dataset["iterations_median"] = round(iterations.get_agg()["median"], 2)
    dataset["waiting4submitter_mean"] = round(waiting4submitter_mean, 2)
    dataset["waiting4submitter_median"] = round(waiting4submitter_median, 2)
    dataset["waiting4reviewer_mean"] = round(waiting4reviewer_mean, 2)
    dataset["waiting4reviewer_median"] = round(waiting4reviewer_median, 2)

    return dataset

def serialize_threads(threads, crowded, threads_object):
    # Function needed to reorder information coming from the
    # Threads class
    l_threads = {}
    if crowded:
        l_threads['people'] = []
    else:
        l_threads['len'] = []
    l_threads['subject'] = []
    l_threads['date'] = []
    l_threads['initiator'] = []
    for email_people in threads:
        if crowded:
            email = email_people[0]
        else:
            email = email_people
        if crowded:    
            l_threads['people'].append(email_people[1])
        else:
            l_threads['len'].append(threads_object.lenThread(email.message_id))
        subject = email.subject.replace(",", " ")
        subject = subject.replace("\n", " ")
        l_threads['subject'].append(subject)
        l_threads['date'].append(email.date.strftime("%Y-%m-%d"))
        l_threads['initiator'].append(email.initiator_name.replace(",", " "))

    return l_threads

def mls_report(dbcon, filters):
    # Per release MLS information
    emails = mls.EmailsSent(dbcon, filters)
    createJSON(emails.get_agg(), "./release/mls_emailssent.json")

    senders = mls.EmailsSenders(dbcon, filters)
    createJSON(senders.get_agg(), "./release/mls_emailssenders.json")

    senders_init = mls.SendersInit(dbcon, filters)
    createJSON(senders_init.get_agg(), "./release/mls_sendersinit.json")

    dataset = {}
    dataset["sent"] = emails.get_agg()["sent"]
    dataset["senders"] = senders.get_agg()["senders"]
    dataset["senders_init"] = senders_init.get_agg()["senders_init"]

    from vizgrimoire.analysis.threads import Threads
    SetDBChannel(dbcon.user, dbcon.password, dbcon.database)
    threads = Threads(filters.startdate, filters.enddate, dbcon.identities_db)
    top_longest_threads = threads.topLongestThread(10)
    top_longest_threads = serialize_threads(top_longest_threads, False, threads)
    createJSON(top_longest_threads, "./release/mls_top_longest_threads.json")
    createCSV(top_longest_threads, "./release/mls_top_longest_threads.csv")

    #top_crowded_threads = threads.topCrowdedThread(10)
    #top_crowded_threads = serialize_threads(top_crowded_threads, True, threads)
    #createJSON(top_crowded_threads, "./release/mls_top_crowded_threads.json")
    #createCSV(top_crowded_threads, "./release/mls_top_crowded_threads.csv")

    return dataset


def parse_urls(urls):
    # Funtion needed to remove "odd" characters
    qs_aux = []
    for url in urls:
        #url = url.replace("https://ask.openstack.org/en/question/", "")
        url = url.replace("_", "\_")
        qs_aux.append(url)
    return qs_aux

def get_qa_subjects(sites):
    subjects = []
    if len(sites) > 0 and len(sites[0]) == 1:
        string = ""
        for value in sites:
            string = string + value
        sites = []
        sites.append(string)
    for site in sites:
        url_parts = site.split("/")
        parts = url_parts[5].split("-")
        count = 0
        # some random number to take just the first four
        # words of the url in order to build some sentence
        # for the link
        sentence = ""
        while count < 6:
            if len(parts) > count:
                sentence = sentence + " " + parts[count]
            count = count + 1

        subjects.append(sentence)

    return subjects

def qaforums_report(dbcon, filters):
    # Aggregated information per release 
    print filters.startdate
    questions = qa.Questions(dbcon, filters)
    createJSON(questions.get_agg(), "./release/qaforums_questions.json")

    answers = qa.Answers(dbcon, filters)
    createJSON(answers.get_agg(), "./release/qaforums_answers.json")

    comments = qa.Comments(dbcon, filters)
    createJSON(comments.get_agg(), "./release/qaforums_comments.json")

    q_senders = qa.QuestionSenders(dbcon, filters)
    createJSON(q_senders.get_agg(), "./release/qaforums_question_senders.json")

    dataset = {}
    dataset["questions"] = questions.get_agg()["qsent"]
    dataset["answers"] = answers.get_agg()["asent"]
    dataset["comments"] = comments.get_agg()["csent"]
    dataset["qsenders"] = q_senders.get_agg()["qsenders"]

    import vizgrimoire.analysis.top_questions_qaforums as top
    tops = top.TopQuestions(dbcon, filters)
    commented = tops.top_commented()
    commented["qid"] = commented.pop("question_identifier")
    # Taking the last part of the URL
    commented["site"] = parse_urls(commented.pop("url"))
    commented["subject"] = get_qa_subjects(commented["site"])
    print commented
    #TODO#createJSON(commented, "./release/qa_top_questions_commented.json")
    #createCSV(commented, "./release/qa_top_questions_commented.csv")

    visited = tops.top_visited()
    visited["qid"] = visited.pop("question_identifier")
    visited["site"] = parse_urls(visited.pop("url"))
    visited["subject"] = get_qa_subjects(visited["site"])
    #commented["site"] = commented.pop("url").split("/")[-2:][1:]

    createJSON(visited, "./release/qa_top_questions_visited.json")
    createCSV(visited, "./release/qa_top_questions_visited.csv")

    crowded = tops.top_crowded()
    crowded["qid"] = crowded.pop("question_identifier")
    crowded["site"] = parse_urls(crowded.pop("url"))
    crowded["subject"] = get_qa_subjects(crowded["site"])
    createJSON(crowded, "./release/qa_top_questions_crowded.json")
    createCSV(crowded, "./release/qa_top_questions_crowded.csv")

    filters.npeople = 15
    createJSON(tops.top_tags(), "./release/qa_top_tags.json")
    createCSV(tops.top_tags(), "./release/qa_top_tags.csv")

    return dataset

def irc_report(dbcon, filters):
    # per release information for IRC
    pass
    sent = irc.Sent(dbcon, filters)
    createJSON(sent.get_agg(), "./release/irc_sent.json")

    senders = irc.Senders(dbcon, filters)
    createJSON(senders.get_agg(), "./release/irc_senders.json")

    dataset = {}
    dataset["sent"] = sent.get_agg()["sent"]
    dataset["senders"] = senders.get_agg()["senders"]

    top_senders = senders.get_list()
    createJSON(top_senders, "./release/irc_top_senders.json")
    createCSV(top_senders, "./release/irc_top_senders.csv")

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

def init_env():
    # Init environment
    grimoirelib = path.join("..","vizgrimoire")
    metricslib = path.join("..","vizgrimoire","metrics")
    studieslib = path.join("..","vizgrimoire","analysis")
    datahandler = path.join("..","vizgrimoire","datahandlers")
    alchemy = path.join("..")
    for dir in [grimoirelib,metricslib,studieslib,datahandler,alchemy]:
        sys.path.append(dir)

    # env vars for R
    environ["LANG"] = ""
    environ["R_LIBS"] = "../../r-lib"


def data_source_increment_activity(opts, people_out, affs_out):
    # Per data source, the increment or decrement of the activity is displayed
    dataset = {}

    data_sources = ["Gits", "Tickets", "Mailing Lists", "Gerrit", "Askbot", "IRC"]
    action = ["commits", "closed tickets", "sent emails", "submitted reviews", "posted questions", "messages"]
    net_values = []
    rel_values = [] #percentage wrt the previous 365 days

    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
    its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
    mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities)
    scr_dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities)
    qaforums_dbcon = QAForumsQuery(opts.dbuser, opts.dbpassword, opts.dbqaforums, opts.dbidentities)
    irc_dbcon = IRCQuery(opts.dbuser, opts.dbpassword, opts.dbirc, opts.dbidentities)

    period = "month"
    type_analysis = None
    releases = opts.releases.split(",")[-2:]
    startdate = "'"+releases[0]+"'"
    enddate = "'"+releases[1]+"'"
    filters = MetricFilters(period, startdate, enddate, None, 10, people_out, affs_out)

    commits = scm.Commits(scm_dbcon, filters)
    closed = its.Closed(its_dbcon, filters)
    emails = mls.EmailsSent(mls_dbcon, filters)
    submitted = scr.Submitted(scr_dbcon, filters)
    questions = qa.Questions(qaforums_dbcon, filters)
    messages = irc.Sent(irc_dbcon, filters)


    from vizgrimoire.ITS import ITS
    ITS.set_backend("jira")

    net_values.append(commits.get_trends(releases[1], 90)["commits_90"])
    rel_values.append(commits.get_trends(releases[1], 90)["percentage_commits_90"])
    net_values.append(closed.get_trends(releases[1], 90)["closed_90"])
    rel_values.append(closed.get_trends(releases[1], 90)["percentage_closed_90"])
    net_values.append(emails.get_trends(releases[1], 90)["sent_90"])
    rel_values.append(emails.get_trends(releases[1], 90)["percentage_sent_90"])
    net_values.append(submitted.get_trends(releases[1], 90)["submitted_90"])
    rel_values.append(submitted.get_trends(releases[1], 90)["percentage_submitted_90"])
    net_values.append(questions.get_trends(releases[1], 90)["qsent_90"])
    rel_values.append(questions.get_trends(releases[1], 90)["percentage_qsent_90"])
    net_values.append(messages.get_trends(releases[1], 90)["sent_90"])
    rel_values.append(messages.get_trends(releases[1], 90)["percentage_sent_90"])

    createCSV({"datasource":data_sources, "metricsnames":action, "relativevalues":rel_values, "netvalues":net_values}, "./release/data_source_evolution.csv")

def integrated_projects_activity(dbcon, opts, people_out, affs_out):
    # Commits per integrated project

    projects = integrated_projects(dbcon)

    projects_ids = projects["subproject_id"]
    projects_list = []
    commits_list = []

    period = "month"
    releases = opts.releases.split(",")[-2:]
    startdate = "'"+releases[0]+"'"
    enddate = "'"+releases[1]+"'"

    for project_id in projects_ids:
        project_title = "'" + project_id + "'"
        type_analysis = ["project", project_title]
        project_filters = MetricFilters(period, startdate, enddate, type_analysis, 10,
                                        people_out, affs_out)

        commits = scm.Commits(dbcon, project_filters)

        projects_list.append(project_id)
        commits_list.append(int(commits.get_agg()["commits"]))

    createCSV({"projects":projects_list, "commits":commits_list}, "./release/integrated_projects_commits.csv")


def integrated_projects_top_orgs(dbcon, people_out, affs_out):
    # Top orgs contributing to each integrated project
    # The two first companies are depicted.
    projects = integrated_projects(dbcon)

    projects_ids = projects["subproject_id"]
    projects_list = []
    commits_top1 = []
    commits_top2 = []
    orgs_top1 = []
    orgs_top2 = []

    period = "month"
    releases = opts.releases.split(",")[-2:]
    startdate = "'"+releases[0]+"'"
    enddate = "'"+releases[1]+"'"

    for project_id in projects_ids:
        project_title = "'" + project_id + "'"
        type_analysis = ["project", project_title]
        project_filters = MetricFilters(period, startdate, enddate, type_analysis, 10,
                                        people_out, affs_out)

        companies = scm.Companies(dbcon, project_filters)
        activity = companies.get_list()

        projects_list.append(project_id)
        commits_top1.append(activity["company_commits"][0])
        commits_top2.append(activity["company_commits"][1])
        orgs_top1.append(activity["name"][0])
        orgs_top2.append(activity["name"][1])

    createCSV({"projects":projects_list, "commitstopone":commits_top1, "commitstoptwo":commits_top2, "orgstopone":orgs_top1, "orgstoptwo":orgs_top2}, "./release/integrated_projects_top_orgs.csv")

def integrated_projects_top_contributors(scm_dbcon, people_out, affs_out):
    # Top contributor per integrated project
    projects = integrated_projects(scm_dbcon)

    projects_ids = projects["subproject_id"]
    projects_list = []
    commits = []
    top_contributors = []

    period = "month"
    releases = opts.releases.split(",")[-2:]
    startdate = "'"+releases[0]+"'"
    enddate = "'"+releases[1]+"'"

    for project_id in projects_ids:
        project_title = "'" + project_id + "'"
        type_analysis = ["project", project_title]
        project_filters = MetricFilters(period, startdate, enddate, type_analysis, 10,
                                        people_out, affs_out)
        authors = scm.Authors(scm_dbcon, project_filters)
        activity = authors.get_list()

        projects_list.append(project_id)
        commits.append(activity["commits"][0])
        top_contributors.append(activity["authors"][0])

    createCSV({"projects":projects_list, "commits":commits, "contributors":top_contributors}, "./release/integrated_projects_top_contributors.csv")

def projects_efficiency(opts, people_out, affs_out):
    # BMI and time to review in mean per general project
    scr_dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities, opts.dbprojects)
    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities, opts.dbprojects)
    its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities, opts.dbprojects)

    bmi_list = []
    time2review_list = []
    bmi_its = []

    period = "month"
    releases = opts.releases.split(",")[-2:]
    startdate = "'"+releases[0]+"'"
    enddate = "'"+releases[1]+"'"

    type_analysis = None
    project_filters = MetricFilters(period, startdate, enddate, type_analysis, 10,
                                        people_out, affs_out)
    scr_bmi = scr.BMISCR(scr_dbcon, project_filters)
    time2review = scr.TimeToReview(scr_dbcon, project_filters)

    # ITS BMI index
    from vizgrimoire.ITS import ITS
    ITS.set_backend("jira")


    opened = its.Opened(its_dbcon, project_filters)
    closed = its.Closed(its_dbcon, project_filters)

    tickets_opened = opened.get_agg()["opened"]
    tickets_closed = closed.get_agg()["closed"]

    its_bmi = 0
    if tickets_closed > 0:
       its_bmi = round(float(tickets_closed)/float(tickets_opened), 2)


    bmi_list.append(round(scr_bmi.get_agg()["bmiscr"], 2))
    time2review_list.append(round(time2review.get_agg()["review_time_days_median"], 2))
    bmi_its.append(its_bmi)


    createCSV({"projects":"general", "bmi":bmi_list, "timereview":time2review_list, "bmiits":bmi_its}, "./release/integrated_projects_efficiency.csv")

def timezone_analysis(opts):
    from vizgrimoire.analysis.timezone import Timezone
    from vizgrimoire.SCM import SCM
    from vizgrimoire.MLS import MLS

    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities, opts.dbprojects)
    mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities, opts.dbprojects)

    period = "month"
    releases = opts.releases.split(",")[-2:]
    startdate = "'"+releases[0]+"'"
    enddate = "'"+releases[1]+"'"
    filters = MetricFilters(period, startdate, enddate, [], 10, "", "")

    tz = Timezone(scm_dbcon, filters)
    dataset = tz.result(SCM)
    labels = dataset["tz"]
    commits = dataset["commits"]
    authors = dataset["authors"]
    bar_chart("Timezone git activity", labels, commits, "commits_tz", authors, ["commits", "authors"])

    tz = Timezone(mls_dbcon, filters)
    dataset = tz.result(MLS)
    messages = dataset["messages"]
    authors = dataset["authors"]
    bar_chart("Timezone mailing list activity", labels, messages, "messages_tz", authors, ["messages", "authors"])


def general_info(opts, releases, people_out, affs_out):
    # General info from MLS, IRC and QAForums.
    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities, opts.dbprojects)

    # analysis currently failing
    #timezone_analysis(opts)

    core = []
    regular = []
    occasional = []
    authors_month = []

    emails = []
    emails_senders =  []
    emails_senders_init = []
    questions = []
    answers = []
    comments = []
    qsenders = []
    irc_sent = []
    irc_senders = []
    releases_data = {}
    for release in releases:
        startdate = "'" + release[0] + "'"
        enddate = "'" + release[1] + "'"
        print "General info per release: " + startdate + " - " + enddate
        filters = MetricFilters("month", startdate, enddate, None, opts.npeople, people_out, affs_out)
        # SCM info
        print "    General info: SCM"
        scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities, opts.dbprojects)
        dataset = scm_general(scm_dbcon, filters)
        core.append(dataset["core"])
        regular.append(dataset["regular"])
        occasional.append(dataset["occasional"])
        authors_month.append(float(dataset["authorsperiod"]))
        top_authors = dataset["topauthors"]
        release_pos = releases.index(release)
        createCSV(top_authors, "./release/top_authors_release" + str(release_pos)+ ".csv")

        # MLS info
        print "    General info: MLS"
        mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities, opts.dbprojects)
        dataset = mls_report(mls_dbcon, filters)
        emails.append(dataset["sent"])
        emails_senders.append(dataset["senders"])
        emails_senders_init.append(dataset["senders_init"])

        # QAForums info 
        print "    General info: QAForums"
        qaforums_dbcon = QAForumsQuery(opts.dbuser, opts.dbpassword, opts.dbqaforums, opts.dbidentities, opts.dbprojects)
        dataset = qaforums_report(qaforums_dbcon, filters)
        questions.append(dataset["questions"])
        answers.append(dataset["answers"])
        comments.append(dataset["comments"])
        qsenders.append(dataset["qsenders"])

        # IRC info
        print "    General info: IRC"
        irc_dbcon = IRCQuery(opts.dbuser, opts.dbpassword, opts.dbirc, opts.dbidentities, opts.dbprojects)
        dataset = irc_report(irc_dbcon, filters)
        irc_sent.append(dataset["sent"])
        irc_senders.append(dataset["senders"])



    labels = ["15-Q1", "15-Q2", "15-Q3", "15-Q4"]
    #labels = ["2013-Q3", "2013-Q4", "2014-Q1", "2014-Q2"]
    bar_chart("Emails sent", labels, emails, "emails")

    createCSV({"labels":labels, "emails":emails}, "./release/emails.csv")
    bar_chart("People sending emails", labels, emails_senders, "emails_senders")
    createCSV({"labels":labels, "senders":emails_senders}, "./release/emails_senders.csv")
    bar_chart("People initiating threads", labels, emails_senders_init, "emails_senders_init")
    createCSV({"labels":labels, "senders":emails_senders_init}, "./release/emails_senders_init.csv")
    bar_chart("Questions", labels, questions, "questions")
    createCSV({"labels":labels, "questions":questions}, "./release/questions.csv")
    bar_chart("Answers", labels, answers, "answers")
    createCSV({"labels":labels, "answers":answers}, "./release/answers.csv")
    bar_chart("Comments", labels, comments, "comments")
    createCSV({"labels":labels, "comments":comments}, "./release/comments.csv")
    bar_chart("People asking Questions", labels, qsenders, "question_senders")
    createCSV({"labels":labels, "senders":qsenders}, "./release/question_senders.csv")
    bar_chart("Messages in IRC channels", labels, irc_sent, "irc_sent")
    createCSV({"labels":labels, "messages":irc_sent}, "./release/irc_sent.csv")
    bar_chart("People in IRC channels", labels, irc_senders, "irc_senders")
    createCSV({"labels":labels, "senders":irc_senders}, "./release/irc_senders.csv")

    bar3_chart("Community structure", labels, regular, "onion", core, occasional, ["casual", "regular", "core"])
    createCSV({"labels":labels, "core":core, "regular":regular, "occasional":occasional}, "./release/onion_model.csv")
    bar_chart("Developers per month", labels, authors_month, "authors_month")
    createCSV({"labels":labels, "authormonth":authors_month}, "./release/authors_month.csv")

    # other analysis
    print "Other analysis"
    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)

    # Increment of activity in the last 365 days by data source
    #data_source_increment_activity(opts, people_out, affs_out)

    # Commits and reviews per integrated project
    # Deprecated function, 'integrated' projects do not exist any more.
    #integrated_projects_activity(scm_dbcon, opts, people_out, affs_out)

    # Top orgs per integrated project
    # Deprecated function, 'integrated' projects do not exist any more.
    #integrated_projects_top_orgs(scm_dbcon, people_out, affs_out)

    # Top contributors per integrated project 
    # Deprecated function, 'integrated' projects do not exist any more.
    #integrated_projects_top_contributors(scm_dbcon, people_out, affs_out)


    # Efficiency per general project
    projects_efficiency(opts, people_out, affs_out)

    # TZ analysis
    timezone_analysis(opts)

def releases_info(startdate, enddate, opts, people_out, affs_out):
    # Releases information.
    data = {}
    filters = MetricFilters("month", startdate, enddate, None, opts.npeople,
                             people_out, affs_out)
    # SCM report
    scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
    dataset = scm_report(scm_dbcon, filters)
    data["scm"] = dataset

    #ITS report
    its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
    dataset = its_report(its_dbcon, filters)
    data["its"] = dataset

    #SCR Report
    scr_dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities)
    dataset = scr_report(scr_dbcon, filters)
    data["scr"] = dataset

    return data


def print_n_draw(agg_data):
    project = "general"
    # The releases information is print in CSV/JSON format and specific charts are built

    labels = ["15-Q1", "15-Q2", "15-Q3", "15-Q4"]
    #labels = ["2013-Q3", "2013-Q4", "2014-Q1", "2014-Q2"]
    project_name = project.replace(" ", "")

    commits = agg_data["commits"]
    submitted = agg_data["submitted"]
    bar_chart("Commits and reviews: " + project, labels, submitted, "commits"  + project_name, commits, ["reviews", "commits"])
    createCSV({"labels":labels, "commits":commits, "submitted":submitted}, "./release/commits"+project_name+".csv")


    authors = agg_data["authors"]
    bar_chart("Authors " + project, labels, authors, "authors" + project_name)
    createCSV({"labels":labels, "authors":authors}, "./release/authors"+project_name+".csv")

    opened = agg_data["opened"]
    closed = agg_data["closed"]
    bar_chart("Opened and closed tickets: " + project, labels, opened, "closed" + project_name, closed, ["opened", "closed"])
    createCSV({"labels":labels, "closed":closed, "opened":opened}, "./release/closed"+project_name+".csv")

    bmi = agg_data["bmi"]
    bar_chart("Efficiency closing tickets: " + project, labels, bmi, "bmi" + project_name)
    createCSV({"labels":labels, "bmi":bmi}, "./release/bmi"+project_name+".csv")

    merged = agg_data["merged"]
    abandoned = agg_data["abandoned"]
    bmiscr = agg_data["bmiscr"]
    bar_chart("Merged and abandoned reviews: " + project, labels, merged, "submitted_reviews" + project_name, abandoned, ["merged", "abandoned"])
    createCSV({"labels":labels, "merged":merged, "abandoned":abandoned, "bmi":bmiscr}, "./release/submitted_reviews"+project_name+".csv")

    bmiscr = agg_data["bmiscr"]
    bar_chart("Changesets efficiency: " + project, labels, bmiscr, "bmiscr" + project_name)

    iters_reviews_avg = agg_data["iters_reviews_avg"]
    iters_reviews_median = agg_data["iters_reviews_median"]
    bar_chart("Patchsets per Changeset: " + project, labels, iters_reviews_avg, "patchsets_avg" + project_name, iters_reviews_median, ["mean", "median"])
    createCSV({"labels":labels, "meanpatchsets":iters_reviews_avg, "medianpatchsets":iters_reviews_median}, "./release/scr_patchsets_iterations" + project_name+".csv")

    active_core_reviewers = agg_data["active_core_reviewers"]
    bar_chart("Active Core Reviewers: " + project, labels, active_core_reviewers, "active_core_scr"+project_name)
    createCSV({"labels":labels, "activecorereviewers":active_core_reviewers}, "./release/active_core_scr"+project_name+".csv")

    review_avg = agg_data["review_avg"]
    review_median = agg_data["review_median"]
    bar_chart("Time to merge (days): " + project, labels, review_avg, "timetoreview_median" + project_name, review_median, ["mean", "median"])
    createCSV({"labels":labels, "mediantime":review_median, "meantime":review_avg}, "./release/timetoreview_median"+project_name+".csv")

    waiting4reviewer_mean = agg_data["waiting4reviewer_mean"]
    waiting4reviewer_median = agg_data["waiting4reviewer_median"]
    bar_chart("Time waiting for the reviewer: " + project, labels, waiting4reviewer_mean, "waiting4reviewer_avg" + project_name, waiting4reviewer_median, ["avg", "median"])
    createCSV({"labels":labels, "mediantime":waiting4reviewer_median, "meantime":waiting4reviewer_mean}, "./release/timewaiting4reviewer_median"+project_name+".csv")

    waiting4submitter_mean = agg_data["waiting4submitter_mean"]
    waiting4submitter_median = agg_data["waiting4submitter_median"]
    bar_chart("Time waiting for the submitter: " + project, labels, waiting4submitter_mean, "waiting4submitter_avg" + project_name, waiting4submitter_median, ["avg", "median"])
    createCSV({"labels":labels, "mediantime":waiting4submitter_median, "meantime":waiting4submitter_mean}, "./release/timewaiting4submitter_median"+project_name+".csv")


def order_data(agg_data, releases):
    # Ordering dta coming from releases
    for release in releases:
        agg_data["labels"].append(release[1])

        #scm
        agg_data["commits"].append(releases_data[release]["scm"]["commits"])
        agg_data["authors"].append(releases_data[release]["scm"]["authors"])

        #its
        agg_data["opened"].append(releases_data[release]["its"]["opened"])
        agg_data["closed"].append(releases_data[release]["its"]["closed"])
        if releases_data[release]["its"]["opened"] > 0:
           agg_data["bmi"].append(round(float(releases_data[release]["its"]["closed"])/float(releases_data[release]["its"]["opened"]), 2))
        else:
           agg_data["bmi"].append(0)

        #scr
        agg_data["submitted"].append(releases_data[release]["scr"]["submitted"])
        agg_data["merged"].append(releases_data[release]["scr"]["merged"])
        agg_data["abandoned"].append(releases_data[release]["scr"]["abandoned"])
        agg_data["review_avg"].append(releases_data[release]["scr"]["review_time_days_avg"])
        agg_data["review_median"].append(releases_data[release]["scr"]["review_time_days_median"])
        agg_data["active_core_reviewers"].append(releases_data[release]["scr"]["active_core"])
        agg_data["iters_reviews_avg"].append(releases_data[release]["scr"]["iterations_mean"])
        agg_data["iters_reviews_median"].append(releases_data[release]["scr"]["iterations_median"])
        agg_data["bmiscr"].append(releases_data[release]["scr"]["bmiscr"])
        agg_data["waiting4submitter_mean"].append(releases_data[release]["scr"]["waiting4submitter_mean"])
        agg_data["waiting4submitter_median"].append(releases_data[release]["scr"]["waiting4submitter_median"])
        agg_data["waiting4reviewer_mean"].append(releases_data[release]["scr"]["waiting4reviewer_mean"])
        agg_data["waiting4reviewer_median"].append(releases_data[release]["scr"]["waiting4reviewer_median"])

    return agg_data


if __name__ == '__main__':

    locale.setlocale(locale.LC_ALL, 'en_US.utf8')

    init_env()

    from vizgrimoire.metrics.metrics import Metrics
    from vizgrimoire.metrics.query_builder import DSQuery, SCMQuery, QAForumsQuery, MLSQuery, SCRQuery, ITSQuery, IRCQuery
    from vizgrimoire.metrics.metrics_filter import MetricFilters
    import vizgrimoire.metrics.scm_metrics as scm
    import vizgrimoire.metrics.qaforums_metrics as qa
    import vizgrimoire.metrics.mls_metrics as mls
    import vizgrimoire.metrics.scr_metrics as scr
    import vizgrimoire.metrics.its_metrics as its
    import vizgrimoire.metrics.irc_metrics as irc
    from vizgrimoire.GrimoireUtils import createJSON
    from vizgrimoire.GrimoireSQL import SetDBChannel
    from vizgrimoire.datahandlers.data_handler import DHESA

    # parse options
    opts = read_options()

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = build_releases(opts.releases)


    people_out = ["OpenStack Jenkins","Launchpad Translations on behalf of nova-core","Jenkins","OpenStack Hudson","gerrit2@review.openstack.org","linuxdatacenter@gmail.com","Openstack Project Creator","Openstack Gerrit","openstackgerrit"]
    affs_out = ["-Bot","-Individual","-Unknown"]

    releases_data = {}
    for release in releases:
        print "    Release: " + str(release[0]) + " - " + str(release[1])
        releases_data[release] = {}

        startdate = "'" + release[0] + "'"
        enddate = "'" + release[1] + "'"
        # Per release and project, an analysis is undertaken
        releases_data[release] = releases_info(startdate, enddate, opts, people_out, affs_out)

    # Information is now stored in lists. Each list for each metric contains the values
    # of the releases analysis. Each entry in the list corresponds to the value of such 
    # metric in such release and in such project.
    print "    Ordering data"
    metrics = ["labels", "commits", "authors", "opened", "submitted", "merged", "abandoned", "bmiscr",
               "closed", "active_core_reviewers", "iters_reviews_avg", "iters_reviews_median", "bmi",
               "review_avg", "review_median", "waiting4submitter_mean", "waiting4submitter_median",
               "waiting4reviewer_mean", "waiting4reviewer_median"]
    agg_data = {}
    for metric in metrics:
        # init agg_data structure
        agg_data[metric] = []

    agg_data = order_data(agg_data, releases)

    print_n_draw(agg_data)


    # general info: scm, mls, irc and qaforums
    general_info(opts, releases, people_out, affs_out)

    data_source_increment_activity(opts, people_out, affs_out)
