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
## python openstack_report.py -a dic_cvsanaly_openstack_2259 -d dic_bicho_openstack_gerrit_3392_bis -i dic_cvsanaly_openstack_2259 -r 2013-07-01,2013-10-01,2014-01-01,2014-04-01,2014-07-01 -c lcanas_bicho_openstack_1376 -b lcanas_mlstats_openstack_1376 -f dic_sibyl_openstack_3194_new -e dic_irc_openstack_3277


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
    ppl.barh(y_pos, xvalues, grid='x')
    plt.yticks(y_pos, yvalues)
    plt.savefig(file_name + ".eps")
    plt.close()


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

    project_name = filters.type_analysis[1]
    project_name = project_name.replace(" ", "")

    commits = scm.Commits(dbcon, filters)
    createJSON(commits.get_agg(), "./release/scm_commits_"+project_name+".json")

    authors = scm.Authors(dbcon, filters)
    createJSON(authors.get_agg(), "./release/scm_authors_"+project_name+".json")

    dataset = {}
    dataset["commits"] = commits.get_agg()["commits"]
    dataset["authors"] = authors.get_agg()["authors"]

    # tops authors activity
    #from top_authors_projects import TopAuthorsProjects
    #top_authors = TopAuthorsProjects(dbcon, filters)
    top_authors = authors.get_list()
    createJSON(top_authors, "./release/scm_top_authors_project_"+project_name+".json")
    createCSV(top_authors, "./release/scm_top_authors_project_"+project_name+".csv", ["id"])

    # top companies activity
    from top_companies_projects import TopCompaniesProjects
    #top_companies = TopCompaniesProjects(dbcon, filters)
    #top_companies = top_companies.result()
    companies = scm.Companies(dbcon, filters)
    top_companies = companies.get_list(filters)
    createJSON(top_companies, "./release/scm_top_companies_project_"+project_name+".json")
    createCSV(top_companies, "./release/scm_top_companies_project_"+project_name+".csv")

    return dataset

def its_report(dbcon, filters):
    from ITS import ITS
    ITS.set_backend("launchpad")

    project_name = filters.type_analysis[1]
    project_name = project_name.replace(" ", "")

    opened = its.Opened(dbcon, filters)
    createJSON(opened.get_agg(), "./release/its_opened_"+project_name+".json")

    closed = its.Closed(dbcon, filters)
    createJSON(closed.get_agg(), "./release/its_closed_"+project_name+".json")

    dataset = {}
    dataset["opened"] = opened.get_agg()["opened"]
    dataset["closed"] = closed.get_agg()["closed"]

    return dataset


def scr_report(dbcon, filters):

    project_name = filters.type_analysis[1]
    project_name = project_name.replace(" ", "")


    submitted = scr.Submitted(dbcon, filters)
    createJSON(submitted.get_agg(), "./release/scr_submitted_"+project_name+".json")

    merged = scr.Merged(dbcon, filters)
    createJSON(merged.get_agg(), "./release/scr_merged.json_"+project_name+"")

    abandoned = scr.Abandoned(dbcon, filters)
    createJSON(abandoned.get_agg(), "./release/scr_abandoned_"+project_name+".json")

    waiting4reviewer = scr.ReviewsWaitingForReviewer(dbcon, filters)
    createJSON(waiting4reviewer.get_agg(), "./release/scr_waiting4reviewer_"+project_name+".json")

    waiting4submitter = scr.ReviewsWaitingForSubmitter(dbcon, filters)
    createJSON(waiting4submitter.get_agg(), "./release/scr_waiting4submitter_"+project_name+".json")

    filters.period = "month"
    time2review = scr.TimeToReview(dbcon, filters)

    dataset = {}
    dataset["submitted"] = submitted.get_agg()["submitted"]
    dataset["merged"] = merged.get_agg()["merged"]
    dataset["abandoned"] = abandoned.get_agg()["abandoned"]
    dataset["waiting4reviewer"] = waiting4reviewer.get_agg()["ReviewsWaitingForReviewer"]
    dataset["waiting4submitter"] = waiting4submitter.get_agg()["ReviewsWaitingForSubmitter"]
    dataset["review_time_days_median"] = time2review.get_agg()["review_time_days_median"]
    dataset["review_time_days_avg"] = time2review.get_agg()["review_time_days_avg"]

    return dataset

def serialize_threads(threads, crowded, threads_object):

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

    from threads import Threads
    SetDBChannel(dbcon.user, dbcon.password, dbcon.database)
    threads = Threads(filters.startdate, filters.enddate, dbcon.identities_db)
    top_longest_threads = threads.topLongestThread(10)
    top_longest_threads = serialize_threads(top_longest_threads, False, threads)
    createJSON(top_longest_threads, "./release/mls_top_longest_threads.json")
    createCSV(top_longest_threads, "./release/mls_top_longest_threads.csv")

    top_crowded_threads = threads.topCrowdedThread(10)
    top_crowded_threads = serialize_threads(top_crowded_threads, True, threads)
    createJSON(top_crowded_threads, "./release/mls_top_crowded_threads.json")
    createCSV(top_crowded_threads, "./release/mls_top_crowded_threads.csv")

    return dataset


def parse_urls(urls):
    qs_aux = []
    for url in urls:
        url = url.replace("https://ask.openstack.org/en/question/", "")
        url = url.replace("_", "\_")
        qs_aux.append(url)
    return qs_aux


def qaforums_report(dbcon, filters):
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

    import top_questions_qaforums as top
    tops = top.TopQuestions(dbcon, filters)
    commented = tops.top_commented()
    commented["qid"] = commented.pop("question_identifier")
    # Taking the last part of the URL
    commented["site"] = parse_urls(commented.pop("url"))
    createJSON(commented, "./release/qa_top_questions_commented.json")
    createCSV(commented, "./release/qa_top_questions_commented.csv")

    visited = tops.top_visited()
    visited["qid"] = visited.pop("question_identifier")
    visited["site"] = parse_urls(visited.pop("url"))
    #commented["site"] = commented.pop("url").split("/")[-2:][1:]
    
    createJSON(visited, "./release/qa_top_questions_visited.json")
    createCSV(visited, "./release/qa_top_questions_visited.csv")

    crowded = tops.top_crowded()
    crowded["qid"] = crowded.pop("question_identifier")
    crowded["site"] = parse_urls(crowded.pop("url"))
    createJSON(crowded, "./release/qa_top_questions_crowded.json")
    createCSV(crowded, "./release/qa_top_questions_crowded.csv")

    filters.npeople = 15
    createJSON(tops.top_tags(), "./release/qa_top_tags.json")
    createCSV(tops.top_tags(), "./release/qa_top_tags.csv")
    
    return dataset

def irc_report(dbcon, filters):
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


def general_info(opts, releases, people_out, affs_out):

    # General info from MLS, IRC and QAForums.
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
        filters = MetricFilters("month", startdate, enddate, [], opts.npeople, people_out, affs_out)

        # MLS info
        mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities)
        dataset = mls_report(mls_dbcon, filters)
        emails.append(dataset["sent"])
        emails_senders.append(dataset["senders"])
        emails_senders_init.append(dataset["senders_init"])

        # QAForums info 
        qaforums_dbcon = QAForumsQuery(opts.dbuser, opts.dbpassword, opts.dbqaforums, opts.dbidentities)
        dataset = qaforums_report(qaforums_dbcon, filters)
        questions.append(dataset["questions"])
        answers.append(dataset["answers"])
        comments.append(dataset["comments"])
        qsenders.append(dataset["qsenders"])

        # IRC info
        irc_dbcon = IRCQuery(opts.dbuser, opts.dbpassword, opts.dbirc, opts.dbidentities)
        dataset = irc_report(irc_dbcon, filters)
        irc_sent.append(dataset["sent"])
        irc_senders.append(dataset["senders"])


    labels = ["2013-Q3", "2013-Q4", "2014-Q1", "2014-Q2"]
    barh_chart("Emails sent", labels, emails, "emails")
    createCSV({"labels":labels, "emails":emails}, "./release/emails.csv")
    barh_chart("People sending emails", labels, emails_senders, "emails_senders")
    createCSV({"labels":labels, "senders":emails_senders}, "./release/emails_senders.csv")
    barh_chart("People initiating threads", labels, emails_senders_init, "emails_senders_init")
    createCSV({"labels":labels, "senders":emails_senders_init}, "./release/emails_senders_init.csv")
    barh_chart("Questions", labels, questions, "questions")
    createCSV({"labels":labels, "questions":questions}, "./release/questions.csv")
    barh_chart("Answers", labels, answers, "answers")
    createCSV({"labels":labels, "answers":answers}, "./release/answers.csv")
    barh_chart("Comments", labels, comments, "comments")
    createCSV({"labels":labels, "comments":comments}, "./release/comments.csv")
    barh_chart("People asking Questions", labels, qsenders, "question_senders")
    createCSV({"labels":labels, "senders":qsenders}, "./release/question_senders.csv")
    barh_chart("Messages in IRC channels", labels, irc_sent, "irc_sent")
    createCSV({"labels":labels, "messages":irc_sent}, "./release/irc_sent.csv")
    barh_chart("People in IRC channels", labels, irc_senders, "irc_senders")
    createCSV({"labels":labels, "senders":irc_senders}, "./release/irc_senders.csv")



if __name__ == '__main__':

    locale.setlocale(locale.LC_ALL, 'en_US')

    init_env()

    from metrics import Metrics
    from query_builder import DSQuery, SCMQuery, QAForumsQuery, MLSQuery, SCRQuery, ITSQuery, IRCQuery
    from metrics_filter import MetricFilters
    import scm_metrics as scm
    import qaforums_metrics as qa
    import mls_metrics as mls
    import scr_metrics as scr
    import its_metrics as its
    import irc_metrics as irc
    from GrimoireUtils import createJSON
    from GrimoireSQL import SetDBChannel

    # parse options
    opts = read_options()    

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = build_releases(opts.releases)


    # Projects analysis. This includes SCM, SCR and ITS.
    projects_list = projects(opts.dbuser, opts.dbpassword, opts.dbidentities)
    people_out = ["OpenStack Jenkins","Launchpad Translations on behalf of nova-core","Jenkins","OpenStack Hudson","gerrit2@review.openstack.org","linuxdatacenter@gmail.com","Openstack Project Creator","Openstack Gerrit","openstackgerrit"]
    affs_out = ["-Bot","-Individual","-Unknown"]

    for project in projects_list:
        releases_data = {}
        for release in releases:
            releases_data[release] = {}

            startdate = "'" + release[0] + "'"
            enddate = "'" + release[1] + "'"
            filters = MetricFilters("month", startdate, enddate, ["project", str(project)], opts.npeople,
                                    people_out, affs_out)
            scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
            #SCM report
            dataset = scm_report(scm_dbcon, filters)
            releases_data[release]["scm"] = dataset

            #ITS report
            its_dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
            dataset = its_report(its_dbcon, filters)
            releases_data[release]["its"] = dataset

            #SCR Report
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
        bmi = []
        review_avg = []
        review_median = []
        for release in releases:
            labels.append(release[1])
            #scm
            commits.append(releases_data[release]["scm"]["commits"])
            authors.append(releases_data[release]["scm"]["authors"])
            #its
            opened.append(releases_data[release]["its"]["opened"])
            closed.append(releases_data[release]["its"]["closed"])
            if releases_data[release]["its"]["opened"] > 0:
                bmi.append(float(releases_data[release]["its"]["closed"])/float(releases_data[release]["its"]["opened"]))
            else:
                bmi.append(0)
            #scr
            submitted.append(releases_data[release]["scr"]["submitted"])
            merged.append(releases_data[release]["scr"]["merged"])
            abandoned.append(releases_data[release]["scr"]["abandoned"])
            review_avg.append(releases_data[release]["scr"]["review_time_days_avg"])
            review_median.append(releases_data[release]["scr"]["review_time_days_median"])
        
        labels = ["2013-Q3", "2013-Q4", "2014-Q1", "2014-Q2"]        
        project_name = project.replace(" ", "")
        barh_chart("Commits " + project, labels, commits, "commits"  + project_name)
        createCSV({"labels":labels, "commits":commits}, "./release/commits"+project_name+".csv")
        barh_chart("Authors " + project, labels, authors, "authors" + project_name)
        createCSV({"labels":labels, "authors":authors}, "./release/authors"+project_name+".csv")
        barh_chart("Opened tickets " +  project, labels, opened, "opened" + project_name)
        createCSV({"labels":labels, "opened":opened}, "./release/opened"+project_name+".csv")
        barh_chart("Closed tickets " + project, labels, closed, "closed" + project_name)
        createCSV({"labels":labels, "closed":closed}, "./release/closed"+project_name+".csv")
        barh_chart("Efficiency closing tickets " + project, labels, bmi, "bmi" + project_name)
        createCSV({"labels":labels, "bmi":bmi}, "./release/bmi"+project_name+".csv")
        barh_chart("Submitted reviews " + project, labels, submitted, "submitted_reviews" + project_name)
        createCSV({"labels":labels, "submitted":submitted}, "./release/submitted_reviews"+project_name+".csv")
        barh_chart("Merged reviews " + project, labels, merged, "merged_reviews" + project_name)
        createCSV({"labels":labels, "merged":merged}, "./release/merged"+project_name+".csv")
        barh_chart("Abandoned reviews  " + project, labels, abandoned, "abandoned_reviews" + project_name)
        createCSV({"labels":labels, "abandoned":abandoned}, "./release/abandoned"+project_name+".csv")

        barh_chart("Time to review (median)  " + project, labels, review_median, "timetoreview_median" + project_name)
        createCSV({"labels":labels, "mediantime":review_median}, "./release/timetoreview_median"+project_name+".csv")

        barh_chart("Time to review (mean)  " + project, labels, review_avg, "timetoreview_avg" + project_name)
        createCSV({"labels":labels, "avgtime":review_avg}, "./release/timetoreview_avg"+project_name+".csv")

    # general info: mls, irc and qaforums
    general_info(opts, releases, people_out, affs_out)

