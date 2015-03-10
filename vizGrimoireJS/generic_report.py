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
## This scripts aims at providing an easy way to obtain some figures and json/csv files
## for a set of basic metrics per data source.
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
        plt.legend(legend)

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
    parser.add_option("--dir",
                      action="store",
                      dest="output_dir",
                      default="./report/")
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

def scm_report(dbcon, filters, output_dir):
    # Basic activity and community metrics in source code
    # management systems

    dataset = {}

    from vizgrimoire.analysis.onion_model import CommunityStructure
    onion = CommunityStructure(dbcon, filters)
    result = onion.result()

    dataset["scm_core"] = result["core"]
    dataset["scm_regular"] = result["regular"]
    dataset["scm_occasional"] = result["occasional"]

    authors_period = scm.AuthorsPeriod(dbcon, filters)
    dataset["scm_authorsperiod"] = float(authors_period.get_agg()["avg_authors_month"])

    authors = scm.Authors(dbcon, filters)
    top_authors = authors.get_list()
    createJSON(top_authors, output_dir + "scm_top_authors.json")
    createCSV(top_authors, output_dir + "scm_top_authors.csv")

    commits = scm.Commits(dbcon, filters)
    dataset["scm_commits"] = commits.get_agg()["commits"] 

    authors = scm.Authors(dbcon, filters)
    dataset["scm_authors"] = authors.get_agg()["authors"]

    #companies = scm.Companies(dbcon, filters)
    #top_companies = companies.get_list(filters)
    #createJSON()
    #createCSV()

    return dataset

def its_report(dbcon, filters):
    # basic metrics for ticketing systems

    dataset = {}

    from vizgrimoire.ITS import ITS
    ITS.set_backend("launchpad")

    opened = its.Opened(dbcon, filters)
    dataset["its_opened"] = opened.get_agg()["opened"]

    closed = its.Closed(dbcon, filters)
    dataset["its_closed"] = closed.get_agg()["closed"]

    return dataset


def scr_report(dbcon, filters):
    # review system basic set of metrics

    dataset = {}

    submitted = scr.Submitted(dbcon, filters)
    dataset["scr_submitted"] = submitted.get_agg()["submitted"]

    merged = scr.Merged(dbcon, filters)
    dataset["scr_merged"] = merged.get_agg()["merged"]

    abandoned = scr.Abandoned(dbcon, filters)
    dataset["scr_abandoned"] = abandoned.get_agg()["abandoned"]

    waiting4reviewer = scr.ReviewsWaitingForReviewer(dbcon, filters)
    dataset["scr_waiting4reviewer"] = waiting4reviewer.get_agg()["ReviewsWaitingForReviewer"]

    waiting4submitter = scr.ReviewsWaitingForSubmitter(dbcon, filters)
    dataset["scr_waiting4submitter"] = waiting4submitter.get_agg()["ReviewsWaitingForSubmitter"]

    filters.period = "month"
    time2review = scr.TimeToReview(dbcon, filters)
    dataset["scr_review_time_days_median"] = round(time2review.get_agg()["review_time_days_median"], 2)
    dataset["scr_review_time_days_avg"] = round(time2review.get_agg()["review_time_days_avg"], 2)

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

def mls_report(dbcon, filters, output_dir):
    
    dataset = {}

    emails = mls.EmailsSent(dbcon, filters)
    dataset["mls_sent"] = emails.get_agg()["sent"]

    senders = mls.EmailsSenders(dbcon, filters)
    dataset["mls_senders"] = senders.get_agg()["senders"]

    senders_init = mls.SendersInit(dbcon, filters)
    dataset["mls_senders_init"] = senders_init.get_agg()["senders_init"]

    from vizgrimoire.analysis.threads import Threads
    SetDBChannel(dbcon.user, dbcon.password, dbcon.database)
    threads = Threads(filters.startdate, filters.enddate, dbcon.identities_db)
    top_longest_threads = threads.topLongestThread(10)
    top_longest_threads = serialize_threads(top_longest_threads, False, threads)
    createJSON(top_longest_threads, output_dir + "/mls_top_longest_threads.json")
    createCSV(top_longest_threads, output_dir + "/mls_top_longest_threads.csv")

    top_crowded_threads = threads.topCrowdedThread(10)
    top_crowded_threads = serialize_threads(top_crowded_threads, True, threads)
    createJSON(top_crowded_threads, output_dir + "/mls_top_crowded_threads.json")
    createCSV(top_crowded_threads, output_dir + "/mls_top_crowded_threads.csv")

    return dataset


def parse_urls(urls):
    qs_aux = []
    for url in urls:
        url = url.replace("https://ask.openstack.org/en/question/", "")
        url = url.replace("_", "\_")
        qs_aux.append(url)
    return qs_aux


def qaforums_report(dbcon, filters, output_dir):
    # basic metrics for qaforums

    dataset = {}

    questions = qa.Questions(dbcon, filters)
    dataset["qa_questions"] = questions.get_agg()["qsent"]

    answers = qa.Answers(dbcon, filters)
    dataset["qa_answers"] = answers.get_agg()["asent"]

    comments = qa.Comments(dbcon, filters)
    dataset["qa_comments"] = comments.get_agg()["csent"]

    q_senders = qa.QuestionSenders(dbcon, filters)
    dataset["qa_qsenders"] = q_senders.get_agg()["qsenders"]

    import vizgrimoire.analysis.top_questions_qaforums as top
    tops = top.TopQuestions(dbcon, filters)
    commented = tops.top_commented()
    commented["qid"] = commented.pop("question_identifier")
    # Taking the last part of the URL
    commented["site"] = parse_urls(commented.pop("url"))
    createJSON(commented, output_dir + "/qa_top_questions_commented.json")
    createCSV(commented, output_dir + "/qa_top_questions_commented.csv")

    visited = tops.top_visited()
    visited["qid"] = visited.pop("question_identifier")
    visited["site"] = parse_urls(visited.pop("url"))
    createJSON(visited, output_dir + "/qa_top_questions_visited.json")
    createCSV(visited, output_dir + "/qa_top_questions_visited.csv")

    crowded = tops.top_crowded()
    crowded["qid"] = crowded.pop("question_identifier")
    crowded["site"] = parse_urls(crowded.pop("url"))
    createJSON(crowded, output_dir + "/qa_top_questions_crowded.json")
    createCSV(crowded, output_dir + "./qa_top_questions_crowded.csv")

    filters.npeople = 15
    createJSON(tops.top_tags(), output_dir + "/qa_top_tags.json")
    createCSV(tops.top_tags(), output_dir + "/qa_top_tags.csv")
    
    return dataset

def irc_report(dbcon, filters, output_dir):
    # irc basic report

    dataset = {}

    sent = irc.Sent(dbcon, filters)
    dataset["irc_sent"] = sent.get_agg()["sent"]

    senders = irc.Senders(dbcon, filters)
    dataset["irc_senders"] = senders.get_agg()["senders"]

    top_senders = senders.get_list()
    createJSON(top_senders, output_dir + "/irc_top_senders.json")
    createCSV(top_senders, output_dir  + "/irc_top_senders.csv")

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


def draw(dataset, labels, output_dir):
    # create charts and write down csv files with list of metrics

    for metric in dataset.keys():
        bar_chart(metric, labels, dataset[metric], output_dir + "/" + metric)


def update_data(data, dataset):
    # dataset is a list of metrics eg: {"metric":value2}
    # data contains the same list of metrics, but with previous information
    #  eg: {"metric":[value0, value1]}
    # the output would be: {"metric":[value0, value1, value2]}
         
    for metric in dataset.keys():
        if data.has_key(metric):
            data[metric].append(dataset[metric])
        else:
            data[metric] = [dataset[metric]]

    return data

if __name__ == '__main__':

    locale.setlocale(locale.LC_ALL, 'en_US')

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

    # parse options
    opts = read_options()    

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = build_releases(opts.releases)

    # Projects analysis. This includes SCM, SCR and ITS.
    people_out = ["OpenStack Jenkins","Launchpad Translations on behalf of nova-core","Jenkins","OpenStack Hudson","gerrit2@review.openstack.org","linuxdatacenter@gmail.com","Openstack Project Creator","Openstack Gerrit","openstackgerrit"]
    affs_out = ["-Bot","-Individual","-Unknown"]

    labels = ["2012-S2", "2013-S1", "2013-S2", "2014-S1"]

    data = {}
    for release in releases:

        dataset = {}
        startdate = "'" + release[0] + "'"
        enddate = "'" + release[1] + "'"
        filters = MetricFilters("month", startdate, enddate, [], opts.npeople, people_out, affs_out)

        if opts.dbcvsanaly is not None:
            dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
            dataset.update(scm_report(dbcon, filters, opts.output_dir))

        if opts.dbbicho is not None:
            dbcon = ITSQuery(opts.dbuser, opts.dbpassword, opts.dbbicho, opts.dbidentities)
            dataset.update(its_report(dbcon, filters))

        if opts.dbreview is not None:
            dbcon = SCRQuery(opts.dbuser, opts.dbpassword, opts.dbreview, opts.dbidentities)
            dataset.update(scr_report(dbcon, filters))

        if opts.dbirc is not None:
            dbcon = IRCQuery(opts.dbuser, opts.dbpassword, opts.dbirc, opts.dbidentities)
            dataset.update(irc_report(dbcon, filters, opts.output_dir))
  
        if opts.dbqaforums is not None:
            dbcon = QAForumsQuery(opts.dbuser, opts.dbpassword, opts.dbqaforums, opts.dbidentities)
            dataset.update(qaforums_report(dbcon, filters, opts.output_dir))

        if opts.dbmlstats is not None:
            dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities)
            dataset.update(mls_report(dbcon, filters, opts.output_dir))

        data = update_data(data, dataset)

    createJSON(dataset, opts.output_dir + "/report.json")
    draw(data, labels, opts.output_dir)
    
