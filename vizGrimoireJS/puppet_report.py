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
## example: PYTHONPATH=./:../vizgrimoire/metrics/:../vizgrimoire/analysis/:../vizgrimoire/ python puppet_report.py  -a lcanas_cvsanaly_puppetlabs_copy -i lcanas_cvsanaly_puppetlabs_copy -f acs_sibyl_puppetlabs_copy -b lcanas_mlstats_puppetlabs_copy -u root --dbpassword="" -r 2014-04-01,2014-07-01 -g week

from optparse import OptionParser
from os import listdir
from os.path import isfile, join
import imp, inspect

from metrics import Metrics
from query_builder import DSQuery, SCMQuery, QAForumsQuery, MLSQuery
from metrics_filter import MetricFilters
import scm_metrics as scm
import qaforums_metrics as qa
import mls_metrics as mls
from GrimoireUtils import createJSON
from GrimoireSQL import SetDBChannel


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
    print(commits.get_agg())

    authors = scm.Authors(dbcon, filters)
    createJSON(authors.get_agg(), "./release/scm_authors.json")
    print(authors.get_agg())

    from contributors_new_gone import ContributorsNewGoneSCM
    from SCM import SCM
    newcommers_leavers = ContributorsNewGoneSCM(dbcon, filters)
    newcommers_leavers.result(SCM, "./release/")

    from SCM import top_people
    top_authors = {}
    bots = SCM.get_bots() 
    SetDBChannel(dbcon.user, dbcon.password, dbcon.database)
    top_authors["authors"] =  top_people(90, filters.startdate, filters.enddate, "author", bots, str(filters.npeople))
    createJSON(top_authors, "./release/scm_top_authors.json")
    createCSV(top_authors["authors"], "./release/scm_top_authors.csv")

    from SCM import repos_name
    top_repos = repos_name(filters.startdate, filters.enddate)
    createJSON(top_repos, "./release/scm_top_repositories.json")
    createCSV(top_repos, "./release/scm_top_repositories.csv")

def qaforums_report(dbcon, filters):
    questions = qa.Questions(dbcon, filters)
    createJSON(questions.get_agg(), "./release/qaforums_questions.json")

    answers = qa.Answers(dbcon, filters)
    createJSON(answers.get_agg(), "./release/qaforums_answers.json")

    comments = qa.Comments(dbcon, filters)
    createJSON(comments.get_agg(), "./release/qaforums_answers.json")

    senders = qa.QuestionSenders(dbcon, filters)
    createJSON(senders.get_agg(), "./release/qaforums_people_posting_questions.json")

    people_replying = qa.AnswerSenders(dbcon, filters)
    createJSON(people_replying.get_agg(), "./release/qaforums_people_posting_answers.json")

    from top_questions_qaforums import TopQuestions
    top_questions = TopQuestions(dbcon, filters)

    top_visited_questions = top_questions.top_visited()
    createCSV(top_visited_questions, "./release/qaforums_top_visited_questions.csv", ['question_identifier'])
    createJSON(top_visited_questions, "./release/qaforums_top_visited_questions.json")    
    
    top_commented_questions = top_questions.top_commented()
    createCSV(top_commented_questions, "./release/qaforums_top_commented_questions.csv", ['question_identifier'])
    createJSON(top_commented_questions, "./release/qaforums_top_commented_questions.json")

    top_crowded_questions = top_questions.top_crowded()
    createCSV(top_crowded_questions, "./release/qaforums_top_crowded_questions.csv")
    createJSON(top_crowded_questions, "./release/qaforums_top_crowded_questions.json",['question_identifier'])

    from top_qaforums import TopQAForums
    top_participants = TopQAForums(dbcon, filters)
    createJSON(top_participants.result(), "./release/qaforums_top_participants.json")
    createCSV(top_participants.result(), "./release/qaforums_top_participants.csv",['id'])

def mls_report(dbcon, filters):

    emails_sent = mls.EmailsSent(dbcon, filters)
    createJSON(emails_sent.get_agg(), "./release/mls_emailsent.json")
    print(emails_sent.get_agg())

    emails_senders = mls.EmailsSenders(dbcon, filters)
    createJSON(emails_senders.get_agg(), "./release/mls_emailsenders.json")
    print(emails_senders.get_agg())

    from MLS import MLS
    from MLS import top_senders
    bots = []
    SetDBChannel(dbcon.user, dbcon.password, dbcon.database)
    top = {}
    top["EmailSenders"] = top_senders(90, filters.startdate, filters.enddate, dbcon.identities_db,bots, str(filters.npeople))
    createJSON(top, "./release/mls_top_email_senders.json")
    createCSV(top["EmailSenders"], "./release/mls_top_email_senders.csv")

    from threads import Threads
    SetDBChannel(dbcon.user, dbcon.password, dbcon.database)
    top_threads = {}
    top_threads['threads'] = MLS.getLongestThreads(filters.startdate, filters.enddate, dbcon.identities_db, str(filters.npeople))
    createJSON(top_threads, "./release/mls_top_longest_threads.json")
    #createCSV(top_threads["threads"], "./release/mls_top_longest_threads.csv")

    main_topics= Threads(filters.startdate, filters.enddate, dbcon.identities_db)
    top_crowded = main_topics.topCrowdedThread(filters.npeople)
    l_threads = {}
    l_threads['message_id'] = []
    l_threads['people'] = []
    l_threads['subject'] = []
    l_threads['date'] = []
    l_threads['initiator_name'] = []
    l_threads['initiator_id'] = []
    l_threads['url'] = []
    for email_people in top_crowded:
        email = email_people[0]
        l_threads['message_id'].append(email.message_id)
        l_threads['people'].append(email_people[1])
        l_threads['subject'].append(email.subject)
        l_threads['date'].append(email.date.strftime("%Y-%m-%d"))
        l_threads['initiator_name'].append(email.initiator_name)
        l_threads['initiator_id'].append(email.initiator_id)
        l_threads['url'].append(email.url)
    createJSON(l_threads, "./release/mls_top_crowded_threads.json")
    createCSV(l_threads, "./release/mls_top_crowded_threads.csv")
   
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
            body += unicode(data[k][cont])
            body += u','
        body = body[:-1]
        body += u'\n'
        cont += 1
    fd.write(header.encode('utf-8'))
    fd.write('\n')
    fd.write(body.encode('utf-8'))
    fd.close()
    print "CSV file generated at: %s" % (filepath)


if __name__ == '__main__':

    # parse options
    opts = read_options()    

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = build_releases(opts.releases)

    for release in releases:
        startdate = "'" + release[0] + "'"
        enddate = "'" + release[1] + "'"
        filters = MetricFilters("month", startdate, enddate, []) 
        scm_dbcon = SCMQuery(opts.dbuser, opts.dbpassword, opts.dbcvsanaly, opts.dbidentities)
        #SCM report
        scm_report(scm_dbcon, filters)

        #QAForums report
        qa_dbcon = QAForumsQuery(opts.dbuser, opts.dbpassword, opts.dbqaforums, opts.dbidentities)
        qaforums_report(qa_dbcon, filters)

        #MLS Report
        mls_dbcon = MLSQuery(opts.dbuser, opts.dbpassword, opts.dbmlstats, opts.dbidentities)
        mls_report(mls_dbcon, filters)



