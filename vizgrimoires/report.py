#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Helper classes for doing queries to ElasticSearch
#
# Copyright (C) 2016 Bitergia
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
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import logging
import os

import matplotlib as mpl
# This avoids the use of the $DISPLAY value for the charts
mpl.use('Agg')
import matplotlib.pyplot as plt
import prettyplotlib as ppl
import numpy as np

from collections import OrderedDict

from dateutil import parser
from datetime import datetime

from metrics.scm_metrics import Commits, Committers, Authors, ProjectsSCM
from metrics.its_metrics import BMITickets, Closed, DaysToClose, Opened, ProjectsITS
from metrics.mls_metrics import EmailsSent, EmailsSenders, ProjectsMLS
from metrics.github_prs_metrics import BMIPR, ClosedPR, DaysToClosePR, ProjectsPR, SubmittedPR

# Default values so it works without params
ES_URL = 'http://localhost:9200'
ES_INDEX = 'test'
START = parser.parse('1900-01-01')
END = parser.parse('2100-01-01')

class Report():
    GIT_INDEX = 'git_enrich'
    GITHUB_INDEX = 'github_issues_enrich'
    EMAIL_INDEX = 'mbox_enrich'

    metric2index = {
        Commits: GIT_INDEX,
        Authors: GIT_INDEX,
        ProjectsSCM: GIT_INDEX,
        BMITickets: GITHUB_INDEX,
        Closed: GITHUB_INDEX,
        DaysToClose: GITHUB_INDEX,
        Opened: GITHUB_INDEX,
        ProjectsITS: GITHUB_INDEX,
        BMIPR: GITHUB_INDEX,
        ClosedPR: GITHUB_INDEX,
        DaysToClosePR: GITHUB_INDEX,
        SubmittedPR: GITHUB_INDEX,
        ProjectsPR: GITHUB_INDEX,
        EmailsSent: EMAIL_INDEX,
        EmailsSenders: EMAIL_INDEX,
        ProjectsMLS: EMAIL_INDEX
    }

    def __init__(self, es_url=ES_URL, start=START, end=END, data_dir=None):
        if not es_url:
            es_url=ES_URL
        self.es_url = es_url
        self.start = start
        self.end = end
        self.data_dir = data_dir

    def get_vis(self):
        logging.info("Generating the vis ...")

        scm_index = 'git_enrich'
        commits = Commits(self.es_url, scm_index, start=self.start)
        authors = Authors(self.es_url, scm_index, start=self.start)

        tsc = commits.get_ts()
        tsa = authors.get_ts()

        self.ts_chart("Commits", tsc['unixtime'], tsc['value'], "commits.eps")
        self.ts_chart("Authors", tsa['unixtime'], tsa['value'], "authors.eps")

        self.bar_chart("Authors", tsa['date'][0:2], tsa['value'][0:2],
                       "authors-bar.eps", tsc['value'][2:4],
                       legend=["authors","commits"])


    def bar3_chart(self, title, labels, data1, file_name, data2, data3, legend=["", ""]):

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


        plt.savefig(file_name)
        plt.close()

    def bar_chart(self, title, labels, data1, file_name, data2 = None, legend=["", ""]):

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

        plt.savefig(file_name)
        plt.close()

    def ts_chart(self, title, unixtime_dates, data, file_name):

        fig = plt.figure()
        plt.title(title)

        dates = []
        for unixdate in unixtime_dates:
            dates.append(datetime.fromtimestamp(float(unixdate)))

        ppl.plot(dates, data)
        fig.autofmt_xdate()
        fig.savefig(file_name)

    def __get_metrics_git(self):
        interval = 'quarter'
        scm_index = 'git_enrich'
        commits = Commits(self.es_url, scm_index, start=self.start)
        authors = Authors(self.es_url, scm_index, start=self.start)
        committers = Committers(self.es_url, scm_index, start=self.start)

        logging.info("Commits total: %s", commits.get_agg())
        logging.info("Commits ts: %s", commits.get_ts())
        logging.info("Commits list: %s", commits.get_list())
        logging.info("Commits trend: %s", commits.get_trends(interval))
        logging.info("Authors total: %s", authors.get_agg())
        logging.info("Authors ts: %s", authors.get_ts())
        logging.info("Authors list: %s", authors.get_list())
        logging.info("Authors trend: %s", authors.get_trends(interval))
        logging.info("Committers total: %s", committers.get_agg())
        logging.info("Committers ts: %s", committers.get_ts())
        logging.info("Committers list: %s", committers.get_list())
        logging.info("Committers trend: %s", committers.get_trends(interval))

    def __get_metrics_github_issues(self):
        interval = 'quarter'
        github_index = 'github_issues_enrich'
        opened = Opened(self.es_url, github_index, start=self.start)
        closed = Closed(self.es_url, github_index, start=self.start)

        # GitHub Issues
        logging.info("Closed total: %s", closed.get_agg())
        logging.info("Closed ts: %s", closed.get_ts())
        logging.info("Closed list: %s", closed.get_list())
        logging.info("Closed trend: %s", closed.get_trends(interval))
        logging.info("Opened total: %s", opened.get_agg())
        logging.info("Opened ts: %s", opened.get_ts())
        logging.info("Opened list: %s", opened.get_list())
        logging.info("Opened trend: %s", opened.get_trends(interval))


    def __get_metrics_github_prs(self):
        interval = 'quarter'
        github_index = 'github_issues_enrich'
        submitted = SubmittedPR(self.es_url, github_index, start=self.start)
        closed = ClosedPR(self.es_url, github_index, start=self.start)

        # GitHub Issues
        logging.info("Closed total: %s", closed.get_agg())
        logging.info("Closed ts: %s", closed.get_ts())
        logging.info("Closed list: %s", closed.get_list())
        logging.info("Closed trend: %s", closed.get_trends(interval))
        logging.info("Submitted total: %s", submitted.get_agg())
        logging.info("Submitted ts: %s", submitted.get_ts())
        logging.info("Submitted list: %s", submitted.get_list())
        logging.info("Submitted trend: %s", submitted.get_trends(interval))


    def __get_metrics_emails(self):
        interval = 'quarter'
        mbox_index = 'mbox_enrich'
        sent = EmailsSent(self.es_url, mbox_index, start=self.start)
        senders = EmailsSenders(self.es_url, mbox_index, start=self.start)

        # GitHub Issues
        logging.info("Sent total: %s", sent.get_agg())
        logging.info("Sent ts: %s", sent.get_ts())
        logging.info("Sent list: %s", sent.get_list())
        logging.info("Sent trend: %s", sent.get_trends(interval))
        logging.info("Senders total: %s", senders.get_agg())
        logging.info("Senders ts: %s", senders.get_ts())
        logging.info("Senders list: %s", senders.get_list())
        logging.info("Senders trend: %s", senders.get_trends(interval))


    def get_metrics(self):
        """ Get the metrics """

        self.__get_metrics_git()
        self.__get_metrics_github_issues()
        self.__get_metrics_emails()
        self.__get_metrics_github_prs()

    def __get_trend_percent(self, last, prev):
        trend = last - prev
        trend_percent = None
        if last == 0:
            if prev > 0:
                trend_percent = -100
            else:
                trend_percent = 0
        else:
            trend_percent = int((trend/last)*100)

        return trend_percent

    def get_metric_index(self, metric_cls):
        return self.metric2index[metric_cls]

    def get_es_indexes(self, metric_cls):
        indexes = []
        for metric in self.metric2index:
            if self.metric2index[metric] not in indexes:
                indexes.append(self.metric2index[metric])
        return indexes

    def get_metric_ds(self, metric_cls):
        metric2ds = {
            Commits: "Git",
            Authors: "Git",
            Closed: "GitHub",
            Opened: "GitHub",
            ClosedPR: "GitHub",
            SubmittedPR: "GitHub",
            EmailsSent: "MailingList"
        }

        return metric2ds[metric_cls]


    def sec_overview(self):
        # Overview: Activity and Authors

        """
        Activity during the last 90 days and its evolution

        type: CSV
        file_name: data_source_evolution.csv
        columns: metricsnames, netvalues, relativevalues, datasource
        description: for the specified data source, we need the main activity
        metrics. This is the comparison of the last interval of
        analysis (in the example 90 days) with the previous 90 days. Net values
        are the total numbers, while the change is the percentage of such
        number if compared to the previous interval of analysis.
        commits, closed tickets, sent tickets, closed pull requests,
        open pull requests, sent emails
        """

        metrics = [Commits, Closed, Opened, ClosedPR, SubmittedPR, EmailsSent]

        file_name = os.path.join(self.data_dir, 'data_source_evolution.csv')

        csv = 'metricsnames,netvalues,relativevalues,datasource\n'
        for metric in metrics:
            # commits comparing current month with previous month
            es_index = self.get_metric_index(metric)
            ds = self.get_metric_ds(metric)
            m = metric(self.es_url, es_index, interval='month', start=self.start)
            ts = m.get_ts()
            last = ts['value'][len(ts['value'])-1]
            prev = ts['value'][len(ts['value'])-2]
            csv += "%s,%i,%i,%s" % (metric.name, last,
                                    self.__get_trend_percent(last, prev),
                                    ds)
            csv += "\n"
        with open(file_name, "w") as f:
            f.write(csv)

        """
        Authors per month:

        type: EPS
        file_name: authors_month.eps
        description: average number of developers per month by quarters
        (so we have the average number of developers per month during
        those three months). If the approach is to work at the level of month,
        then just the number of developers per month.

        type: CSV
        file_name: authors_month.csv
        columns: labels,authormonth
        description: same as above
        """

        authors = Authors(self.es_url, self.get_metric_index(Authors),
                          interval='month', start=self.start)
        tsa = authors.get_ts()
        file_name = os.path.join(self.data_dir, "authors_month.eps")
        x_val = [parser.parse(val).strftime("%Y-%m") for val in tsa['date']]
        self.bar_chart("Authors", x_val, tsa['value'],
                       file_name, legend=["Authors"])

        csv = 'labels,authormonth\n'
        for i in range(0, len(tsa['value'])):
            date_str = parser.parse(tsa['date'][i]).strftime("%Y-%m")
            csv += date_str+","+str(tsa['value'][i])+"\n"
        file_name = os.path.join(self.data_dir, 'authors_month.csv')
        with open(file_name, "w") as f:
            f.write(csv)

        """
        Process
        Closed changesets out of opened changesets (REI), closed ticket out
        of opened tickets (BMI) and median time to merge in Gerrit (TTM)
        (Potential title: Project Efficiency)
        type:CSV
        file_name: efficiency.csv
        description: Closed PRs / Open PRs, Closed Issues / Open Issues,
        median time to close a PR, median time to close an Issue
        """

        bmi_p = BMIPR(self.es_url, self.get_metric_index(BMIPR),
                      interval='month', start=self.start).get_agg()
        bmi_t = BMITickets(self.es_url, self.get_metric_index(BMITickets),
                           interval='month', start=self.start).get_agg()

        ttc_p = DaysToClosePR(self.es_url, self.get_metric_index(DaysToClosePR),
                              interval='month', start=self.start).get_agg()
        ttc_t = DaysToClose(self.es_url, self.get_metric_index(DaysToClosePR),
                            interval='month', start=self.start).get_agg()

        csv = 'bmipullrequest,bmiissues,timeclosepullrequets,timecloseissues\n'
        csv += "%0.2f,%0.2f,%0.2f,%0.2f" % (bmi_p, bmi_t, ttc_p, ttc_t)
        file_name = os.path.join(self.data_dir, 'efficiency.csv')
        with open(file_name, "w") as f:
            f.write(csv)

    def sec_com_channels(self):
        """
        Emails:
            type: EPS
            file_name: emails.eps
            description: number of emails sent per period of analysis
            type: CSV
            file_name: emails.csv
            columns: labels,emails
        """
        emails = EmailsSent(self.es_url, self.get_metric_index(EmailsSent),
                           interval='month', start=self.start)
        ts = emails.get_ts()
        file_name = os.path.join(self.data_dir, 'emails.eps')
        x_val = [parser.parse(val).strftime("%Y-%m") for val in ts['date']]
        self.bar_chart("Emails", x_val, ts['value'], file_name)
        csv = 'labels,emails\n'
        for i in range(0, len(ts['value'])):
            date_str = parser.parse(ts['date'][i]).strftime("%Y-%m")
            csv += date_str+","+str(ts['value'][i])+"\n"
        file_name = os.path.join(self.data_dir, 'emails.csv')
        with open(file_name, "w") as f:
            f.write(csv)

        """
        Email Senders:

            type: EPS
            file_name: emails_senders.eps
            description: number of people sending emails per period of analysis
            type: CSV
            file_name: emails_senders.csv
            columns: labels,emails
        """
        emails = EmailsSenders(self.es_url, self.get_metric_index(EmailsSenders),
                               interval='month', start=self.start)
        ts = emails.get_ts()
        file_name = os.path.join(self.data_dir, 'emails_senders.eps')
        x_val = [parser.parse(val).strftime("%Y-%m") for val in ts['date']]
        self.bar_chart("Email Senders", x_val, ts['value'], file_name)
        csv = 'labels,senders\n'
        for i in range(0, len(ts['value'])):
            date_str = parser.parse(ts['date'][i]).strftime("%Y-%m")
            csv += date_str+","+str(ts['value'][i])+"\n"
        file_name = os.path.join(self.data_dir, 'emails_senders.csv')
        with open(file_name, "w") as f:
            f.write(csv)

    def sec_project_activity(self, project=None):
        """
        Activity

        Commits and Pull Requests:

        type: EPS
        file_name: commitsreviews<project_name>.eps
        description: number of commits and pull requests per project
        type: CSV
        file_name: commitsreviews<project_name>.csv
        columns: commits,labels,submitted
        """

        commits = Commits(self.es_url, self.get_metric_index(Commits),
                          interval='month', esfilters={"project": project},
                          start=self.start)
        commits_agg = commits.get_agg()

        submitted = SubmittedPR(self.es_url, self.get_metric_index(SubmittedPR),
                                interval='month', esfilters={"project": project},
                                start=self.start)
        submitted_agg = submitted.get_agg()

        closed = ClosedPR(self.es_url, self.get_metric_index(ClosedPR),
                                interval='month', esfilters={"project": project},
                                start=self.start)
        closed_agg = closed.get_agg()


        print(project, commits_agg, submitted_agg, closed_agg)


        # self.ts_chart("Commits " + project, ts['unixtime'], ts['value'],
        #               "commits"+project+".eps")
        # csv = 'labels,commits\n'
        # for i in range(0, len(ts['value'])):
        #     csv += ts['date'][i]+","+str(ts['value'][i])+"\n"
        # with open("emails.csv", "w") as f:
        #     f.write(csv)


        """
        Opened and Closed Pull Requests:

        type: EPS
        file_name: openedclosed_pullrequests_<project_name>.eps
        description: number of opened and closed pull requests per project
        type: CSV
        file_name: openedclosed_pullrequests_<project_name>.csv
        columns: labels,opened,closed
        """

        """
        Opened and Closed Issues:

        type: EPS
        file_name: openedclosed_issues_<project_name>.eps
        description: number of opened and closed issues per project
        type: CSV
        file_name: openedclosed_issues_<project_name>.csv
        columns: labels,opened,closed
        """

    def sec_project_community(self, project=None):
        """
        Main organizations

        type: CSV
        file_name: scm_top_organizations<project_name>.csv
        columns: organization_commits,name
        """
        commits = Commits(self.es_url, self.get_metric_index(Commits),
                          esfilters={"project": project}, start=self.start)
        commits.FIELD_NAME = 'author_org_name'

        top = commits.get_list()


    def sec_project_process(self, project=None):
        """
        BMI Pull Requests

        type: EPS
        file_name: bmi_prs<project_name>.eps
        description: closed PRs out of open PRs in a period of time
        type: CSV
        file_name: bmi_prs<project_name>.csv
        columns: bmi,labels
        """

        bmipr = BMIPR(self.es_url, self.get_metric_index(BMIPR),
                      interval='month', esfilters={"project": project},
                      start=self.start)

        ts = bmipr.get_ts()
        agg = bmipr.get_agg()


    def sec_projects(self):
        """
        This activity is displayed at the general level, aggregating all
        of the projects, with the name 'general' and per project using
        the name of each project. This activity is divided into three main
        layers: activity, community and process.
        """

        #
        # "general" project sectios
        #

        #
        # per project sectios
        #



        # Just one level projects supported yet

        # First we need to get the list of projects per data source and
        # join all lists in the overall projects list

        projects_scm = ProjectsSCM(self.es_url,
                                   self.get_metric_index(ProjectsSCM),
                                   start=self.start).get_list()['project']
        projects_its = ProjectsITS(self.es_url,
                                   self.get_metric_index(ProjectsITS),
                                   start=self.start).get_list()['project']
        projects_pr = ProjectsPR(self.es_url,
                                 self.get_metric_index(ProjectsPR),
                                 start=self.start).get_list()['project']
        projects_mls = ProjectsMLS(self.es_url,
                                   self.get_metric_index(ProjectsMLS),
                                   start=self.start).get_list()['project']

        projects = list(set(projects_scm+projects_its+projects_pr+projects_mls))

        for project in projects:
            self.sec_project_activity(project)
            self.sec_project_community(project)
            self.sec_project_process(project)


    def sections(self):
        secs = OrderedDict()
        secs['Overview'] = self.sec_overview
        secs['Communication Channels'] = self.sec_com_channels
        # secs['Detailed Activity by Project'] = self.sec_projects

        return secs

    def create(self):
        logging.info("Generating the report data ...")

        # self.get_metrics()
        # self.get_vis()

        for section in self.sections():
            logging.info("Generating %s", section)
            self.sections()[section]()

        logging.info("Done")


def get_params():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true')
    parser.add_argument('-e', '--elastic-url', help="Elastic URL to get alerts from")
    parser.add_argument('-d', '--data-dir', default='data',
                        help="Directory to store the data results")
    parser.add_argument('-s', '--start-date', default='2015-01-01',
                        help="Start date for the report")

    return parser.parse_args()

if __name__ == '__main__':

    args = get_params()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    elastic = args.elastic_url
    data_dir = args.data_dir

    report = Report(elastic, start=parser.parse(args.start_date),
                    data_dir=data_dir)
    report.create()
