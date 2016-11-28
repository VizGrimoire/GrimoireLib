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

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

from dateutil import parser

from metrics.scm_metrics import Commits, Committers, Authors
from metrics.its_metrics import Opened, Closed

# Default values so it works without params
ES_URL = 'http://localhost:9200'
ES_INDEX = 'test'
START = parser.parse('1900-01-01')
END = parser.parse('2100-01-01')

class Report():

    def __init__(self, es_url=ES_URL, start=START, end=END):
        if not es_url:
            es_url=ES_URL
        self.start = start
        self.end = end
        self.es_url = es_url

    def create(self):
        logging.info("Generating the report data ...")

        self.get_metrics()


    def __get_metrics_git(self):
        period = 'quarter'
        scm_index = 'git_enrich'
        commits = Commits(self.es_url, scm_index)
        authors = Authors(self.es_url, scm_index)
        committers = Committers(self.es_url, scm_index)

        logging.info("Commits total: %s", commits.get_agg())
        logging.info("Commits ts: %s", commits.get_ts())
        logging.info("Commits list: %s", commits.get_list())
        logging.info("Commits trend: %s", commits.get_trends(period))
        logging.info("Authors total: %s", authors.get_agg())
        logging.info("Authors ts: %s", authors.get_ts())
        logging.info("Authors list: %s", authors.get_list())
        logging.info("Authors trend: %s", authors.get_trends(period))
        logging.info("Committers total: %s", committers.get_agg())
        logging.info("Committers ts: %s", committers.get_ts())
        logging.info("Committers list: %s", committers.get_list())
        logging.info("Committers trend: %s", committers.get_trends(period))

    def __get_metrics_github_issues(self):
        period = 'quarter'
        github_index = 'github_issues_enrich'
        opened = Opened(self.es_url, github_index)
        closed = Closed(self.es_url, github_index)

        # GitHub Issues
        logging.info("Closed total: %s", closed.get_agg())
        logging.info("Closed ts: %s", closed.get_ts())
        logging.info("Closed list: %s", closed.get_list())
        logging.info("Closed trend: %s", closed.get_trends(period))
        logging.info("Opened total: %s", opened.get_agg())
        logging.info("Opened ts: %s", opened.get_ts())
        logging.info("Opened list: %s", opened.get_list())
        logging.info("Opened trend: %s", opened.get_trends(period))


    def get_metrics(self):
        """ Get the metrics """

        self.__get_metrics_git()
        self.__get_metrics_github_issues()



def get_params():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true')
    parser.add_argument('-e', '--elastic-url', help="Elastic URL to get alerts from")

    args = parser.parse_args()

    return args

if __name__ == '__main__':

    args = get_params()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    elastic = args.elastic_url

    report = Report(elastic)
    report.create()
