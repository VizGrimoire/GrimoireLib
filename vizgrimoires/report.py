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

import pytz

from datetime import datetime, timedelta

from dateutil import parser

from esquery import ElasticQuery
from metrics.scm_metrics import Commits

# Default values so it works without params
ES_URL = 'http://localhost:9200'
ES_INDEX = 'test'
START = parser.parse('1900-01-01')
END = parser.parse('2100-01-01')

def get_now():
    # Return now datetime with timezome
    return datetime.utcnow().replace(tzinfo=pytz.utc)

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


    def get_metrics(self):
        """ Get the metrics """

        scm_index = 'git_enrich'
        commits = Commits(self.es_url, scm_index)

        logging.info("Commits total: %s", commits.get_agg())
        logging.info("Commits ts: %s", commits.get_ts())
        logging.info("Commits list: %s", commits.get_list('hash'))


    def get_metric_trends(self, period):

        def get_metrics_query():
            return ElasticQuery.get_count(self.start, self.end)

        PERIODS = {'day':1, 'week':7, 'month':30, 'year':365}
        DEFAULT_PERIOD = 'week'


        if period not in PERIODS.keys():
            raise RuntimeError('Period not supported: %s', period)

        offset = PERIODS[period]
        # Last interval metrics
        self.end = get_now()
        self.start = self.end - timedelta(days=offset)
        val_last_period = self.get_metrics_data()['hits']['total']
        # Previous interval metrics
        self.end = self.start
        self.start = self.end - timedelta(days=offset)
        val_previous_period = self.get_metrics_data()['hits']['total']
        trend = val_last_period - val_previous_period
        trend_percent = None
        if val_last_period == 0:
            if val_previous_period > 0:
                trend_percent = -100
            else:
                trend_percent = 0
        else:
            trend_percent = int((trend/val_last_period)*100)

        return trend_percent

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
