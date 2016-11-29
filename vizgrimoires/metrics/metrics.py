#!/usr/bin/python3
## Copyright (C) 2016 Bitergia
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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>

import pytz

import requests

from datetime import datetime, timedelta

from esquery import ElasticQuery

class Metrics(object):
    """Root of hierarchy of Entities (Metrics)

    This is the root hierarchy of the of the metric classes
    defined for each of the data sources.

    When instantiated Metrics, we can obtain specific representations
    of such entity. Timeseries of datasets (get_ts method), aggregated
    data (get_agg method) or lists of elements (get_list method).

    """

    id = None
    name = None
    desc = None
    filters = None
    FIELD_DATE='metadata__updated_on'


    def __init__(self, es_url, es_index, start=None, end=None, esfilter=None):
        """es connection and filter to be used"""
        self.es_url = es_url
        self.es_index = es_index
        self.start = start
        self.end = end
        self.filter = esfilter

    def get_definition(self):
        def_ = {
               "id":self.id,
               "name":self.name,
               "desc":self.desc
        }
        return def_

    def get_query(self, evolutionary=False):
        """Private method that returns a valid ElasticSearch query.


        :evolutionary: boolean
            If True, an evolutionary analysis sql is provided
            If False, an aggregated analysis sql is provided

        """
        raise NotImplementedError

    def get_metrics_data(self, query):
        """ Get the metrics data from ES """
        url = self.es_url+'/' + self.es_index + '/_search'
        r = requests.post(url, query)
        r.raise_for_status()
        return r.json()


    def get_ts (self):
        """Returns a time series of a specific class

        A timeseries consists of a unixtime date, labels, some other
        fields and the data of the specific instantiated class per
        period. This is built on a hash table.

        """
        query = self.get_query(True)
        res = self.get_metrics_data(query)
        # Time to convert it to our grimoire timeseries format
        ts = {"date":[],"value":[],"unixtime":[]}
        for bucket in res['aggregations'][str(ElasticQuery.AGGREGATION_ID)]['buckets']:
            ts['date'].append(bucket['key_as_string'])
            if str(ElasticQuery.AGGREGATION_ID+1) in bucket:
                # We have a subaggregation with the value
                ts['value'].append(bucket[str(ElasticQuery.AGGREGATION_ID+1)]['value'])
            else:
                ts['value'].append(bucket['doc_count'])
            # unixtime comes in ms from ElasticSearch
            ts['unixtime'].append(bucket['key']/1000)
        return ts

    def get_agg(self):
        """ Returns an aggregated value """
        query = self.get_query(False)
        res = self.get_metrics_data(query)
        # We need to extract the data from the JSON res
        # If we have agg data use it
        if 'aggregations' in res and res['aggregations'][str(ElasticQuery.AGGREGATION_ID)]['value']:
            agg = res['aggregations'][str(ElasticQuery.AGGREGATION_ID)]['value']
        else:
            agg = res['hits']['total']

        return agg

    def get_trends(self, period='quarter'):

        PERIODS = {'day':1, 'week':7, 'month':30, 'quarter':90, 'year':365}

        if period not in PERIODS.keys():
            raise RuntimeError('Period not supported: %s' % period)

        offset = PERIODS[period]
        # Last interval metrics
        end = datetime.utcnow().replace(tzinfo=pytz.utc)
        start = end - timedelta(days=offset)

        last_commits = type(self)(self.es_url, self.es_index, start, end)
        val_last_period = last_commits.get_agg()
        # Previous interval metrics
        end = start
        start = end - timedelta(days=offset)
        prev_commits = type(self)(self.es_url, self.es_index, start, end)
        val_previous_period = prev_commits.get_agg()
        trend = val_last_period - val_previous_period
        trend_percent = None
        if val_last_period == 0:
            if val_previous_period > 0:
                trend_percent = -100
            else:
                trend_percent = 0
        else:
            trend_percent = int((trend/val_last_period)*100)

        return (val_last_period, trend_percent)


    def get_list(self, field, filters=None):
        query = ElasticQuery.get_agg_count(field, filters=filters)
        res = self.get_metrics_data(query)
        l = {field:[],"value":[]}
        for bucket in res['aggregations'][str(ElasticQuery.AGGREGATION_ID)]['buckets']:
            l[field].append(bucket['key'])
            l['value'].append(bucket['doc_count'])
        return l
