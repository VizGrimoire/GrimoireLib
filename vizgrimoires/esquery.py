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

DEFAULT_TS_FIELD = 'metadata__updated_on'

class ElasticQuery():
    """ Helper class for building Elastic queries """

    AGGREGATION_ID = 1
    AGG_SIZE = 100

    @classmethod
    def get_query_range(cls, field=DEFAULT_TS_FIELD, start=None, end=None):

        if not start and not end:
            return ''

        start_end = ''
        if start:
            start_end = '"gte": "%s",' % start.isoformat()
        if end:
            start_end += '"lte": "%s",' % end.isoformat()
        start_end = start_end[:-1]  # remove last comma

        query_range = """
        {
          "range": {
            "%s": {
              %s
            }
          }
        }
        """ % (field, start_end)

        return query_range

    @classmethod
    def get_query_all(cls, field=None, start=None, end=None):
        if not field:
            field=DEFAULT_TS_FIELD
        query_range = cls.get_query_range(field, start, end)

        if query_range:
            query_range = ", " +  query_range

        query_all = """
          "query": {
            "bool": {
              "must": [
                {
                  "query_string": {
                    "analyze_wildcard": true,
                    "query": "*"
                  }
                }
                %s
              ]
            }
          }
        """ % (query_range)

        return query_all

    @classmethod
    def get_query_agg_terms(cls, field, interval=None, timezone=None):

        if interval:
            # 1d, 1w, 1d, 1M, 1y
            interval = '"interval": "%s",' % interval
            size = ''
        else:
            interval =''
            size = '"size": %i,' % cls.AGG_SIZE
        if timezone:
            # Europe/Berlin
            timezone = '"timezone": "%s",' % timezone
        else:
            timezone = ''

        query_agg = """
          "aggs": {
            "%i": {
              "terms": {
                "field": "%s",
                %s %s %s
                "order": {
                  "_count": "desc"
                }
              }
            }
          }
        """ % (cls.AGGREGATION_ID, field, interval, timezone, size)

        return query_agg

    @classmethod
    def get_query_agg_max(cls, field):
        query_agg = """
          "aggs": {
            "%i": {
              "max": {
                "field": "%s"
              }
            }
          }
        """ % (cls.AGGREGATION_ID, field)

        return query_agg

    @classmethod
    def get_query_agg_count(cls, field):
        query_agg = """
          "aggs": {
            "%i": {
              "cardinality": {
                "field": "%s"
              }
            }
          }
        """ % (cls.AGGREGATION_ID, field)

        return query_agg

    @classmethod
    def get_query_agg_count_ts(cls, field, time_field):
        """ Time series for an aggregation metric """
        query_agg = """
             "aggs": {
                "%i": {
                  "date_histogram": {
                    "field": "%s",
                    "interval": "1M",
                    "time_zone": "Europe/Berlin",
                    "min_doc_count": 1
                  },
                  "aggs": {
                    "%i": {
                      "cardinality": {
                        "field": "%s"
                      }
                    }
                  }
                }
            }
        """ % (cls.AGGREGATION_ID, time_field, cls.AGGREGATION_ID+1, field)

        return query_agg


    @classmethod
    def get_count(cls, start = None, end = None):
        query_all = cls.get_query_all(start=start, end=end)

        query = """
            {
              "size": 0,
              %s
              }
        """ % (query_all)

        return query


    @classmethod
    def get_agg_count(cls, field, date_field=None, start=None, end=None, agg_type="terms"):
        """ if field and date_field the time series of the agg is collected """

        query_all = cls.get_query_all(field=date_field, start=start, end=end)

        if not date_field:
            if agg_type == "terms":
                query_agg = ElasticQuery.get_query_agg_terms(field)
            elif agg_type == "max":
                query_agg = ElasticQuery.get_query_agg_max(field)
            elif agg_type == "count":
                query_agg = ElasticQuery.get_query_agg_count(field)
            else:
                RuntimeError("Aggregation of %s not supported", agg_type)
        else:
            if agg_type == "count":
                query_agg = ElasticQuery.get_query_agg_count_ts(field, date_field)
            else:
                RuntimeError("Aggregation of %s in ts not supported", agg_type)

        query = """
            {
              "size": 0,
              %s,
              %s
              }
        """ % (query_agg, query_all)

        return query
