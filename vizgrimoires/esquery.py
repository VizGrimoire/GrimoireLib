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
    def get_query_filters(cls, filters=None):
        """ {"name1":"value1", "name2":"value2"} """

        query_filters = ''

        if not filters:
            return query_filters

        for name in filters:
            query_filters += """
                {
                  "match": {
                    "%s": {
                      "query": "%s",
                      "type": "phrase"
                    }
                  }
                }
            """ % (name, filters[name])
            query_filters += ","

        query_filters = query_filters[:-1]  # Remove the last comma

        return query_filters

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
    def get_query_basic(cls, field=None, start=None, end=None, filters=None):
        if not field:
            field=DEFAULT_TS_FIELD
        query_range = cls.get_query_range(field, start, end)
        if query_range:
            query_range = ", " +  query_range

        query_filters = cls.get_query_filters(filters)
        if query_filters:
            query_filters = ", " +  query_filters

        query_basic = """
          "query": {
            "bool": {
              "must": [
                {
                  "query_string": {
                    "analyze_wildcard": true,
                    "query": "*"
                  }
                }
                %s %s
              ]
            }
          }
        """ % (query_range, query_filters)

        return query_basic


    @classmethod
    def get_query_agg_terms(cls, field, interval=None, time_zone=None):
        query_agg = """
          "aggs": {
            "%i": {
              "terms": {
                "field": "%s",
                "size": %i,
                "order": {
                    "_count": "desc"
                }
              }
            }
          }
        """ % (cls.AGGREGATION_ID, field, cls.AGG_SIZE)
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
    def get_query_agg_count(cls, field, agg_id=None):
        if not agg_id:
            agg_id=cls.AGGREGATION_ID
        query_agg = """
          "aggs": {
            "%i": {
              "cardinality": {
                "field": "%s"
              }
            }
          }
        """ % (agg_id, field)

        return query_agg

    @classmethod
    def get_query_agg_count_ts(cls, field, time_field, interval=None, time_zone=None):
        """ Time series for an aggregation metric """
        if not interval:
            interval = '1M'
        if not time_zone:
            time_zone = 'Europe/Berlin'

        if not field:
            count_agg = ''
        else:
            count_agg = cls.get_query_agg_count(field, agg_id=cls.AGGREGATION_ID+1)
            count_agg = ", " + count_agg

        query_agg = """
             "aggs": {
                "%i": {
                  "date_histogram": {
                    "field": "%s",
                    "interval": "%s",
                    "time_zone": "%s",
                    "min_doc_count": 1
                  }
                  %s
                }
            }
        """ % (cls.AGGREGATION_ID, time_field, interval, time_zone, count_agg)

        return query_agg


    @classmethod
    def get_count(cls, start=None, end=None, filters=None):
        query_basic = cls.get_query_basic(start=start, end=end, filters=filters)

        query = """
            {
              "size": 0,
              %s
              }
        """ % (query_basic)


        return query


    @classmethod
    def get_agg_count(cls, field=None, date_field=None, start=None, end=None,
                      filters=None, agg_type="terms"):
        """ if field and date_field the time series of the agg is collected """

        query_basic = cls.get_query_basic(field=date_field, start=start, end=end, filters=filters)

        if not date_field:
            if agg_type == "terms":
                query_agg = ElasticQuery.get_query_agg_terms(field)
            elif agg_type == "max":
                query_agg = ElasticQuery.get_query_agg_max(field)
            elif agg_type == "count":
                query_agg = ElasticQuery.get_query_agg_count(field)
            else:
                raise RuntimeError("Aggregation of %s not supported" % agg_type)
        else:
            if agg_type == "count":
                query_agg = ElasticQuery.get_query_agg_count_ts(field, date_field)
            else:
                raise RuntimeError("Aggregation of %s in ts not supported" % agg_type)

        query = """
            {
              "size": 0,
              %s,
              %s
              }
        """ % (query_agg, query_basic)

        return query
