#!/usr/bin/python3
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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##   Alvaro del Castillo  <acs@bitergia.com>
##


from esquery import ElasticQuery
from metrics.metrics import Metrics


FIELD_DATE='metadata__updated_on'

class Commits(Metrics):
    """ Commits metric class for source code management systems """

    id = "commits"
    name = "Commits"
    desc = "Changes to the source code"

    def get_query(self, evolutionary=False):
        if not evolutionary:
            query = ElasticQuery.get_count(start=self.start, end=self.end)
        else:
            query = ElasticQuery.get_agg_count(FIELD_DATE, start=self.start,
                                               end=self.end)
        return query

    def get_list(self):
        field = "hash"
        return super(Commits, self).get_list(field)

class Authors(Metrics):
    """ Authors metric class for source code management systems """

    id = "authors"
    name = "Authors"
    desc = "People authoring commits (changes to source code)"
    FIELD_COUNT = 'author_uuid' # field used to count Authors
    FIELD_NAME = 'author_name' # field used to count Authors

    def get_query(self, evolutionary):
        if not evolutionary:
            query = ElasticQuery.get_agg_count(self.FIELD_COUNT, start=self.start,
                                               end=self.end, agg_type="count")
        else:
            query = ElasticQuery.get_agg_count(self.FIELD_COUNT, start=self.start,
                                               end=self.end, date_field=FIELD_DATE, agg_type="count")
        return query

    def get_list(self):
        return super(type(self), self).get_list(self.FIELD_NAME)

class Committers(Metrics):
    """ Committers metric class for source code management systems """

    id = "committers"
    name = "Committers"
    desc = "Number of developers committing (merging changes to source code)"
    FIELD_COUNT = 'Commit_uuid'
    FIELD_NAME = 'Commit_name'

    def get_query(self, evolutionary):
        if not evolutionary:
            query = ElasticQuery.get_agg_count(self.FIELD_COUNT, start=self.start, end=self.end, agg_type="count")
        else:
            query = ElasticQuery.get_agg_count(self.FIELD_COUNT, start=self.start, end=self.end, date_field=FIELD_DATE, agg_type="count")
        return query

    def get_list(self):
        return super(type(self), self).get_list(self.FIELD_NAME)
