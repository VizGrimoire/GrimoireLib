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
##   Alvaro del Castillo <acs@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>

from esquery import ElasticQuery
from metrics.metrics import Metrics


class SubmittedPR(Metrics):
    id = "submitted"
    name = "Submitted reviews"
    desc = "Number of submitted code review processes"
    filters = {"pull_request":"true"}

    def get_query(self, evolutionary=False):
        if not evolutionary:
            query = ElasticQuery.get_count(start=self.start, end=self.end, filters=self.esfilters)
        else:
            query = ElasticQuery.get_agg_count(date_field=self.FIELD_DATE, start=self.start,
                                               end=self.end, filters=self.esfilters, agg_type="count",
                                               interval=self.interval)
        return query

    def get_list(self):
        field = "url"
        return super(type(self), self).get_list(field)


class ClosedPR(Metrics):
    id = "closed"
    name = "Closed reviews"
    desc = "Number of closed review processes (merged or abandoned)"
    filters = {"pull_request":"true", "state":"closed"}

    def get_query(self, evolutionary=False):
        if not evolutionary:
            query = ElasticQuery.get_count(start=self.start, end=self.end, filters=self.esfilters)
        else:
            query = ElasticQuery.get_agg_count(date_field=self.FIELD_DATE, start=self.start,
                                               end=self.end, filters=self.esfilters,
                                               agg_type="count", interval=self.interval)
        return query

    def get_list(self):
        field = "url"
        return super(type(self), self).get_list(field)

class ProjectsPR(Metrics):
    """ Projects in the review code management system """

    id = "projects"
    name = "Projects"
    desc = "Projects in the review code management system"
    FIELD_NAME = 'project' # field used to list projects

    def get_list(self):
        return super(type(self), self).get_list(self.FIELD_NAME)


class Reviewers(Metrics):
    """ People assigned to pull requests """
    id = "reviewers"
    name = "Reviewers"
    desc = "Number of persons reviewing code review activities"

class Closers(Metrics):
    id = "closers"
    name = "Closers"
    desc = "Number of persons closing code review activities"

# Pretty similar to ITS openers
class Submitters(Metrics):
    id = "submitters"
    name = "Submitters"
    desc = "Number of persons submitting code review processes"
