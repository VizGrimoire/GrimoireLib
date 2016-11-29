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
##   Alvaro del Castillo <acs@bitergia.com>
##

from esquery import ElasticQuery
from metrics.metrics import Metrics


class EmailsSent(Metrics):
    """ Emails metric class for mailing lists analysis """

    id = "sent"
    name = "Emails Sent"
    desc = "Emails sent to mailing lists"

    def get_query(self, evolutionary=False):
        if not evolutionary:
            query = ElasticQuery.get_count(start=self.start, end=self.end)
        else:
            query = ElasticQuery.get_agg_count(date_field=self.FIELD_DATE, start=self.start,
                                               end=self.end, agg_type="count",
                                               interval=self.interval)
        return query

    def get_list(self):
        field = "Message-ID"
        return super(type(self), self).get_list(field)


class EmailsSenders(Metrics):
    """ Emails Senders class for mailing list analysis """

    id = "senders"
    name = "Email Senders"
    desc = "People sending emails"

    FIELD_COUNT = 'author_uuid' # field used to count Authors
    FIELD_NAME = 'author_name' # field used to count Authors

    def get_query(self, evolutionary):
        if not evolutionary:
            query = ElasticQuery.get_agg_count(field=self.FIELD_COUNT, start=self.start,
                                               end=self.end, agg_type="count",
                                               interval=self.interval)
        else:
            query = ElasticQuery.get_agg_count(field=self.FIELD_COUNT, start=self.start,
                                               end=self.end, date_field=self.FIELD_DATE,
                                               agg_type="count", interval=self.interval)
        return query

    def get_list(self):
        return super(type(self), self).get_list(self.FIELD_NAME)

class ProjectsMLS(Metrics):
    """ Projects in the mailing lists """

    id = "projects"
    name = "Projects"
    desc = "Projects in the mailing lists"
    FIELD_NAME = 'project' # field used to list projects


    def get_list(self):
        return super(type(self), self).get_list(self.FIELD_NAME)
