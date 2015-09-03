#!/usr/bin/env python

# Copyright (C) 2014 Bitergia
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
#
# Authors:
#     Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>

from sets import Set

from vizgrimoire.analysis.analyses import Analyses

from vizgrimoire.metrics.query_builder import EventizerQuery

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.EventsDS import EventsDS

from vizgrimoire.GrimoireUtils import completePeriodIds, createJSON


class AllEvents(Analyses):

    id = "events_list"
    name = "List of events"
    desc = "List of events"
    data_source = EventsDS

    def result(self):

        query = """ SELECT eve.name as event_name,
                           eve.meetup_id as event_id,
                           gro.urlname as group_id,
                           gro.name as group_name,
                           IFNULL(t.rsvps, 0) as rsvps,
                           eve.local_time as date,
                           eve.rating_average as rating,
                           cit.city as city,
                           cit.country as country
                    FROM events eve
                    LEFT JOIN groups gro
                         ON eve.group_id = gro.id
                    LEFT JOIN cities cit
                         ON eve.city_id = cit.id
                    LEFT JOIN (select event_id,
                                      count(distinct(member_id)) as rsvps
                               from rsvps
                               where response='yes'
                               group by event_id) as t
                         ON eve.id = t.event_id
                    GROUP BY eve.meetup_id
                    ORDER BY eve.local_time DESC
                """

        data = self.db.ExecuteQuery(query)
        # TODO: Hardcoded creation of file
        #createJSON(data, "../../../../json/eventizer-events.json")

        return data


if __name__ == '__main__':

    filters = MetricFilters("month", "'2013-04-01'", "'2016-01-01'")
    dbcon = EventizerQuery("root", "", "test_eventizer", "test_eventizer")

    allevents = AllEvents(dbcon, filters)
    print allevents.result()
