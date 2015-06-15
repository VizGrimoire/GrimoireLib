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

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("eve.name as event_name")
        fields.add("eve.meetup_id as event_id")
        fields.add("gro.urlname as group_id")
        fields.add("gro.name as group_name")
        fields.add("count(distinct(rsvps.member_id)) as attendees")
        fields.add("eve.time as date")
        fields.add("eve.rating_average as rating")
        fields.add("cit.city as city")
        fields.add("cit.country as country")

        tables.add("events eve")
        tables.add("groups gro")
        tables.add("rsvps")
        tables.add("cities cit")

        filters.add("eve.group_id = gro.id")
        filters.add("eve.id = rsvps.event_id")
        filters.add("rsvps.response = 'yes'")
        filters.add("eve.city_id = cit.id")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " eve.time ", fields,
                                   tables, filters, False, self.filters.type_analysis)

        query = query + " group by eve.meetup_id "
        query = query + " order by eve.time desc "

        data = self.db.ExecuteQuery(query)
        # TODO: Hardcoded creation of file
        #createJSON(data, "../../../../json/eventizer-events.json")

        return data


if __name__ == '__main__':

    filters = MetricFilters("month", "'2013-04-01'", "'2016-01-01'")
    dbcon = EventizerQuery("root", "", "test_eventizer", "test_eventizer")

    allevents = AllEvents(dbcon, filters)
    print allevents.result()
